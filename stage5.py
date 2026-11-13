"""
مرحله ۵ کارگاه «رَوَند»: فید ترکیبی (Hybrid Fan-out) روی Cassandra
  - نویسنده عادی: Fan-out on Write (مثل مرحله ۳)
  - نویسنده مشهور: Fan-out on Read (فقط در posts_by_author؛ موقع باز شدن فید خوانده می‌شود)
  - ادغام دو منبع، حذف تکرار با post_id، ترتیب قطعی، ۲۰ پست از ۴۸ ساعت اخیر

پیش‌نیاز: جداول مراحل ۱ تا ۳ (q1.sql و q3.sql) ساخته شده باشند.
اجرا:  python stage5.py
"""
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Set, Tuple

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

# ----------------------------- پیکربندی ثابت در طول هر اجرا -----------------------------
CELEBRITY_AUTHORS: Set[str] = {"celeb_1", "celeb_2"}   # مجموعه نویسندگان مشهور (از تنظیمات خوانده می‌شود)
WINDOW_HOURS = 48
FEED_SIZE = 20


@dataclass(frozen=True)
class Post:
    post_id: str
    author_id: str
    published_at: datetime
    content: str
    topic: str


@dataclass
class PublishEvent:
    post_id: str
    author_id: str
    published_at: datetime
    content: str
    topic: str


@dataclass
class PublishMetrics:
    is_celebrity: bool
    followers_written: int
    logical_writes: int
    db_reads: int


@dataclass
class FeedResult:
    posts: List[Post]
    candidate_ms: float
    db_reads: int
    candidates_count: int
    scores: Dict[str, float] = field(default_factory=dict)   # در مرحله ۶ پر می‌شود
    rank_ms: float = 0.0                                      # در مرحله ۶ پر می‌شود


def utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def day_buckets_desc(start: datetime, end: datetime) -> List[date]:
    out, d = [], end.date()
    while d >= start.date():
        out.append(d)
        d -= timedelta(days=1)
    return out


def sort_key(p: Post):
    # ترتیب قطعی: published_at نزولی، سپس post_id نزولی (هم‌جهت با CLUSTERING ORDER جداول)
    return (p.published_at, p.post_id)


def merge_dedupe(posts: List[Post], limit: int) -> List[Post]:
    unique: Dict[str, Post] = {}
    for p in posts:
        unique.setdefault(p.post_id, p)
    return sorted(unique.values(), key=sort_key, reverse=True)[:limit]


class HybridFeedService:
    def __init__(self, session, celebrities: Set[str], run_many=None, concurrency: int = 32):
        self.s = session
        self.celebrities = set(celebrities)
        self.concurrency = concurrency
        self._run_many = run_many or self._default_run_many
        self.db_reads = 0
        self.logical_writes = 0
        p = session.prepare
        self.ins_post = p("""INSERT INTO ravand.posts_by_author
            (author_id, day_bucket, published_at, post_id, content, topic) VALUES (?, ?, ?, ?, ?, ?)""")
        self.sel_followers = p("SELECT follower_id FROM ravand.author_followers WHERE author_id = ?")
        self.ins_feed = p("""INSERT INTO ravand.user_feed
            (user_id, day_bucket, published_at, post_id, author_id, content, topic) VALUES (?, ?, ?, ?, ?, ?, ?)""")
        self.sel_feed = p("""SELECT post_id, author_id, published_at, content, topic FROM ravand.user_feed
            WHERE user_id = ? AND day_bucket = ? AND published_at >= ? AND published_at <= ? LIMIT ?""")
        self.sel_author_posts = p("""SELECT post_id, author_id, published_at, content, topic FROM ravand.posts_by_author
            WHERE author_id = ? AND day_bucket = ? AND published_at >= ? AND published_at <= ? LIMIT ?""")
        self.sel_follows = p("SELECT author_id FROM ravand.user_follows WHERE follower_id = ?")

    # ---------- ابزارهای شمارشگر (هر دستور CQL = یک عملیات) ----------
    def _default_run_many(self, stmt, params_list):
        return execute_concurrent_with_args(self.s, stmt, params_list, concurrency=self.concurrency)

    def _read(self, stmt, params):
        self.db_reads += 1
        return list(self.s.execute(stmt, params))

    def _write(self, stmt, params):
        self.logical_writes += 1
        self.s.execute(stmt, params)

    def _write_many(self, stmt, params_list):
        self.logical_writes += len(params_list)
        for ok, res in self._run_many(stmt, params_list):
            if not ok:
                raise RuntimeError(f"write failed: {res}")

    def reset_counters(self):
        self.db_reads = 0
        self.logical_writes = 0

    # ---------- مسیر انتشار (Path Write) ----------
    def publish(self, ev: PublishEvent) -> PublishMetrics:
        r0, w0 = self.db_reads, self.logical_writes
        ev.published_at = utc(ev.published_at)
        bucket = ev.published_at.date()
        # ۱) پست اصلی همیشه در posts_by_author
        self._write(self.ins_post, (ev.author_id, bucket, ev.published_at, ev.post_id, ev.content, ev.topic))
        followers_written = 0
        is_celeb = ev.author_id in self.celebrities
        if not is_celeb:
            # ۲) نویسنده عادی: Fan-out on Write مثل مرحله ۳ (کلید اصلی => اجرای مجدد بی‌خطر است)
            followers = [r.follower_id for r in self._read(self.sel_followers, (ev.author_id,))]
            self._write_many(self.ins_feed, [
                (f, bucket, ev.published_at, ev.post_id, ev.author_id, ev.content, ev.topic) for f in followers])
            followers_written = len(followers)
        # نویسنده مشهور: هیچ نوشتنی در user_feed (صفر شدن Fan-out)
        return PublishMetrics(is_celeb, followers_written, self.logical_writes - w0, self.db_reads - r0)

    # ---------- مسیر خواندن و ادغام (Path Read & Merge) ----------
    def _read_source(self, stmt, key: str, start: datetime, ref: datetime, limit: int) -> List[Post]:
        """از جدیدترین باکت روزانه به قدیم؛ در هر منبع فقط تا `limit` ردیف لازم خوانده می‌شود."""
        out: List[Post] = []
        for bucket in day_buckets_desc(start, ref):
            remaining = limit - len(out)
            if remaining <= 0:
                break
            for r in self._read(stmt, (key, bucket, start, ref, remaining)):
                out.append(Post(r.post_id, r.author_id, utc(r.published_at), r.content, r.topic))
        return out

    def generate_candidates(self, user_id: str, ref_time: datetime, limit: int) -> List[Post]:
        ref_time = utc(ref_time)
        start = ref_time - timedelta(hours=WINDOW_HOURS)
        follows = {r.author_id for r in self._read(self.sel_follows, (user_id,))}
        # منبع ۱: فید آماده (نویسندگان عادی). ردیف‌های نویسنده‌ای که دیگر دنبال نمی‌شود حذف می‌شوند.
        pushed = [p for p in self._read_source(self.sel_feed, user_id, start, ref_time, limit)
                  if p.author_id in follows]
        # منبع ۲: فقط نویسندگان مشهورِ دنبال‌شده، زنده از posts_by_author
        pulled: List[Post] = []
        for author in sorted(follows & self.celebrities):
            pulled += self._read_source(self.sel_author_posts, author, start, ref_time, limit)
        return merge_dedupe(pushed + pulled, limit)

    def get_feed(self, user_id: str, ref_time: datetime) -> FeedResult:
        """فید زمانی ۲۰ پست‌ه (خروجی مرحله ۵)."""
        r0 = self.db_reads
        t0 = time.perf_counter()
        posts = self.generate_candidates(user_id, ref_time, FEED_SIZE)
        t1 = time.perf_counter()
        return FeedResult(posts, (t1 - t0) * 1000, self.db_reads - r0, len(posts))


# ==================================================================================
#                    داده آزمون قطعی + تست‌های درستی + مقایسه هزینه
# ==================================================================================
REF = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)


def ago(h: float) -> datetime:
    return REF - timedelta(hours=h)


BASE_TABLES = ["users", "user_follows", "author_followers", "posts_by_author", "user_feed"]


def reset_db(session, extra_tables: List[str] = ()):
    for t in list(BASE_TABLES) + list(extra_tables):
        session.execute(f"TRUNCATE ravand.{t}")


def seed_relations(session, run_many, follows: List[Tuple[str, str]], interests: Dict[str, Set[str]],
                   extra_followers: Dict[str, int] = None):
    i_user = session.prepare("INSERT INTO ravand.users (user_id, interests) VALUES (?, ?)")
    i_follows = session.prepare("INSERT INTO ravand.user_follows (follower_id, author_id) VALUES (?, ?)")
    i_foll = session.prepare("INSERT INTO ravand.author_followers (author_id, follower_id) VALUES (?, ?)")
    run_many(i_user, [(u, s) for u, s in interests.items()])
    run_many(i_follows, follows)
    rows = [(a, u) for u, a in follows]
    for author, n in (extra_followers or {}).items():
        rows += [(author, f"fan_{i:06d}") for i in range(n)]
    run_many(i_foll, rows)


def build_dataset() -> List[PublishEvent]:
    evs: List[PublishEvent] = []
    topics_c = ["news", "tech", "travel"]
    topics_a = ["sport", "travel", "tech"]
    for i in range(30):   # نویسنده مشهور پرفعالیت: ۲ تا ۳۱ ساعت قبل
        evs.append(PublishEvent(f"celeb1_{i:03d}", "celeb_1", ago(2 + i), f"celeb post {i}", topics_c[i % 3]))
    for i in range(30):   # نویسنده عادی پرفعالیت
        evs.append(PublishEvent(f"active_{i:03d}", "author_active", ago(3.5 + i * 1.1), f"active post {i}", topics_a[i % 3]))
    # نویسنده کم‌فعالیت: فقط ۲ پست؛ یکی بسیار تازه، یکی نزدیک مرز ۴۸ ساعت
    evs.append(PublishEvent("quiet_001", "author_quiet", ago(1.0), "quiet fresh", "cooking"))
    evs.append(PublishEvent("quiet_002", "author_quiet", ago(47.5), "quiet near boundary", "tech"))
    # دو پست هم‌زمان: یکی از مسیر Push و یکی از مسیر Pull  => باید با post_id نزولی مرتب شوند
    evs.append(PublishEvent("tie_a_push", "author_active", ago(0.25), "tie push", "news"))
    evs.append(PublishEvent("tie_z_pull", "celeb_1", ago(0.25), "tie pull", "news"))
    # بیرون از بازه ۴۸ ساعت (حتی با علاقه‌مندی بالا نباید وارد شوند)
    evs.append(PublishEvent("old_celeb", "celeb_1", ago(49), "too old celeb", "tech"))
    evs.append(PublishEvent("old_active", "author_active", ago(72), "too old active", "tech"))
    # نویسنده مشهوری که دنبال نمی‌شود + نویسنده عادی که دنبال نمی‌شود
    evs.append(PublishEvent("celeb2_001", "celeb_2", ago(0.2), "unfollowed celeb", "tech"))
    evs.append(PublishEvent("stranger_001", "stranger", ago(0.1), "unfollowed normal", "tech"))
    return evs


FOLLOWED = ["celeb_1", "author_active", "author_quiet"]


def load_scenario(session, run_many, service_cls=HybridFeedService, extra_tables: List[str] = ()):
    """آماده‌سازی دیتای مشترک آزمون‌ها (مراحل ۵ و ۶): کاربران، دنبال‌کنندگان، پست‌ها و یک ردیف کهنه."""
    reset_db(session, extra_tables)
    svc = service_cls(session, CELEBRITY_AUTHORS, run_many=run_many)
    follows = [(u, a) for u in ("sara", "amin") for a in FOLLOWED]
    seed_relations(session, run_many, follows,
                   {"sara": {"tech"}, "amin": {"travel"}},
                   extra_followers={"celeb_1": 2000, "author_active": 1, "author_quiet": 1, "stranger": 1})
    events = build_dataset()
    for ev in events:
        svc.publish(ev)
    # ردیف کهنه: sara قبلاً stranger را دنبال می‌کرده، پست او هنوز در user_feed مانده است
    stale = [e for e in events if e.post_id == "stranger_001"][0]
    session.execute(svc.ins_feed, ("sara", stale.published_at.date(), stale.published_at, stale.post_id,
                                   stale.author_id, stale.content, stale.topic))
    return svc, events


def expected_chrono(events: List[PublishEvent], follows: Set[str], limit: int) -> List[str]:
    """مرجع مستقل از دیتابیس (Ground Truth)"""
    start = REF - timedelta(hours=WINDOW_HOURS)
    ps = [Post(e.post_id, e.author_id, e.published_at, e.content, e.topic) for e in events
          if e.author_id in follows and start <= e.published_at <= REF]
    return [p.post_id for p in sorted(ps, key=sort_key, reverse=True)[:limit]]


def default_run_many(session):
    return lambda stmt, params: execute_concurrent_with_args(session, stmt, params, concurrency=32)


def run_stage5_verification(session, run_many=None):
    run_many = run_many or default_run_many(session)
    svc, events = load_scenario(session, run_many)

    print("=" * 78)
    print("تحویل ۱ و ۲: درستی فید ترکیبی، حذف تکرار، ترتیب قطعی، نویسنده کم‌فعالیت")
    print("=" * 78)
    for user in ("sara", "amin"):
        res = svc.get_feed(user, REF)
        ids = [p.post_id for p in res.posts]
        exp = expected_chrono(events, set(FOLLOWED), FEED_SIZE)
        assert ids == exp, f"{user}: {ids} != {exp}"
        assert len(ids) == len(set(ids)) == FEED_SIZE
        assert ids.index("tie_z_pull") < ids.index("tie_a_push"), "tie must be ordered by post_id DESC"
        assert "quiet_001" in ids, "low-activity author's fresh post must not be dropped"
        assert any(p.author_id == "celeb_1" for p in res.posts) and any(p.author_id == "author_active" for p in res.posts)
        assert not {"old_celeb", "old_active", "celeb2_001", "stranger_001"} & set(ids)
    print("PASS  خروجی == مرجع مستقل (۲۰ پست، بدون تکرار، مرتب‌شده، فقط ۴۸ ساعت اخیر، فقط دنبال‌شده‌ها)")
    print("PASS  پست‌های هم‌زمان با post_id نزولی مرتب شدند (tie_z_pull قبل از tie_a_push)")
    print("PASS  پست quiet_001 (نویسنده کم‌فعالیت) جا نماند")
    print("PASS  هم پست‌های Push (عادی) و هم Pull (مشهور) در فید هستند")
    res = svc.get_feed("sara", REF)
    print(f"      زمان تولید فید = {res.candidate_ms:.3f} ms با {res.db_reads} خواندن دیتابیس")

    dup = Post("dup", "celeb_1", ago(5), "x", "news")
    assert [p.post_id for p in merge_dedupe([dup, dup, Post("other", "a", ago(6), "x", "news")], 20)] == ["dup", "other"]
    print("PASS  ادغام پست مشترک دو منبع را یک‌بار نشان می‌دهد")

    # نویسنده عادی که بعداً مشهور می‌شود: پست‌های قدیمی‌اش هم در user_feed هست و هم در posts_by_author
    svc2 = HybridFeedService(session, CELEBRITY_AUTHORS | {"author_active"}, run_many=run_many)
    ids2 = [p.post_id for p in svc2.get_feed("sara", REF).posts]
    assert ids2 == expected_chrono(events, set(FOLLOWED), FEED_SIZE) and len(ids2) == len(set(ids2))
    print("PASS  نویسنده‌ای که وسط کار مشهور می‌شود (پست در هر دو منبع) هیچ‌وقت دوبار نمایش داده نمی‌شود")
    return svc


def cost_comparison(session, run_many=None):
    """مقایسه مرحله ۳ خالص (همه Push) با ترکیبی: ۱ پست مشهور با F دنبال‌کننده، سپس باز شدن فید کاربر."""
    run_many = run_many or default_run_many(session)
    F = 2000
    rows = {}
    for label, celebs in (("stage3_pure_push", set()), ("hybrid", {"celeb_1"})):
        reset_db(session)
        svc = HybridFeedService(session, celebs, run_many=run_many)
        seed_relations(session, run_many, [("sara", "celeb_1"), ("sara", "author_active")], {"sara": {"tech"}},
                       extra_followers={"celeb_1": F, "author_active": 49})
        for i in range(30):
            svc.publish(PublishEvent(f"a{i:03d}", "author_active", ago(3 + i), "x", "tech"))
        svc.reset_counters()
        pm = svc.publish(PublishEvent("celeb_new", "celeb_1", ago(0.5), "new celeb post", "news"))
        svc.reset_counters()
        res = svc.get_feed("sara", REF)
        assert res.posts[0].post_id == "celeb_new"
        rows[label] = (pm.logical_writes, pm.db_reads, res.db_reads)
    print("\n" + "=" * 78)
    print(f"تحویل ۳: مقایسه هزینه (نویسنده مشهور با {F + 1} دنبال‌کننده)")
    print("=" * 78)
    print(f"{'روش':<20}{'نوشتن منطقی هنگام انتشار':<28}{'خواندن هنگام انتشار':<22}{'خواندن هنگام باز شدن فید'}")
    for k, (w, rd, fr) in rows.items():
        print(f"{k:<20}{w:<28}{rd:<22}{fr}")
    assert rows["hybrid"][0] == 1 and rows["stage3_pure_push"][0] == 1 + F + 1
    return rows


if __name__ == "__main__":
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    run_stage5_verification(session)
    cost_comparison(session)
    print("\nSTAGE 5: ALL CHECKS PASSED")
    cluster.shutdown()
