"""
مراحل ۵ و ۶ کارگاه «رَوَند»: فید ترکیبی (Hybrid Fan-out) + شخصی‌سازی + ثبت نمایش/کلیک و CTR
روی همان keyspace و جداول مراحل ۱ تا ۴ (ravand) اجرا می‌شود.

اجرا:   python stage5_6.py            (Cassandra روی 127.0.0.1:9042 با docker-compose کلاس)
"""
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

# ----------------------------- پیکربندی ثابت در طول هر اجرا -----------------------------
CELEBRITY_AUTHORS: Set[str] = {"celeb_1", "celeb_2"}   # مجموعه نویسندگان مشهور (از تنظیمات خوانده می‌شود)
WINDOW_HOURS = 48
FEED_SIZE = 20
CANDIDATE_LIMIT = 100
MODEL_CHRONO = "chrono_v0"      # فید زمانی خالص (مرحله ۵)
MODEL_INTEREST = "interest_v1"  # فید شخصی‌سازی‌شده (مرحله ۶)

NEW_TABLES_CQL = [
    """CREATE TABLE IF NOT EXISTS ravand.feed_impressions_by_day (
        day_bucket date, model_version text, impression_id text,
        user_id text, post_id text, shown_at timestamp,
        PRIMARY KEY ((day_bucket, model_version), impression_id))""",
    """CREATE TABLE IF NOT EXISTS ravand.feed_clicks_by_impression (
        impression_id text, click_id text, clicked_at timestamp,
        PRIMARY KEY ((impression_id), click_id))""",
]


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
    scores: Dict[str, float]
    candidate_ms: float
    rank_ms: float
    db_reads: int
    candidates_count: int


@dataclass
class CtrResult:
    impressions: int
    clicked_impressions: int
    ctr: float


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


def score_post(post: Post, interests: Set[str], ref_time: datetime) -> float:
    hours_age = (ref_time - post.published_at).total_seconds() / 3600.0
    base = 10.0 if post.topic in interests else 0.0
    return round(base + 0.1 * max(0.0, WINDOW_HOURS - hours_age), 6)


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
        self.sel_interests = p("SELECT interests FROM ravand.users WHERE user_id = ?")
        self.ins_impression = p("""INSERT INTO ravand.feed_impressions_by_day
            (day_bucket, model_version, impression_id, user_id, post_id, shown_at) VALUES (?, ?, ?, ?, ?, ?)""")
        self.ins_click = p("""INSERT INTO ravand.feed_clicks_by_impression
            (impression_id, click_id, clicked_at) VALUES (?, ?, ?)""")
        self.sel_impressions = p("""SELECT impression_id FROM ravand.feed_impressions_by_day
            WHERE day_bucket = ? AND model_version = ?""")
        self.sel_first_click = p("SELECT click_id FROM ravand.feed_clicks_by_impression WHERE impression_id = ? LIMIT 1")

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

    def get_user_interests(self, user_id: str) -> Set[str]:
        rows = self._read(self.sel_interests, (user_id,))
        return set(rows[0].interests or []) if rows else set()

    def rank(self, candidates: List[Post], interests: Set[str], ref_time: datetime) -> Tuple[List[Post], Dict[str, float]]:
        ref_time = utc(ref_time)
        scores = {p.post_id: score_post(p, interests, ref_time) for p in candidates}
        ranked = sorted(candidates, key=lambda p: (scores[p.post_id], p.published_at, p.post_id), reverse=True)
        return ranked[:FEED_SIZE], scores

    def get_feed(self, user_id: str, ref_time: datetime, model_version: str) -> FeedResult:
        r0 = self.db_reads
        t0 = time.perf_counter()
        if model_version == MODEL_CHRONO:
            cands = self.generate_candidates(user_id, ref_time, FEED_SIZE)
        else:
            cands = self.generate_candidates(user_id, ref_time, CANDIDATE_LIMIT)
        t1 = time.perf_counter()
        if model_version == MODEL_CHRONO:
            final, scores = cands[:FEED_SIZE], {}
        else:
            interests = self.get_user_interests(user_id)
            final, scores = self.rank(cands, interests, ref_time)
        t2 = time.perf_counter()
        return FeedResult(final, scores, (t1 - t0) * 1000, (t2 - t1) * 1000, self.db_reads - r0, len(cands))

    # ---------- مرحله ۶: ثبت نمایش و کلیک ----------
    @staticmethod
    def request_id(user_id: str, model_version: str, shown_at: datetime) -> str:
        return f"{user_id}|{model_version}|{utc(shown_at).isoformat()}"

    def log_impressions(self, user_id: str, posts: List[Post], model_version: str, shown_at: datetime) -> Dict[str, str]:
        """هر پست فید نهایی یک impression با شناسه قطعی (درخواست + post_id). ثبت مجدد، رکورد را بازنویسی می‌کند نه تکرار."""
        shown_at = utc(shown_at)
        rid = self.request_id(user_id, model_version, shown_at)
        ids = {p.post_id: f"{rid}|{p.post_id}" for p in posts}
        self._write_many(self.ins_impression, [
            (shown_at.date(), model_version, ids[p.post_id], user_id, p.post_id, shown_at) for p in posts])
        return ids

    def log_click(self, impression_id: str, click_id: str, clicked_at: datetime):
        self._write(self.ins_click, (impression_id, click_id, utc(clicked_at)))

    def compute_ctr(self, day: date, model_version: str) -> CtrResult:
        """CTR = (impressionهای متمایزی که حداقل یک کلیک دارند) / (impressionهای متمایز همان روز و نسخه)."""
        imps = [r.impression_id for r in self._read(self.sel_impressions, (day, model_version))]
        self.db_reads += len(imps)
        clicked = 0
        for ok, rows in self._run_many(self.sel_first_click, [(i,) for i in imps]):
            if ok and list(rows):
                clicked += 1
        return CtrResult(len(imps), clicked, (clicked / len(imps)) if imps else 0.0)


# ==================================================================================
#                    داده آزمون قطعی + تست‌های درستی + مقایسه هزینه
# ==================================================================================
REF = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)


def ago(h: float) -> datetime:
    return REF - timedelta(hours=h)


TABLES = ["users", "user_follows", "author_followers", "posts_by_author", "user_feed",
          "feed_impressions_by_day", "feed_clicks_by_impression"]


def reset_db(session):
    for q in NEW_TABLES_CQL:
        session.execute(q)
    for t in TABLES:
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


def expected_chrono(events: List[PublishEvent], follows: Set[str], limit: int) -> List[str]:
    """مرجع مستقل از دیتابیس (Ground Truth)"""
    start = REF - timedelta(hours=WINDOW_HOURS)
    ps = [Post(e.post_id, e.author_id, e.published_at, e.content, e.topic) for e in events
          if e.author_id in follows and start <= e.published_at <= REF]
    return [p.post_id for p in sorted(ps, key=sort_key, reverse=True)[:limit]]


def run_stage5_6_verification(session, run_many=None):
    run_many = run_many or (lambda stmt, params: execute_concurrent_with_args(session, stmt, params, concurrency=32))
    reset_db(session)
    svc = HybridFeedService(session, CELEBRITY_AUTHORS, run_many=run_many)

    followed = ["celeb_1", "author_active", "author_quiet"]
    follows = [(u, a) for u in ("sara", "amin") for a in followed]
    seed_relations(session, run_many, follows,
                   {"sara": {"tech"}, "amin": {"travel"}},
                   extra_followers={"celeb_1": 2000, "author_active": 1, "author_quiet": 1, "stranger": 1})
    # sara و amin در author_followers خودشان از follows آمده‌اند
    events = build_dataset()
    pub = {}
    for ev in events:
        pub[ev.post_id] = svc.publish(ev)

    # ردیف کهنه: sara قبلاً stranger را دنبال می‌کرده، پست او هنوز در user_feed مانده است
    stale = [e for e in events if e.post_id == "stranger_001"][0]
    session.execute(svc.ins_feed, ("sara", stale.published_at.date(), stale.published_at, stale.post_id,
                                   stale.author_id, stale.content, stale.topic))

    print("=" * 78)
    print("تحویل ۱ و ۲: درستی فید ترکیبی، حذف تکرار، ترتیب قطعی، نویسنده کم‌فعالیت")
    print("=" * 78)
    for user in ("sara", "amin"):
        res = svc.get_feed(user, REF, MODEL_CHRONO)
        ids = [p.post_id for p in res.posts]
        exp = expected_chrono(events, set(followed), FEED_SIZE)
        assert ids == exp, f"{user}: {ids} != {exp}"
        assert len(ids) == len(set(ids)) == FEED_SIZE
        assert ids.index("tie_z_pull") < ids.index("tie_a_push"), "tie must be ordered by post_id DESC"
        assert "quiet_001" in ids, "low-activity author's fresh post must not be dropped"
        assert any(p.author_id == "celeb_1" for p in res.posts) and any(p.author_id == "author_active" for p in res.posts)
    print("PASS  خروجی == مرجع مستقل (۲۰ پست، بدون تکرار، مرتب‌شده)")
    print("PASS  پست‌های هم‌زمان با post_id نزولی مرتب شدند (tie_z_pull قبل از tie_a_push)")
    print("PASS  پست quiet_001 (نویسنده کم‌فعالیت) جا نماند")
    # ادغام با تکرار واقعی: پست قدیمیِ نویسنده‌ای که بعداً مشهور شد در هر دو منبع است
    dup = Post("dup", "celeb_1", ago(5), "x", "news")
    assert [p.post_id for p in merge_dedupe([dup, dup, Post("other", "a", ago(6), "x", "news")], 20)] == ["dup", "other"]
    print("PASS  ادغام پست مشترک دو منبع را یک‌بار نشان می‌دهد")

    # بازنگری: نویسنده عادی که بعداً مشهور می‌شود -> پست‌های قدیمی‌اش در user_feed هست و در posts_by_author هم
    svc2 = HybridFeedService(session, CELEBRITY_AUTHORS | {"author_active"}, run_many=run_many)
    ids2 = [p.post_id for p in svc2.get_feed("sara", REF, MODEL_CHRONO).posts]
    assert ids2 == expected_chrono(events, set(followed), FEED_SIZE) and len(ids2) == len(set(ids2))
    print("PASS  نویسنده‌ای که وسط کار مشهور می‌شود (پست در هر دو منبع) هیچ‌وقت دوبار نمایش داده نمی‌شود")

    print("\n" + "=" * 78)
    print("تحویل ۶: عدم نفوذ پست غیرمجاز (دنبال‌نشده / خارج از ۴۸ ساعت) حتی با امتیاز بالا")
    print("=" * 78)
    for user in ("sara", "amin"):
        res = svc.get_feed(user, REF, MODEL_INTEREST)
        shown = {p.post_id for p in res.posts}
        assert not shown & {"celeb2_001", "stranger_001", "old_celeb", "old_active"}
        start = REF - timedelta(hours=WINDOW_HOURS)
        assert all(start <= p.published_at <= REF and p.author_id in set(followed) for p in res.posts)
    print("PASS  celeb2_001 / stranger_001 (دنبال‌نشده) و old_* (خارج بازه) وارد فید شخصی‌سازی‌شده نشدند")

    print("\n" + "=" * 78)
    print("تحویل ۵: مقایسه فید شخصی‌سازی‌شده دو کاربر با نویسندگان یکسان")
    print("=" * 78)
    feeds = {}
    for user in ("sara", "amin"):
        r = svc.get_feed(user, REF, MODEL_INTEREST)
        feeds[user] = r
        print(f"\n{user}  interests={sorted(svc.get_user_interests(user))}  candidates={r.candidates_count}")
        print(f"{'#':<3}{'post_id':<14}{'author':<15}{'topic':<8}{'age(h)':<8}{'score':<10}")
        for i, p in enumerate(r.posts, 1):
            age = (REF - p.published_at).total_seconds() / 3600
            print(f"{i:<3}{p.post_id:<14}{p.author_id:<15}{p.topic:<8}{age:<8.2f}{r.scores[p.post_id]:<10}")
    s_top = [p.topic for p in feeds["sara"].posts]
    a_top = [p.topic for p in feeds["amin"].posts]
    assert s_top[:10] == ["tech"] * 10 and a_top[:10] == ["travel"] * 10
    assert [p.post_id for p in feeds["sara"].posts] != [p.post_id for p in feeds["amin"].posts]
    c_s = {p.post_id for p in svc.get_feed("sara", REF, MODEL_CHRONO).posts}
    c_a = {p.post_id for p in svc.get_feed("amin", REF, MODEL_CHRONO).posts}
    assert c_s == c_a
    print("\nPASS  فید زمانیِ دو کاربر یکسان است ولی فید شخصی‌سازی‌شده‌شان متفاوت و مطابق علاقه است")

    print("\n" + "=" * 78)
    print("تحویل ۷: زمان تولید کاندیدا / زمان رتبه‌بندی (میلی‌ثانیه) و تکرارپذیری")
    print("=" * 78)
    r1 = svc.get_feed("sara", REF, MODEL_INTEREST)
    r2 = svc.get_feed("sara", REF, MODEL_INTEREST)
    print(f"candidate_generation = {r1.candidate_ms:.3f} ms | ranking = {r1.rank_ms:.3f} ms | "
          f"db_reads = {r1.db_reads} | candidates = {r1.candidates_count}")
    assert [p.post_id for p in r1.posts] == [p.post_id for p in r2.posts] and r1.scores == r2.scores
    # اجرای مجدد رویداد انتشار با همان post_id و published_at نباید چیزی عوض کند
    svc.publish([e for e in events if e.post_id == "active_000"][0])
    r3 = svc.get_feed("sara", REF, MODEL_INTEREST)
    assert [p.post_id for p in r3.posts] == [p.post_id for p in r1.posts]
    print("PASS  با ورودی و model_version یکسان دو اجرا دقیقاً یک ترتیب و یک امتیاز دادند؛ replay انتشار هم اثری ندارد")

    print("\n" + "=" * 78)
    print("تحویل ۸: ثبت نمایش/کلیک در Cassandra و CTR به تفکیک روز و نسخه مدل")
    print("=" * 78)

    def click_policy(user, posts):  # شبیه‌سازی قطعی: کاربر فقط ۱۰ پست اول را می‌بیند و روی پست‌های هم‌موضوع با علاقه‌اش کلیک می‌کند
        interests = svc.get_user_interests(user)
        return [p for p in posts[:10] if p.topic in interests]

    expected = {}
    for model in (MODEL_CHRONO, MODEL_INTEREST):
        imp_total = clk_total = 0
        for user in ("sara", "amin"):
            res = svc.get_feed(user, REF, model)
            ids = svc.log_impressions(user, res.posts, model, REF)
            for p in click_policy(user, res.posts):
                svc.log_click(ids[p.post_id], f"click_{ids[p.post_id]}", REF + timedelta(seconds=5))
            imp_total += len(res.posts)
            clk_total += len(click_policy(user, res.posts))
        expected[model] = (imp_total, clk_total)

    # شرایط مرزی: نمایش تکراری، کلیک تکراری روی یک impression، کلیک بدون impression
    res = svc.get_feed("sara", REF, MODEL_INTEREST)
    ids = svc.log_impressions("sara", res.posts, MODEL_INTEREST, REF)            # ثبت مجدد همان نمایش
    first = res.posts[0]
    svc.log_click(ids[first.post_id], "click_second_on_same_impression", REF + timedelta(seconds=9))
    svc.log_click("ghost|impression", "ghost_click", REF)                         # impression وجود ندارد

    for model in (MODEL_CHRONO, MODEL_INTEREST):
        c = svc.compute_ctr(REF.date(), model)
        imp, clk = expected[model]
        assert (c.impressions, c.clicked_impressions) == (imp, clk), (model, c, expected[model])
        print(f"day={REF.date()} model={model:<12} impressions={c.impressions:<3} "
              f"clicked_impressions={c.clicked_impressions:<3} CTR={c.ctr:.3f}")
    print("PASS  نمایش تکراری، چند کلیک روی یک impression و کلیکِ بدون impression، CTR را باد نکردند")
    return svc, events, follows


def cost_comparison(session, run_many=None):
    """مقایسه مرحله ۳ خالص (همه Push) با ترکیبی: ۱ پست مشهور با F دنبال‌کننده، سپس باز شدن فید کاربر."""
    run_many = run_many or (lambda stmt, params: execute_concurrent_with_args(session, stmt, params, concurrency=32))
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
        res = svc.get_feed("sara", REF, MODEL_CHRONO)
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
    run_stage5_6_verification(session)
    cost_comparison(session)
    print("\nALL CHECKS PASSED")
    cluster.shutdown()
