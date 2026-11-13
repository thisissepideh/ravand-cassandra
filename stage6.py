"""
مرحله ۶ کارگاه «رَوَند» (امتیازی): شخصی‌سازی فید + ثبت نمایش/کلیک + CTR
بر پایه مرحله ۵: stage5.py باید کنار همین فایل باشد.

  ۱) تولید کاندیدا: همان مسیر ترکیبی مرحله ۵ با سقف ۱۰۰ پست
  ۲) امتیازدهی قطعی: 10 (اگر topic در interests) + 0.1 * max(0, 48 - hours_age)، گرد شده تا ۶ رقم
  ۳) ترتیب نهایی: (score, published_at, post_id) نزولی، ۲۰ پست برتر
  ۴) ثبت impression و click در Cassandra و محاسبه CTR به تفکیک روز و نسخه مدل

پیش‌نیاز: جداول مراحل ۱ تا ۳. دو جدول جدید را خود این فایل می‌سازد.
اجرا:  python stage6.py
"""
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Set, Tuple

from cassandra.cluster import Cluster

from stage5 import (CELEBRITY_AUTHORS, FEED_SIZE, FOLLOWED, REF, WINDOW_HOURS, FeedResult, HybridFeedService,
                    Post, default_run_many, expected_chrono, load_scenario, utc)

CANDIDATE_LIMIT = 100
MODEL_CHRONO = "chrono_v0"      # فید زمانی خالص (خروجی مرحله ۵)
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
NEW_TABLES = ["feed_impressions_by_day", "feed_clicks_by_impression"]


@dataclass
class CtrResult:
    impressions: int
    clicked_impressions: int
    ctr: float


def score_post(post: Post, interests: Set[str], ref_time: datetime) -> float:
    hours_age = (ref_time - post.published_at).total_seconds() / 3600.0
    base = 10.0 if post.topic in interests else 0.0
    return round(base + 0.1 * max(0.0, WINDOW_HOURS - hours_age), 6)


class PersonalizedFeedService(HybridFeedService):
    def __init__(self, session, celebrities: Set[str], run_many=None, concurrency: int = 32):
        for q in NEW_TABLES_CQL:           # قبل از prepare باید جدول‌ها وجود داشته باشند
            session.execute(q)
        super().__init__(session, celebrities, run_many, concurrency)
        p = session.prepare
        self.sel_interests = p("SELECT interests FROM ravand.users WHERE user_id = ?")
        self.ins_impression = p("""INSERT INTO ravand.feed_impressions_by_day
            (day_bucket, model_version, impression_id, user_id, post_id, shown_at) VALUES (?, ?, ?, ?, ?, ?)""")
        self.ins_click = p("""INSERT INTO ravand.feed_clicks_by_impression
            (impression_id, click_id, clicked_at) VALUES (?, ?, ?)""")
        self.sel_impressions = p("""SELECT impression_id FROM ravand.feed_impressions_by_day
            WHERE day_bucket = ? AND model_version = ?""")
        self.sel_first_click = p("SELECT click_id FROM ravand.feed_clicks_by_impression WHERE impression_id = ? LIMIT 1")

    # ---------- رتبه‌بندی ----------
    def get_user_interests(self, user_id: str) -> Set[str]:
        rows = self._read(self.sel_interests, (user_id,))
        return set(rows[0].interests or []) if rows else set()

    def rank(self, candidates: List[Post], interests: Set[str], ref_time: datetime) -> Tuple[List[Post], Dict[str, float]]:
        ref_time = utc(ref_time)
        scores = {p.post_id: score_post(p, interests, ref_time) for p in candidates}
        ranked = sorted(candidates, key=lambda p: (scores[p.post_id], p.published_at, p.post_id), reverse=True)
        return ranked[:FEED_SIZE], scores

    def get_feed(self, user_id: str, ref_time: datetime, model_version: str = MODEL_INTEREST) -> FeedResult:
        r0 = self.db_reads
        t0 = time.perf_counter()
        limit = FEED_SIZE if model_version == MODEL_CHRONO else CANDIDATE_LIMIT
        cands = self.generate_candidates(user_id, ref_time, limit)
        t1 = time.perf_counter()
        if model_version == MODEL_CHRONO:
            final, scores = cands[:FEED_SIZE], {}
        else:
            final, scores = self.rank(cands, self.get_user_interests(user_id), ref_time)
        t2 = time.perf_counter()
        return FeedResult(final, (t1 - t0) * 1000, self.db_reads - r0, len(cands), scores, (t2 - t1) * 1000)

    # ---------- ثبت نمایش و کلیک ----------
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
#                                  تست‌های مرحله ۶
# ==================================================================================
def run_stage6_verification(session, run_many=None):
    run_many = run_many or default_run_many(session)
    svc, events = load_scenario(session, run_many, PersonalizedFeedService, extra_tables=NEW_TABLES)
    # (جدول‌های جدید در سازنده سرویس ساخته شده‌اند؛ بعد از load_scenario خالی‌اند)

    print("=" * 78)
    print("تحویل ۶: عدم نفوذ پست غیرمجاز (دنبال‌نشده / خارج از ۴۸ ساعت) حتی با امتیاز بالا")
    print("=" * 78)
    start = REF - timedelta(hours=WINDOW_HOURS)
    for user in ("sara", "amin"):
        res = svc.get_feed(user, REF, MODEL_INTEREST)
        shown = {p.post_id for p in res.posts}
        assert not shown & {"celeb2_001", "stranger_001", "old_celeb", "old_active"}
        assert all(start <= p.published_at <= REF and p.author_id in set(FOLLOWED) for p in res.posts)
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
    assert [p.topic for p in feeds["sara"].posts][:10] == ["tech"] * 10
    assert [p.topic for p in feeds["amin"].posts][:10] == ["travel"] * 10
    assert [p.post_id for p in feeds["sara"].posts] != [p.post_id for p in feeds["amin"].posts]
    c_s = [p.post_id for p in svc.get_feed("sara", REF, MODEL_CHRONO).posts]
    c_a = [p.post_id for p in svc.get_feed("amin", REF, MODEL_CHRONO).posts]
    assert c_s == c_a == expected_chrono(events, set(FOLLOWED), FEED_SIZE)
    print("\nPASS  فید زمانیِ دو کاربر یکسان است ولی فید شخصی‌سازی‌شده‌شان متفاوت و مطابق علاقه است")

    print("\n" + "=" * 78)
    print("تحویل ۷: زمان تولید کاندیدا / زمان رتبه‌بندی (میلی‌ثانیه) و تکرارپذیری")
    print("=" * 78)
    r1 = svc.get_feed("sara", REF, MODEL_INTEREST)
    r2 = svc.get_feed("sara", REF, MODEL_INTEREST)
    print(f"candidate_generation = {r1.candidate_ms:.3f} ms | ranking = {r1.rank_ms:.3f} ms | "
          f"db_reads = {r1.db_reads} | candidates = {r1.candidates_count}")
    assert [p.post_id for p in r1.posts] == [p.post_id for p in r2.posts] and r1.scores == r2.scores
    svc.publish([e for e in events if e.post_id == "active_000"][0])      # replay انتشار
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
    svc.log_click(ids[res.posts[0].post_id], "click_second_on_same_impression", REF + timedelta(seconds=9))
    svc.log_click("ghost|impression", "ghost_click", REF)                         # impression وجود ندارد

    for model in (MODEL_CHRONO, MODEL_INTEREST):
        c = svc.compute_ctr(REF.date(), model)
        imp, clk = expected[model]
        assert (c.impressions, c.clicked_impressions) == (imp, clk), (model, c, expected[model])
        print(f"day={REF.date()} model={model:<12} impressions={c.impressions:<3} "
              f"clicked_impressions={c.clicked_impressions:<3} CTR={c.ctr:.3f}")
    print("PASS  نمایش تکراری، چند کلیک روی یک impression و کلیکِ بدون impression، CTR را باد نکردند")


if __name__ == "__main__":
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    run_stage6_verification(session)
    print("\nSTAGE 6: ALL CHECKS PASSED")
    cluster.shutdown()
