"""Regression cases for the wall-clock limit.

On 15 September 2026 a scheduled run hung for 3h27m on a source that accepted
the connection and then never answered. `requests`' own timeout does not catch
that — it limits the gap between bytes, not total time. These checks prove a
stalled source is abandoned, reported, and does not take the run down with it.

Run: python3 test_timeout.py
"""
import time
import scraper as s

failures = []


def check(label, ok, detail=""):
    if ok:
        print(f"  ok    {label}")
    else:
        print(f"  FAIL  {label}  {detail}")
        failures.append(label)


print("1. a blocked read is interrupted, not waited out")
t0 = time.monotonic()
try:
    with s.time_limit(1, "stuck-source"):
        time.sleep(30)          # stands in for a socket that never answers
    check("raised SourceTimeout", False, "no exception")
except s.SourceTimeout as e:
    el = time.monotonic() - t0
    check("raised SourceTimeout", True)
    check("abandoned in about 1s", el < 3, f"took {el:.1f}s")
    check("names the source", "stuck-source" in str(e), str(e))

print("\n2. the alarm is cleared when work finishes early")
with s.time_limit(5, "fast"):
    pass
t0 = time.monotonic()
time.sleep(1.5)                  # would raise here if the alarm still stood
check("no stray alarm after the block", time.monotonic() - t0 >= 1.4)

print("\n3. a nested/reused limit still works afterwards")
try:
    with s.time_limit(1, "again"):
        time.sleep(30)
    check("second use still fires", False)
except s.SourceTimeout:
    check("second use still fires", True)

print("\n4. budgets are the ones intended")
check("watchlist gets the longer budget",
      s._timeout_for("Watchlist") == s.WATCHLIST_TIMEOUT_SECONDS)
check("everything else gets the standard budget",
      s._timeout_for("UXJobs") == s.SOURCE_TIMEOUT_SECONDS)
check("standard budget is under the job cap (20 min)",
      s.SOURCE_TIMEOUT_SECONDS * 9 + s.WATCHLIST_TIMEOUT_SECONDS < 20 * 60,
      f"worst case {s.SOURCE_TIMEOUT_SECONDS * 9 + s.WATCHLIST_TIMEOUT_SECONDS}s")

print("\n5. requests get a (connect, read) pair, not a bare number")
check("HTTP_TIMEOUT is a tuple", isinstance(s.HTTP_TIMEOUT, tuple) and len(s.HTTP_TIMEOUT) == 2,
      repr(s.HTTP_TIMEOUT))

print("\n6. a timed-out source is reported and the others still run")
calls = []


def hangs():
    calls.append("hang")
    time.sleep(30)
    return []


def works():
    calls.append("work")
    return [{"title": "Senior Product Designer", "company": "Fine", "location": "Remote, Europe",
             "url": "https://x.co/1", "source": "Good", "four_day": False, "spain_flag": False,
             "currency_flag": "", "age_label": "Today", "age_date": None,
             "is_stretch": False, "salary": ""}]


original, original_budget = s.SCRAPERS, s.SOURCE_TIMEOUT_SECONDS
s.SCRAPERS = [("Stuck", hangs), ("Good", works)]
s.SOURCE_TIMEOUT_SECONDS = 1
try:
    t0 = time.monotonic()
    jobs, health, alerts = s.collect_all_jobs({})
    el = time.monotonic() - t0
    check("both sources were attempted", calls == ["hang", "work"], str(calls))
    check("the healthy source still returned its job", len(jobs) == 1, f"{len(jobs)} jobs")
    check("the hang did not stall the run", el < 5, f"took {el:.1f}s")
    check("the stall is recorded as an error", health.get("Stuck", {}).get("error_streak") == 1,
          str(health.get("Stuck")))
finally:
    s.SCRAPERS, s.SOURCE_TIMEOUT_SECONDS = original, original_budget

print(f"\n{'ALL PASSED' if not failures else str(len(failures)) + ' FAILED'}")
raise SystemExit(1 if failures else 0)
