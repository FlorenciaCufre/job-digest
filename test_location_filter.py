"""Regression cases for the location filter.

Every string here was taken from a real digest email between 21 Aug and
9 Sep 2026, or is a punctuation variant of one. Run: python3 test_location_filter.py
"""
import scraper as s

# (location, should_location_ok_pass, expected_country_flag)
CASES = [
    # ── Leaked into a real digest and should not have ────────────────────
    ("Remote- UK",              False, ""),   # Everway, 4 Sep 2026
    ("Remote, Poland",          True,  "Poland"),      # Moniepoint, 21 Aug
    ("Remote, PL",              True,  "Poland"),      # Simple Life App, 31 Aug
    ("Portugal Remote",         True,  "Portugal"),    # OutSystems, 21 Aug
    ("Germany (Remote)",        True,  "Germany"),     # Pliant, 1 Sep

    # ── Punctuation variants of the same exclusions ──────────────────────
    ("Remote - UK",             False, ""),
    ("Remote · UK",             False, ""),
    ("Remote, UK",              False, ""),
    ("Remote (UK)",             False, ""),
    ("Remote/UK",               False, ""),
    ("Remote — United States",  False, ""),
    ("Remote (USA)",            False, ""),
    ("Remote- USA",             False, ""),
    ("US Remote",               False, ""),
    ("Remote US",               False, ""),
    ("Remote, US only",         False, ""),
    ("Remote · North America",  False, ""),
    ("Remote, Canada",          False, ""),
    ("United Kingdom",          False, ""),

    # ── Multi-region: an in-scope region means it IS open to her ─────────
    # Real jobGeo strings from Jobicy, 16 Sep 2026. All of these were being
    # dropped silently because an out-of-scope region was also listed.
    ("Europe, USA",                   True,  ""),
    ("APAC, LATAM, Canada, Europe",   True,  ""),
    ("Europe, Türkiye",               True,  ""),
    ("EMEA, Americas",                True,  ""),
    ("Remote - UK, Europe",           True,  ""),
    ("Worldwide, USA",                True,  ""),

    # ...but a region list with nothing in scope is still out
    ("USA, Canada",                   False, ""),
    ("Remote - USA, LATAM",           False, ""),

    # ── Bare European country: shown and badged, not silently dropped ────
    ("France",                        True,  "France"),
    ("Hungary",                       True,  "Hungary"),
    ("Bulgaria, Cyprus, Poland",      True,  "Bulgaria / Cyprus / Poland"),

    # ── Must keep passing, unflagged ─────────────────────────────────────
    ("Remote",                  True,  ""),
    ("EMEA",                    True,  ""),
    ("Remote / EU",             True,  ""),
    ("Remote International",    True,  ""),   # Sweedpos, 8 Sep
    ("Anywhere in the World",   True,  ""),
    ("Europe (Remote)",         True,  ""),
    ("Barcelona, Spain",        True,  ""),
    ("Remote Spain",            True,  ""),
    ("Poland, Spain",           True,  ""),   # Pnlfin — Spain is in scope
    ("Remote Catalunya",        True,  ""),
    # A bare city with no remote/Spain/Europe signal has always failed the
    # allow-list — unchanged. What matters here is that "de" is not read as
    # Germany, so no country flag either way.
    ("Palma de Mallorca",       False, ""),
    ("Palma de Mallorca, Spain (Remote)", True, ""),
    ("Remote, Europe / Poland", True,  ""),   # broad region wins
]


# ── Geography hidden in the job title ────────────────────────────────────────
# Every location rule reads the location FIELD. These postings put the place in
# the title and left the field as a bare "Remote".
# (title, should_be_excluded)
TITLE_CASES = [
    # Reached a real digest with location == "Remote"
    ("Senior Product Designer, London, UK",                              True),
    ("Lead Product Designer / Usability Analytics- Remote, PST Time Zone", True),

    # Same shape, other spellings
    ("Senior Product Designer (US)",                   True),
    ("Senior Product Designer — Remote (USA)",         True),
    ("Lead Product Designer, EST",                     True),
    ("Senior Product Designer - Pacific Time",         True),
    ("Senior Product Designer, New York",              True),
    ("Lead Product Designer, Toronto",                 True),
    ("Senior Product Designer, North America",         True),
    ("Senior Product Designer — US based",             True),
    ("Senior Product Designer, APAC",                  True),

    # Seniority exclusions must still work
    ("VP of Design",                                   True),
    ("VP, Product Design",                             True),
    ("Head of Design",                                 True),
    ("Design Director",                                True),
    ("Chief Design Officer",                           True),

    # "cdo" as loose letters used to fire inside ordinary words
    ("Senior Product Designer, McDonald's",            False),
    ("Senior Product Designer, Greatest Hits",         False),   # contains "est"

    # Must keep passing
    ("Senior Product Designer",                        False),
    ("Lead Product Designer, Growth",                  False),
    ("Senior Product Designer — Remote, Europe",       False),
    ("Senior Product Designer, Barcelona",             False),
    ("Lead Product Designer (EMEA)",                   False),
    ("Senior Product Designer, Design Systems",        False),
]

# ── Blocklist reads the URL, not just the company name ───────────────────────
# (company, url, should_be_blocked)
BLOCKLIST_CASES = [
    ("Chess",       "https://ats.rippling.com/chess/jobs/02e2145b",  True),   # 14 Sep 2026
    ("Accelint",    "https://ats.rippling.com/accelint/jobs/e6d4ad", True),   # 21 Aug 2026
    ("Figma",       "https://boards.greenhouse.io/figma/jobs/1",     True),
    ("Pennylane",   "https://jobs.ashbyhq.com/pennylane/abc",        False),
    ("Too Good To Go", "https://job-boards.greenhouse.io/tgtg/1",    False),
]

# ── Watchlist location gate ──────────────────────────────────────────────────
# Watchlist roles used to skip location filtering entirely.
# (location, should_pass)
WATCHLIST_CASES = [
    ("San Francisco, CA",     False),
    ("Remote - US",           False),
    ("New York",              False),
    ("",                      True),    # unknown passes, but gets badged
    ("Remote / EU",           True),
    ("Barcelona, Spain",      True),
    ("Remote, EMEA",          True),
]


# ── Jobicy parsing ───────────────────────────────────────────────────────────
# Real response shape from jobicy.com/api/v2/remote-jobs, 16 Sep 2026.
JOBICY_PAYLOAD = {"jobs": [
    {"jobTitle": "Staff Product Designer", "companyName": "OpenSea", "jobGeo": "Europe, USA",
     "url": "https://jobicy.com/j/1", "pubDate": "2026-09-15", "jobExcerpt": ""},
    {"jobTitle": "Senior Product Designer", "companyName": "Kinsta", "jobGeo": "Hungary",
     "url": "https://jobicy.com/j/2", "pubDate": "2026-09-15", "jobExcerpt": ""},
    {"jobTitle": "Senior Product Designer", "companyName": "Ashby", "jobGeo": "USA",
     "url": "https://jobicy.com/j/3", "pubDate": "2026-09-15", "jobExcerpt": ""},
    {"jobTitle": "Marketing Designer", "companyName": "Playson", "jobGeo": "Europe",
     "url": "https://jobicy.com/j/4", "pubDate": "2026-09-15", "jobExcerpt": ""},
    {"jobTitle": "Senior Product Designer", "companyName": "Acme", "jobGeo": "Europe",
     "url": "https://jobicy.com/j/5", "pubDate": "2026-09-15", "jobExcerpt": "",
     "salaryMin": 60000, "salaryMax": 80000, "salaryCurrency": "EUR"},
]}


class _FakeJobicyResponse:
    status_code = 200
    def raise_for_status(self): pass
    def json(self): return JOBICY_PAYLOAD


def check_jobicy(failures):
    original = s.requests.get
    s.requests.get = lambda url, **kw: _FakeJobicyResponse()
    try:
        jobs = s.scrape_jobicy()
    finally:
        s.requests.get = original

    got = {(j["company"], j["location"]) for j in jobs}
    expected = {
        ("OpenSea", "Europe, USA"),   # multi-region including Europe — was being dropped
        ("Kinsta",  "Hungary"),       # single country — shown and badged, not dropped
        ("Acme",    "Europe"),
    }
    if got != expected:
        failures.append(f"scrape_jobicy returned {sorted(got)}, expected {sorted(expected)}")

    if s.SOURCE_RAW_COUNTS.get("Jobicy") != len(JOBICY_PAYLOAD["jobs"]):
        failures.append("scrape_jobicy did not record its raw listing count")

    salaried = [j for j in jobs if j["company"] == "Acme"]
    if not salaried or "60,000" not in salaried[0]["salary"]:
        failures.append(f"scrape_jobicy lost the salary: {salaried and salaried[0]['salary']!r}")

    return 3   # number of checks above


def main():
    failures = []
    for loc, want_ok, want_country in CASES:
        got_ok = s.location_ok(loc)
        got_country = s.country_restriction(loc)
        if got_ok != want_ok:
            failures.append(f"location_ok({loc!r}) = {got_ok}, expected {want_ok}")
        if got_country != want_country:
            failures.append(
                f"country_restriction({loc!r}) = {got_country!r}, expected {want_country!r}"
            )

    for title, want_excluded in TITLE_CASES:
        got = s.title_is_excluded(title)
        if got != want_excluded:
            failures.append(
                f"title_is_excluded({title!r}) = {got}, expected {want_excluded}"
            )

    for company, url, want_blocked in BLOCKLIST_CASES:
        got = s.is_blocked_company(company, url)
        if got != want_blocked:
            failures.append(
                f"is_blocked_company({company!r}, {url!r}) = {got}, expected {want_blocked}"
            )

    for loc, want_pass in WATCHLIST_CASES:
        got = s._watchlist_location_ok(loc)
        if got != want_pass:
            failures.append(
                f"_watchlist_location_ok({loc!r}) = {got}, expected {want_pass}"
            )

    # A watchlist row with no location must say so, not invent one.
    job = s._watchlist_job("Senior Product Designer", "Hostaway", "http://x", "", "", 1)
    if not job.get("location_unknown"):
        failures.append("watchlist job with no location is not flagged location_unknown")
    if "EU" in job["location"]:
        failures.append(
            f"watchlist job with no location invented one: {job['location']!r}"
        )

    jobicy_checks = check_jobicy(failures)

    total = (len(CASES) * 2 + len(TITLE_CASES) + len(BLOCKLIST_CASES)
             + len(WATCHLIST_CASES) + 2 + jobicy_checks)
    for f in failures:
        print("FAIL:", f)
    print(f"\n{total - len(failures)}/{total} checks passed.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
