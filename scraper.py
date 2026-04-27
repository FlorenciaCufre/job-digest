#!/usr/bin/env python3
"""
Job Scraper – Senior / Lead Product Designer
Runs daily via GitHub Actions and sends a digest email via Resend.

Sources (4 APIs + 3 HTML scrapers + watchlist):
  APIs:    Remotive, 4DayWeek, Himalayas, Arbeitnow
  Scrapers: WeWorkRemotely, WorkingNomads, Nodesk
  Watchlist: 23 pre-vetted companies via Lever / Ashby / HTML

Changes in this version:
  - Auto-prune seen_jobs: entries older than 30 days removed at run start
  - UK/United Kingdom added to hard-exclude location list
  - Staff / Principal roles separated into a "stretch roles" section
  - Salary sanity cap: values over 500k hidden (display bug guard)
  - Retry logic on transient errors (1 retry, 5s wait)
  - Arbeitnow pagination guard (empty body no longer crashes)
  - Smarter health check: flags sources with 0 raw results for 7+ days
  - Silence-breaker email after 3 days of no send
  - Dead HTML scrapers removed: EURemoteJobs, DailyRemote, RemotifyEurope, RemoteRocketship
  - Watchlist URLs fixed: Apaleo, Pitch
  - Source attribution standardised
"""

import os
import json
import hashlib
import datetime
import time
import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from dateutil import parser as dateparser

# ── Configuration ─────────────────────────────────────────────────────────────

RESEND_API_KEY  = os.environ.get("RESEND_API_KEY", "")
EMAIL_TO        = os.environ.get("EMAIL_TO", "")
EMAIL_FROM      = os.environ.get("EMAIL_FROM", "jobs@yourdomain.com")
SEEN_JOBS_FILE  = Path("seen_jobs.json")
HEALTH_FILE     = Path("source_health.json")

REPOST_DAYS     = 14    # resurface a seen job if reposted after this many days
PRUNE_DAYS      = 30    # remove seen_jobs entries not seen for this many days
SILENCE_DAYS    = 3     # send a health ping if no email sent for this many days
SALARY_MAX      = 500_000  # sanity cap — values above this are display bugs

TITLE_KEYWORDS = [
    "lead product designer",
    "senior product designer",
    "lead designer",
    "head of product design",
]

# Stretch roles — shown in a separate section, lower priority
STRETCH_TITLE_KEYWORDS = [
    "principal product designer",
    "staff product designer",
    "principal designer",
    "staff designer",
]

LOCATION_KEYWORDS = [
    "remote", "spain", "barcelona", "europe", "eu", "worldwide",
    "anywhere", "global", "emea",
]

SPAIN_ONLY_SIGNALS = [
    "spain only", "based in spain", "barcelona only",
    "madrid only", "must be in spain",
]

EXCLUDE_LOCATION = [
    # US
    "us only", "usa only", "united states only",
    "must be located in the us",
    "remote · usa", "remote - usa", "remote, usa",
    "remote · united states", "remote - united states", "remote, united states",
    "united states", " usa",
    # Canada
    "canada only",
    # UK — added
    "uk only", "united kingdom only",
    "remote · united kingdom", "remote - united kingdom", "remote, united kingdom",
    "remote · uk", "remote - uk", "remote, uk",
    "united kingdom",
]

US_DESCRIPTION_SIGNALS = [
    "401(k)", "401k",
    "must be authorized to work in the us",
    "must be authorized to work in the united states",
    "us work authorization",
    "authorized to work in the us",
    "eligible to work in the us",
]

USD_SIGNALS = ["usd", "$ ", "us$"]
GBP_SIGNALS = ["gbp", "£"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

TODAY = datetime.date.today()

# ── Persistence helpers ───────────────────────────────────────────────────────

def load_seen() -> dict:
    if SEEN_JOBS_FILE.exists():
        return json.loads(SEEN_JOBS_FILE.read_text())
    return {}

def save_seen(seen: dict):
    SEEN_JOBS_FILE.write_text(json.dumps(seen, indent=2))

def load_health() -> dict:
    if HEALTH_FILE.exists():
        return json.loads(HEALTH_FILE.read_text())
    return {}

def save_health(health: dict):
    HEALTH_FILE.write_text(json.dumps(health, indent=2))

def prune_seen(seen: dict) -> tuple[dict, int]:
    """Remove entries not seen in the last PRUNE_DAYS days. Returns pruned dict + count removed."""
    cutoff = TODAY - datetime.timedelta(days=PRUNE_DAYS)
    pruned = {}
    removed = 0
    for jid, record in seen.items():
        try:
            last = datetime.date.fromisoformat(record["last_seen"])
            if last >= cutoff:
                pruned[jid] = record
            else:
                removed += 1
        except Exception:
            pruned[jid] = record  # keep if date is unreadable
    return pruned, removed

# ── Matching helpers ──────────────────────────────────────────────────────────

def job_id(title: str, company: str) -> str:
    raw = f"{title.lower().strip()}-{company.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()

def title_matches(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in TITLE_KEYWORDS)

def title_is_stretch(title: str) -> bool:
    t = title.lower()
    return (not title_matches(title)) and any(kw in t for kw in STRETCH_TITLE_KEYWORDS)

def title_matches_any(title: str) -> bool:
    """Matches either primary or stretch keywords."""
    return title_matches(title) or title_is_stretch(title)

def location_ok(location: str) -> bool:
    loc = location.lower().strip()
    if not loc:
        return True
    if any(ex in loc for ex in EXCLUDE_LOCATION):
        return False
    if loc in ("usa", "united states", "us", "remote usa", "remote us",
               "uk", "united kingdom", "remote uk"):
        return False
    return any(kw in loc for kw in LOCATION_KEYWORDS)

def is_us_description(description: str) -> bool:
    if not description:
        return False
    d = description.lower()
    return any(sig in d for sig in US_DESCRIPTION_SIGNALS)

def currency_flag(salary: str) -> str:
    if not salary:
        return ""
    s = salary.lower()
    if any(sig in s for sig in USD_SIGNALS):
        return "usd"
    if any(sig in s for sig in GBP_SIGNALS):
        return "gbp"
    return ""

def is_spain_only(location: str) -> bool:
    loc = location.lower()
    if any(sig in loc for sig in SPAIN_ONLY_SIGNALS):
        return True
    has_spain = any(x in loc for x in ["spain", "barcelona", "madrid"])
    has_remote = any(x in loc for x in ["remote", "anywhere", "worldwide", "global"])
    return has_spain and not has_remote

def sanitise_salary(salary: str) -> str:
    """Return empty string if salary looks like a display bug (> SALARY_MAX)."""
    if not salary:
        return ""
    # Extract the largest number in the string
    nums = re.findall(r"[\d,]+", salary.replace(",", ""))
    for n in nums:
        try:
            if int(n) > SALARY_MAX:
                return ""
        except ValueError:
            pass
    return salary

# ── Age helpers ───────────────────────────────────────────────────────────────

def parse_age(posted_at) -> tuple[str, datetime.date | None]:
    if not posted_at:
        return "Date unknown", None

    date = None

    if isinstance(posted_at, (int, float)):
        try:
            date = datetime.datetime.utcfromtimestamp(posted_at).date()
        except Exception:
            pass

    if date is None and isinstance(posted_at, str):
        lower = posted_at.lower().strip()
        today = TODAY
        relative_map = [
            (r"today|just now|less than a day", 0),
            (r"yesterday",                      1),
            (r"(\d+)\s*day",                    None),
            (r"a day",                          1),
            (r"a week|1 week",                  7),
            (r"(\d+)\s*week",                   None),
            (r"a month|1 month",                30),
            (r"(\d+)\s*month",                  None),
        ]
        matched = False
        for pattern, days in relative_map:
            m = re.search(pattern, lower)
            if m:
                if days is not None:
                    date = today - datetime.timedelta(days=days)
                else:
                    n = int(m.group(1))
                    if "day" in pattern:
                        date = today - datetime.timedelta(days=n)
                    elif "week" in pattern:
                        date = today - datetime.timedelta(weeks=n)
                    elif "month" in pattern:
                        date = today - datetime.timedelta(days=n * 30)
                matched = True
                break
        if not matched:
            try:
                date = dateparser.parse(posted_at).date()
            except Exception:
                pass

    if date is None:
        return "Date unknown", None

    delta = (TODAY - date).days
    if delta == 0:
        label = "Today"
    elif delta == 1:
        label = "Yesterday"
    elif delta <= 6:
        label = f"{delta} days ago"
    elif delta <= 13:
        label = f"{delta // 7} week ago"
    elif delta <= 20:
        label = f"{delta // 7} weeks ago"
    else:
        label = date.strftime("%-d %b")

    return label, date

def age_color(date: datetime.date | None) -> str:
    if date is None:
        return "#9ca3af"
    delta = (TODAY - date).days
    if delta <= 3:
        return "#059669"
    if delta <= 10:
        return "#d97706"
    return "#9ca3af"

# ── Fetch helper with retry ───────────────────────────────────────────────────

def fetch(url: str, timeout: int = 15, retries: int = 1) -> BeautifulSoup | None:
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            # Retry on 429 (rate limit) or 5xx (server error)
            if attempt < retries and status in (429, 500, 502, 503, 504):
                print(f"  ↻ Retry {attempt + 1} for {url} (status {status})")
                time.sleep(5)
                continue
            print(f"  ⚠ Could not fetch {url}: {e}")
            return None
        except Exception as e:
            if attempt < retries:
                print(f"  ↻ Retry {attempt + 1} for {url}: {e}")
                time.sleep(5)
                continue
            print(f"  ⚠ Could not fetch {url}: {e}")
            return None
    return None

# ── Scrapers ──────────────────────────────────────────────────────────────────

def scrape_remotive() -> list[dict]:
    jobs = []
    try:
        r = requests.get(
            "https://remotive.com/api/remote-jobs",
            params={"category": "Design", "limit": 100},
            timeout=15,
        )
        for j in r.json().get("jobs", []):
            title = j.get("title", "")
            if not title_matches_any(title):
                continue
            location = j.get("candidate_required_location", "")
            if not location_ok(location):
                continue
            description = j.get("description", "") or ""
            if is_us_description(description):
                continue
            salary = sanitise_salary(j.get("salary", "") or "")
            age_label, age_date = parse_age(j.get("publication_date") or j.get("posted"))
            jobs.append({
                "title":         title,
                "company":       j.get("company_name", ""),
                "location":      location or "Remote",
                "salary":        salary,
                "url":           j.get("url", ""),
                "source":        "Remotive",
                "four_day":      False,
                "spain_flag":    is_spain_only(location),
                "currency_flag": currency_flag(salary),
                "age_label":     age_label,
                "age_date":      age_date,
                "is_stretch":    title_is_stretch(title),
            })
    except Exception as e:
        print(f"  ⚠ Remotive error: {e}")
    return jobs


def scrape_4dayweek() -> list[dict]:
    jobs = []
    page = 1
    while True:
        try:
            r = requests.get(
                "https://4dayweek.io/api/v2/jobs",
                params={
                    "category":         "design",
                    "level":            "senior,lead",
                    "work_arrangement": "remote",
                    "limit":            100,
                    "page":             page,
                },
                timeout=15,
            )
            data = r.json()
            items = data.get("data", [])
            if not items:
                break
            for j in items:
                title = j.get("title", "") or j.get("role", "")
                if not title_matches_any(title):
                    continue

                remote_allowed = j.get("remote_allowed", [])
                if remote_allowed:
                    countries = [loc.get("country", "").lower() for loc in remote_allowed]
                    non_eu = [c for c in countries if c not in (
                        "united states", "usa", "us", "canada",
                        "united kingdom", "uk",
                    )]
                    if countries and not non_eu:
                        continue
                    country_display = [loc.get("country", "") for loc in remote_allowed]
                    location = "Remote – " + ", ".join(c for c in country_display if c) if country_display else "Remote"
                else:
                    location = "Remote"

                if not location_ok(location):
                    continue

                description = j.get("description", "") or ""
                if is_us_description(description):
                    continue

                sal_min = j.get("salary_min")
                sal_max = j.get("salary_max")
                cur = j.get("salary_currency", "")
                if sal_min and sal_max:
                    salary = f"{cur} {sal_min:,} – {sal_max:,}"
                elif sal_min:
                    salary = f"{cur} {sal_min:,}+"
                else:
                    salary = ""
                salary = sanitise_salary(salary)

                age_label, age_date = parse_age(j.get("posted_at"))
                jobs.append({
                    "title":         title,
                    "company":       j.get("company", {}).get("name", "") if isinstance(j.get("company"), dict) else "",
                    "location":      location,
                    "salary":        salary,
                    "url":           j.get("url", ""),
                    "source":        "4DayWeek",
                    "four_day":      True,
                    "spain_flag":    False,
                    "currency_flag": currency_flag(salary),
                    "age_label":     age_label,
                    "age_date":      age_date,
                    "is_stretch":    title_is_stretch(title),
                })
            if not data.get("has_more"):
                break
            page += 1
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠ 4DayWeek error (page {page}): {e}")
            break
    return jobs


def scrape_himalayas() -> list[dict]:
    jobs = []
    offset = 0
    while True:
        try:
            r = requests.get(
                "https://himalayas.app/jobs/api/search",
                params={
                    "q":         "product designer",
                    "seniority": "senior,lead",
                    "limit":     20,
                    "offset":    offset,
                },
                headers=HEADERS,
                timeout=15,
            )
            data = r.json()
            items = data if isinstance(data, list) else data.get("jobs", [])
            if not items:
                break
            for j in items:
                title = j.get("title", "")
                if not title_matches_any(title):
                    continue
                restrictions = j.get("locationRestrictions", []) or []
                location = ", ".join(restrictions) if restrictions else "Remote"
                if not location_ok(location):
                    continue
                salary = sanitise_salary(_himalayas_salary(j))
                age_label, age_date = parse_age(j.get("pubDate"))
                jobs.append({
                    "title":         title,
                    "company":       j.get("companyName", ""),
                    "location":      location,
                    "salary":        salary,
                    "url":           j.get("applicationLink", ""),
                    "source":        "Himalayas",
                    "four_day":      False,
                    "spain_flag":    is_spain_only(location),
                    "currency_flag": currency_flag(salary),
                    "age_label":     age_label,
                    "age_date":      age_date,
                    "is_stretch":    title_is_stretch(title),
                })
            if len(items) < 20:
                break
            offset += 20
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠ Himalayas error (offset {offset}): {e}")
            break
    return jobs

def _himalayas_salary(j: dict) -> str:
    lo = j.get("minSalary")
    hi = j.get("maxSalary")
    cur = j.get("currency", "")
    if lo and hi:
        return f"{cur} {int(lo):,} – {int(hi):,}"
    if lo:
        return f"{cur} {int(lo):,}+"
    return ""


def scrape_arbeitnow() -> list[dict]:
    jobs = []
    page = 1
    while True:
        try:
            r = requests.get(
                "https://www.arbeitnow.com/api/job-board-api",
                params={"page": page},
                timeout=15,
            )
            # Guard: empty or non-JSON body (happens on last page)
            if not r.content or not r.content.strip():
                break
            try:
                data = r.json()
            except ValueError:
                break

            items = data.get("data", [])
            if not items:
                break
            for j in items:
                title = j.get("title", "")
                if not title_matches_any(title):
                    continue
                if not j.get("remote", False):
                    continue
                location = j.get("location", "") or "Remote"
                if not location_ok(location):
                    continue
                age_label, age_date = parse_age(j.get("created_at") or j.get("date"))
                jobs.append({
                    "title":         title,
                    "company":       j.get("company_name", ""),
                    "location":      location,
                    "salary":        "",
                    "url":           j.get("url", ""),
                    "source":        "Arbeitnow",
                    "four_day":      False,
                    "spain_flag":    is_spain_only(location),
                    "currency_flag": "",
                    "age_label":     age_label,
                    "age_date":      age_date,
                    "is_stretch":    title_is_stretch(title),
                })
            if not data.get("links", {}).get("next"):
                break
            page += 1
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠ Arbeitnow error (page {page}): {e}")
            break
    return jobs


def scrape_weworkremotely() -> list[dict]:
    jobs = []
    soup = fetch("https://weworkremotely.com/categories/remote-design-jobs.rss")
    if not soup:
        return jobs
    for item in soup.find_all("item"):
        title_tag = item.find("title")
        if not title_tag:
            continue
        raw = title_tag.text.strip()
        company, title = (raw.split(":", 1) if ":" in raw else ("", raw))
        company, title = company.strip(), title.strip()
        if not title_matches_any(title):
            continue
        region_tag = item.find("region")
        location = region_tag.text.strip() if region_tag else "Remote"
        if not location_ok(location):
            continue
        pub_date = item.find("pubdate") or item.find("pubDate")
        age_label, age_date = parse_age(pub_date.text.strip() if pub_date else None)
        link_tag = item.find("link")
        url = link_tag.next_sibling.strip() if link_tag else ""
        jobs.append({
            "title":         title,
            "company":       company,
            "location":      location,
            "salary":        "",
            "url":           url,
            "source":        "WeWorkRemotely",
            "four_day":      False,
            "spain_flag":    is_spain_only(location),
            "currency_flag": "",
            "age_label":     age_label,
            "age_date":      age_date,
            "is_stretch":    title_is_stretch(title),
        })
    return jobs


def _html_scraper(
    url: str,
    source: str,
    card_sel: str,
    title_sel: str,
    company_sel: str,
    location_sel: str,
    link_sel: str,
    base_url: str = "",
    default_location: str = "Remote",
    date_sel: str = "",
) -> list[dict]:
    jobs = []
    soup = fetch(url)
    if not soup:
        return jobs
    for card in soup.select(card_sel):
        title_el    = card.select_one(title_sel)
        company_el  = card.select_one(company_sel) if company_sel else None
        location_el = card.select_one(location_sel) if location_sel else None
        link_el     = card.select_one(link_sel) if link_sel else None
        date_el     = card.select_one(date_sel) if date_sel else None
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title_matches_any(title):
            continue
        location = location_el.get_text(strip=True) if location_el else default_location
        if not location_ok(location):
            continue
        company = company_el.get_text(strip=True) if company_el else ""
        href = link_el["href"] if link_el and link_el.has_attr("href") else ""
        url_full = f"{base_url}{href}" if href.startswith("/") else href
        raw_date = date_el.get_text(strip=True) if date_el else None
        age_label, age_date = parse_age(raw_date)
        jobs.append({
            "title":         title,
            "company":       company,
            "location":      location,
            "salary":        "",
            "url":           url_full,
            "source":        source,
            "four_day":      False,
            "spain_flag":    is_spain_only(location),
            "currency_flag": "",
            "age_label":     age_label,
            "age_date":      age_date,
            "is_stretch":    title_is_stretch(title),
        })
    return jobs


def scrape_workingnomads() -> list[dict]:
    return _html_scraper(
        url="https://www.workingnomads.com/jobs?tag=product-design&location=europe",
        source="WorkingNomads",
        card_sel=".job-item, [class*='job_item'], article",
        title_sel="h2, h3, h4, [class*='title']",
        company_sel="[class*='company'], .company",
        location_sel="[class*='location']",
        link_sel="a[href]",
        base_url="https://www.workingnomads.com",
        default_location="Europe / Remote",
        date_sel="[class*='date'], time",
    )

def scrape_nodesk() -> list[dict]:
    return _html_scraper(
        url="https://nodesk.co/remote-jobs/?query=product+design",
        source="Nodesk",
        card_sel="article, .job, [class*='job-item']",
        title_sel="h2, h3, [class*='title']",
        company_sel="[class*='company']",
        location_sel="[class*='location']",
        link_sel="a[href]",
        base_url="https://nodesk.co",
        date_sel="time, [class*='date']",
    )


# ── Watchlist scrapers ────────────────────────────────────────────────────────

WATCHLIST = [
    # Tier 1 — pursue actively
    {"name": "Hostaway",       "url": "https://careers.hostaway.com",                        "ats": "html",  "tier": 1},
    {"name": "Pennylane",      "url": "https://jobs.lever.co/pennylane",                     "ats": "lever", "tier": 1},
    {"name": "Dovetail",       "url": "https://dovetail.com/careers/",                       "ats": "html",  "tier": 1},
    {"name": "Too Good To Go", "url": "https://toogoodtogo.com/en/careers",                  "ats": "html",  "tier": 1},
    {"name": "Doctolib",       "url": "https://careers.doctolib.com",                        "ats": "html",  "tier": 1},
    {"name": "Pleo",           "url": "https://jobs.ashbyhq.com/pleo",                       "ats": "ashby", "tier": 1},
    # Tier 2 — monitor, apply when role appears
    {"name": "Productboard",   "url": "https://www.productboard.com/careers/open-positions/","ats": "html",  "tier": 2},
    {"name": "Automattic",     "url": "https://automattic.com/work-with-us/",                "ats": "html",  "tier": 2},
    {"name": "Synthesia",      "url": "https://www.synthesia.io/careers",                    "ats": "html",  "tier": 2},
    {"name": "Qonto",          "url": "https://jobs.lever.co/qonto",                         "ats": "lever", "tier": 2},
    {"name": "Alan",           "url": "https://jobs.alan.com",                               "ats": "html",  "tier": 2},
    {"name": "Attio",          "url": "https://jobs.ashbyhq.com/attio",                      "ats": "ashby", "tier": 2},
    {"name": "Intercom",       "url": "https://www.intercom.com/careers",                    "ats": "html",  "tier": 2},
    {"name": "Maze",           "url": "https://maze.co/careers/",                            "ats": "html",  "tier": 2},
    {"name": "TheyDo",         "url": "https://www.theydo.com/careers",                      "ats": "html",  "tier": 2},
    {"name": "Hotjar",         "url": "https://www.hotjar.com/careers/",                     "ats": "html",  "tier": 2},
    {"name": "PostHog",        "url": "https://posthog.com/careers",                         "ats": "html",  "tier": 2},
    {"name": "Apaleo",         "url": "https://job-boards.greenhouse.io/apaleo",             "ats": "greenhouse", "tier": 2},
    # Tier 3 — speculative / small teams / rare openings
    {"name": "Rows",           "url": "https://rows.com/careers",                            "ats": "html",  "tier": 3},
    {"name": "Raycast",        "url": "https://www.raycast.com/careers",                     "ats": "html",  "tier": 3},
    {"name": "Readdle",        "url": "https://readdle.com/careers",                         "ats": "html",  "tier": 3},
    {"name": "Pitch",          "url": "https://pitch.com/jobs",                              "ats": "html",  "tier": 3},
    {"name": "Granola",        "url": "https://www.granola.ai/jobs",                         "ats": "html",  "tier": 3},
]

WATCHLIST_TIER_LABELS = {1: "⭐ Tier 1", 2: "📌 Tier 2", 3: "🔍 Tier 3"}


def _watchlist_job(title, company, url, location, salary, tier) -> dict:
    loc = location or "Remote / EU"
    age_label, age_date = parse_age(None)
    salary = sanitise_salary(salary or "")
    return {
        "title":          title,
        "company":        company,
        "location":       loc,
        "salary":         salary,
        "url":            url,
        "source":         f"Watchlist · {company}",
        "four_day":       False,
        "spain_flag":     is_spain_only(loc),
        "currency_flag":  currency_flag(salary),
        "age_label":      age_label,
        "age_date":       age_date,
        "watchlist":      True,
        "watchlist_tier": tier,
        "is_stretch":     title_is_stretch(title),
    }


def _scrape_lever_watchlist(base_url: str, company_name: str, tier: int) -> list[dict]:
    slug = base_url.rstrip("/").split("/")[-1]
    try:
        r = requests.get(
            f"https://api.lever.co/v0/postings/{slug}?mode=json",
            timeout=15,
        )
        r.raise_for_status()
        jobs = []
        for p in r.json():
            title = p.get("text", "")
            if not title_matches_any(title):
                continue
            location = p.get("categories", {}).get("location", "")
            jobs.append(_watchlist_job(
                title, company_name,
                p.get("hostedUrl", base_url),
                location, "", tier,
            ))
        return jobs
    except Exception as e:
        print(f"  ⚠ Watchlist Lever ({company_name}): {e}")
        return []


def _scrape_ashby_watchlist(base_url: str, company_name: str, tier: int) -> list[dict]:
    slug = base_url.rstrip("/").split("/")[-1]
    try:
        r = requests.get(
            f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
            timeout=15,
        )
        r.raise_for_status()
        jobs = []
        for p in r.json().get("jobs", []):
            title = p.get("title", "")
            if not title_matches_any(title):
                continue
            loc = p.get("location") or p.get("locationName") or ""
            if isinstance(loc, list):
                loc = ", ".join(loc)
            jobs.append(_watchlist_job(
                title, company_name,
                p.get("jobUrl", base_url),
                loc, "", tier,
            ))
        return jobs
    except Exception as e:
        print(f"  ⚠ Watchlist Ashby ({company_name}): {e}")
        return []


def _scrape_greenhouse_watchlist(base_url: str, company_name: str, tier: int) -> list[dict]:
    """Greenhouse job board API — more reliable than scraping the HTML page."""
    slug = base_url.rstrip("/").split("/")[-1]
    try:
        r = requests.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
            timeout=15,
        )
        r.raise_for_status()
        jobs = []
        for p in r.json().get("jobs", []):
            title = p.get("title", "")
            if not title_matches_any(title):
                continue
            loc = p.get("location", {}).get("name", "") if isinstance(p.get("location"), dict) else ""
            jobs.append(_watchlist_job(
                title, company_name,
                p.get("absolute_url", base_url),
                loc, "", tier,
            ))
        return jobs
    except Exception as e:
        print(f"  ⚠ Watchlist Greenhouse ({company_name}): {e}")
        return []


def _scrape_html_watchlist(url: str, company_name: str, tier: int) -> list[dict]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        jobs = []
        seen_hrefs = set()
        for a in soup.find_all("a", href=True):
            title = a.get_text(strip=True)
            if not title or not title_matches_any(title):
                continue
            href = a["href"]
            if not href.startswith("http"):
                from urllib.parse import urljoin
                href = urljoin(url, href)
            if href in seen_hrefs:
                continue
            seen_hrefs.add(href)
            jobs.append(_watchlist_job(title, company_name, href, "", "", tier))
        return jobs
    except Exception as e:
        print(f"  ⚠ Watchlist HTML ({company_name}): {e}")
        return []


def scrape_watchlist() -> list[dict]:
    all_jobs = []
    for company in WATCHLIST:
        name = company["name"]
        url  = company["url"]
        ats  = company["ats"]
        tier = company["tier"]
        if ats == "lever":
            jobs = _scrape_lever_watchlist(url, name, tier)
        elif ats == "ashby":
            jobs = _scrape_ashby_watchlist(url, name, tier)
        elif ats == "greenhouse":
            jobs = _scrape_greenhouse_watchlist(url, name, tier)
        else:
            jobs = _scrape_html_watchlist(url, name, tier)
        print(f"  · {name}: {len(jobs)} match(es)")
        all_jobs.extend(jobs)
        time.sleep(0.5)
    return all_jobs


# ── Collect + health check ────────────────────────────────────────────────────

SCRAPERS = [
    ("Remotive",       scrape_remotive),
    ("4DayWeek",       scrape_4dayweek),
    ("Himalayas",      scrape_himalayas),
    ("Arbeitnow",      scrape_arbeitnow),
    ("WeWorkRemotely", scrape_weworkremotely),
    ("WorkingNomads",  scrape_workingnomads),
    ("Nodesk",         scrape_nodesk),
    ("Watchlist",      scrape_watchlist),
]

# Days of zero raw results before a health alert fires
ZERO_RESULT_ALERT_DAYS = 7


def collect_all_jobs(health: dict) -> tuple[list[dict], dict, list[str]]:
    all_jobs = []
    alerts   = []
    today_str = TODAY.isoformat()

    for name, fn in SCRAPERS:
        print(f"→ {name}...")
        raw_count = 0
        try:
            results = fn()
            raw_count = len(results)
            print(f"  ✓ {raw_count} matching jobs")
            all_jobs.extend(results)

            h = health.setdefault(name, {
                "last_fetch_date":   None,
                "error_streak":      0,
                "zero_result_streak": 0,
            })
            h["last_fetch_date"] = today_str
            h["error_streak"]    = 0

            # Track zero raw result streaks (post-filter, but still useful signal)
            if raw_count == 0:
                h["zero_result_streak"] = h.get("zero_result_streak", 0) + 1
                if h["zero_result_streak"] >= ZERO_RESULT_ALERT_DAYS:
                    alerts.append(
                        f"{name} — 0 results for {h['zero_result_streak']} consecutive days "
                        f"(may be broken or CSS changed)"
                    )
            else:
                h["zero_result_streak"] = 0

        except Exception as e:
            print(f"  ✗ {name} failed: {e}")
            h = health.setdefault(name, {
                "last_fetch_date":    None,
                "error_streak":       0,
                "zero_result_streak": 0,
            })
            h["error_streak"] = h.get("error_streak", 0) + 1
            if h["error_streak"] >= 3:
                alerts.append(
                    f"{name} — fetch error for {h['error_streak']} consecutive days: {e}"
                )

        time.sleep(1)

    return all_jobs, health, alerts


# ── Deduplication + repost detection ─────────────────────────────────────────

def process_jobs(
    jobs: list[dict], seen: dict
) -> tuple[list[dict], list[dict], dict]:
    new_jobs    = []
    repost_jobs = []
    today_str   = TODAY.isoformat()

    for job in jobs:
        jid = job_id(job["title"], job["company"])

        if jid not in seen:
            seen[jid] = {
                "first_seen": today_str,
                "last_seen":  today_str,
                "count":      1,
            }
            new_jobs.append(job)
        else:
            record = seen[jid]
            last = datetime.date.fromisoformat(record["last_seen"])
            days_since = (TODAY - last).days

            if days_since >= REPOST_DAYS:
                job["repost_days"] = (
                    TODAY - datetime.date.fromisoformat(record["first_seen"])
                ).days
                repost_jobs.append(job)

            record["last_seen"] = today_str
            record["count"] = record.get("count", 1) + 1

    return new_jobs, repost_jobs, seen


# ── Email builder ─────────────────────────────────────────────────────────────

def _job_card_html(j: dict, is_repost: bool = False) -> str:
    badges = ""
    if j.get("four_day"):
        badges += '<span style="display:inline-block;background:#eff6ff;color:#1d4ed8;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;margin-right:5px;">🟢 4-day week</span>'
    if j.get("spain_flag"):
        badges += '<span style="display:inline-block;background:#fff7ed;color:#c2410c;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;margin-right:5px;">⚠️ Verify location</span>'
    if j.get("currency_flag") == "usd":
        badges += '<span style="display:inline-block;background:#fef2f2;color:#991b1b;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;margin-right:5px;">🇺🇸 USD — likely US hire</span>'
    if j.get("currency_flag") == "gbp":
        badges += '<span style="display:inline-block;background:#fefce8;color:#854d0e;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;margin-right:5px;">🇬🇧 GBP — verify eligibility</span>'
    if j.get("watchlist"):
        tier  = j.get("watchlist_tier", 2)
        label = WATCHLIST_TIER_LABELS.get(tier, "📌 Watchlist")
        badges += f'<span style="display:inline-block;background:#f0fdf4;color:#166534;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;margin-right:5px;">{label} Watchlist</span>'
    if is_repost:
        badges += f'<span style="display:inline-block;background:#f5f3ff;color:#6d28d9;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;margin-right:5px;">🔄 Reposted · first seen {j.get("repost_days", "?")}d ago</span>'

    age_label = j.get("age_label", "")
    age_col   = age_color(j.get("age_date"))
    age_html  = f'<span style="font-size:11px;color:{age_col};font-weight:600;">{age_label}</span>' if age_label else ""

    salary_html = ""
    if j.get("salary"):
        salary_html = f'<span style="color:#059669;font-size:12px;">💰 {j["salary"]}</span> &nbsp; '

    return f"""
    <tr>
      <td style="padding:10px 0 16px;border-bottom:1px solid #f3f4f6;">
        {"<div style='margin-bottom:5px;'>" + badges + "</div>" if badges else ""}
        <a href="{j['url']}" style="font-size:15px;font-weight:600;color:#111827;text-decoration:none;line-height:1.3;">
          {j['title']}
        </a><br>
        <span style="font-size:13px;color:#6b7280;">
          {j['company']} &nbsp;·&nbsp; {j['location']}
        </span><br>
        <div style="margin-top:4px;">
          {salary_html}{age_html}
        </div>
        <a href="{j['url']}" style="display:inline-block;margin-top:8px;padding:5px 14px;
           background:#111827;color:#fff;font-size:12px;font-weight:500;
           text-decoration:none;border-radius:6px;">
          View &amp; Apply →
        </a>
      </td>
    </tr>
    """


def build_email(
    new_jobs: list[dict],
    repost_jobs: list[dict],
    alerts: list[str],
    is_silence_breaker: bool = False,
) -> str:
    today_str = TODAY.strftime("%A, %d %B %Y")

    # Split primary vs stretch
    new_primary  = [j for j in new_jobs    if not j.get("is_stretch")]
    new_stretch  = [j for j in new_jobs    if j.get("is_stretch")]
    rep_primary  = [j for j in repost_jobs if not j.get("is_stretch")]
    rep_stretch  = [j for j in repost_jobs if j.get("is_stretch")]

    four_day_count = sum(1 for j in new_jobs + repost_jobs if j.get("four_day"))
    spain_count    = sum(1 for j in new_jobs + repost_jobs if j.get("spain_flag"))

    # Summary pills
    if is_silence_breaker:
        pills = '<span style="background:#f0fdf4;color:#166534;font-size:13px;font-weight:600;padding:4px 12px;border-radius:20px;">✅ Scraper healthy — nothing new today</span>'
    else:
        pills = f"""
        <span style="background:#f0fdf4;color:#166534;font-size:13px;font-weight:600;padding:4px 12px;border-radius:20px;">
          {len(new_primary)} new role{"s" if len(new_primary) != 1 else ""}
        </span>"""
        if new_stretch:
            pills += f"""
        &nbsp;<span style="background:#f5f3ff;color:#7c3aed;font-size:13px;font-weight:600;padding:4px 12px;border-radius:20px;">
          🔭 {len(new_stretch)} stretch
        </span>"""
        if repost_jobs:
            pills += f"""
        &nbsp;<span style="background:#f5f3ff;color:#6d28d9;font-size:13px;font-weight:600;padding:4px 12px;border-radius:20px;">
          🔄 {len(repost_jobs)} reposted
        </span>"""
        if four_day_count:
            pills += f"""
        &nbsp;<span style="background:#eff6ff;color:#1d4ed8;font-size:13px;font-weight:600;padding:4px 12px;border-radius:20px;">
          🟢 {four_day_count} × 4-day week
        </span>"""
        if spain_count:
            pills += f"""
        &nbsp;<span style="background:#fff7ed;color:#c2410c;font-size:13px;font-weight:600;padding:4px 12px;border-radius:20px;">
          ⚠️ {spain_count} × verify location
        </span>"""

    # Alert banner
    alert_html = ""
    if alerts:
        alert_items = "".join(f"<li>{a}</li>" for a in alerts)
        alert_html = f"""
        <tr>
          <td style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;
                     padding:12px 16px;margin:0 32px 16px;">
            <p style="margin:0 0 6px;font-size:13px;font-weight:600;color:#991b1b;">
              🔴 Source health alerts
            </p>
            <ul style="margin:0;padding-left:18px;font-size:12px;color:#7f1d1d;">
              {alert_items}
            </ul>
          </td>
        </tr>"""

    def source_section(jobs, label=None, is_repost=False, is_stretch_section=False):
        if not jobs:
            return ""
        by_source: dict[str, list] = {}
        for j in sorted(jobs, key=lambda x: (not x.get("four_day"), x.get("spain_flag", False))):
            by_source.setdefault(j["source"], []).append(j)

        label_color = "#7c3aed" if is_stretch_section else "#374151"
        html = ""
        if label:
            html += f"""
            <tr><td style="padding:20px 0 4px;">
              <p style="margin:0;font-size:13px;font-weight:700;color:{label_color};
                        text-transform:uppercase;letter-spacing:0.06em;">{label}</p>
            </td></tr>"""

        for source, sjobs in by_source.items():
            html += f"""
            <tr><td style="padding:8px 0 2px;font-size:11px;font-weight:600;
                           text-transform:uppercase;letter-spacing:0.08em;color:#9ca3af;
                           border-top:1px solid #f3f4f6;">{source}</td></tr>"""
            for j in sjobs:
                html += _job_card_html(j, is_repost=is_repost)
        return html

    new_section     = source_section(new_primary)
    stretch_section = source_section(
        new_stretch + rep_stretch,
        label="🔭 Stretch roles — worth checking at smaller companies",
        is_stretch_section=True,
    )
    repost_section  = source_section(rep_primary, label="Reposted roles", is_repost=True)

    silence_note = ""
    if is_silence_breaker:
        silence_note = """
        <tr><td style="padding:16px 0 8px;">
          <p style="margin:0;font-size:13px;color:#6b7280;">
            No new roles today, but the scraper ran without errors.
            You'll hear from it again when something new surfaces.
          </p>
        </td></tr>"""

    source_count = len(SCRAPERS) - 1  # exclude Watchlist from source count
    watchlist_count = len(WATCHLIST)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f9fafb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f9fafb;padding:32px 16px;">
  <tr><td align="center">
  <table width="580" cellpadding="0" cellspacing="0"
         style="background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.08);">

    <!-- Header -->
    <tr><td style="background:#111827;padding:28px 32px;">
      <p style="margin:0;color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:0.1em;">Daily Job Digest</p>
      <h1 style="margin:4px 0 0;color:#fff;font-size:22px;font-weight:700;">Senior &amp; Lead Product Designer</h1>
      <p style="margin:6px 0 0;color:#6b7280;font-size:13px;">{today_str}</p>
    </td></tr>

    <!-- Pills -->
    <tr><td style="padding:20px 32px 8px;">{pills}</td></tr>

    <!-- Legend -->
    <tr><td style="padding:4px 32px 8px;">
      <p style="margin:0;font-size:11px;color:#9ca3af;line-height:1.6;">
        🟢 4-day week &nbsp;|&nbsp; ⚠️ Verify location/hybrid &nbsp;|&nbsp;
        🔄 Repost — role still open &nbsp;|&nbsp;
        🔭 Stretch — Staff/Principal at smaller companies &nbsp;|&nbsp;
        🇺🇸 USD — likely US hire &nbsp;|&nbsp; 🇬🇧 GBP — verify eligibility &nbsp;|&nbsp;
        <span style="color:#059669;">●</span> Fresh &nbsp;
        <span style="color:#d97706;">●</span> Getting older &nbsp;
        <span style="color:#9ca3af;">●</span> Stale
      </p>
    </td></tr>

    <!-- Alert banner -->
    {alert_html}

    <!-- Jobs -->
    <tr><td style="padding:8px 32px 28px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        {silence_note}
        {new_section}
        {repost_section}
        {stretch_section}
      </table>
    </td></tr>

    <!-- Footer -->
    <tr><td style="background:#f9fafb;padding:16px 32px;border-top:1px solid #f3f4f6;">
      <p style="margin:0;font-size:11px;color:#9ca3af;">
        {source_count} sources + {watchlist_count} watchlist companies &nbsp;|&nbsp; Remote · Spain · Europe &nbsp;|&nbsp;
        Senior &amp; Lead Product Designer only
      </p>
    </td></tr>

  </table>
  </td></tr>
</table>
</body>
</html>"""


def build_silence_breaker_email() -> str:
    return build_email([], [], [], is_silence_breaker=True)


# ── Send ──────────────────────────────────────────────────────────────────────

def send_email(html: str, subject: str):
    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type":  "application/json",
        },
        json={
            "from":    EMAIL_FROM,
            "to":      [EMAIL_TO],
            "subject": subject,
            "html":    html,
        },
        timeout=15,
    )
    if r.status_code == 200:
        print(f"✅ Email sent: {subject}")
    else:
        print(f"✗ Email failed: {r.status_code} {r.text}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*52}")
    print(f"Job Scraper – {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*52}\n")

    seen   = load_seen()
    health = load_health()

    # Auto-prune stale seen_jobs entries
    seen, pruned_count = prune_seen(seen)
    if pruned_count:
        print(f"🧹 Pruned {pruned_count} stale entries from seen_jobs (>{PRUNE_DAYS} days old)\n")
    print(f"Previously seen jobs: {len(seen)}\n")

    all_jobs, health, alerts = collect_all_jobs(health)
    save_health(health)

    if alerts:
        print("\n⚠ Health alerts:")
        for a in alerts:
            print(f"  · {a}")

    print(f"\nTotal matching jobs: {len(all_jobs)}")
    new_jobs, repost_jobs, seen = process_jobs(all_jobs, seen)
    save_seen(seen)

    new_primary = [j for j in new_jobs    if not j.get("is_stretch")]
    new_stretch = [j for j in new_jobs    if j.get("is_stretch")]
    print(f"New: {len(new_primary)} primary · {len(new_stretch)} stretch · Reposted: {len(repost_jobs)}")

    today_str = TODAY.strftime("%d %b %Y")

    if new_jobs or repost_jobs or alerts:
        parts = []
        if new_primary:
            parts.append(f"{len(new_primary)} new")
        if new_stretch:
            parts.append(f"{len(new_stretch)} stretch")
        if repost_jobs:
            parts.append(f"{len(repost_jobs)} reposted")
        subject = f"🎨 {' · '.join(parts)} · {today_str}"
        html = build_email(new_jobs, repost_jobs, alerts)
        send_email(html, subject)
        health["last_email_date"] = TODAY.isoformat()
        save_health(health)
        return

    # Nothing to report — check if silence-breaker is needed
    last_email_str = health.get("last_email_date")
    if last_email_str:
        last_email = datetime.date.fromisoformat(last_email_str)
        days_silent = (TODAY - last_email).days
    else:
        days_silent = SILENCE_DAYS  # treat as overdue if never recorded

    if days_silent >= SILENCE_DAYS:
        print(f"📭 {days_silent} days since last email — sending silence-breaker.")
        html    = build_silence_breaker_email()
        subject = f"✅ Scraper healthy, nothing new · {today_str}"
        send_email(html, subject)
        health["last_email_date"] = TODAY.isoformat()
        save_health(health)
    else:
        print(f"Nothing to report — no email sent ({days_silent} day(s) since last send).")


if __name__ == "__main__":
    main()
