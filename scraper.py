#!/usr/bin/env python3
"""
Job Scraper – Senior / Lead Product Designer
Runs daily via GitHub Actions and sends a digest email via Resend.

Sources (4 APIs + 6 HTML scrapers):
  APIs:    Remotive, 4DayWeek, Himalayas, Arbeitnow
  Scrapers: WeWorkRemotely, EURemoteJobs, WorkingNomads, Nodesk,
            DailyRemote, RemotifyEurope, RemoteRocketship

Features:
  - Age displayed on every listing
  - Repost detection (resurfaces after 14+ days with flag)
  - Spain/hybrid flag
  - 4-day week badge
  - Source health check (flags sources with 0 results for 3+ days)
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

RESEND_API_KEY      = os.environ.get("RESEND_API_KEY", "")
EMAIL_TO            = os.environ.get("EMAIL_TO", "")
EMAIL_FROM          = os.environ.get("EMAIL_FROM", "jobs@yourdomain.com")
SEEN_JOBS_FILE      = Path("seen_jobs.json")       # {job_id: {first_seen, last_seen, count}}
HEALTH_FILE         = Path("source_health.json")   # {source: {last_result_date, zero_streak}}
REPOST_DAYS         = 14   # resurface a seen job if reposted after this many days

TITLE_KEYWORDS = [
    "lead product designer",
    "senior product designer",
    "lead designer",
    "principal product designer",
    "head of product design",
    "staff product designer",
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
    "us only", "usa only", "united states only", "canada only",
    "uk only", "must be located in the us",
    # US remote patterns
    "remote · usa", "remote - usa", "remote, usa",
    "remote · united states", "remote - united states", "remote, united states",
    "united states", " usa",
]

# Signals in job description text that indicate US-only hiring
US_DESCRIPTION_SIGNALS = [
    "401(k)", "401k",
    "must be authorized to work in the us",
    "must be authorized to work in the united states",
    "us work authorization",
    "authorized to work in the us",
    "eligible to work in the us",
]

# Currencies that suggest non-EU hiring (flag, not hard exclude)
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
    """
    Returns {job_id: {"first_seen": "YYYY-MM-DD", "last_seen": "YYYY-MM-DD", "count": int}}
    """
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

# ── Matching helpers ──────────────────────────────────────────────────────────

def job_id(title: str, company: str) -> str:
    raw = f"{title.lower().strip()}-{company.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()

def title_matches(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in TITLE_KEYWORDS)

def location_ok(location: str) -> bool:
    loc = location.lower().strip()
    if not loc:
        return True
    if any(ex in loc for ex in EXCLUDE_LOCATION):
        return False
    # Catch bare "USA" / "United States" as the whole location string
    if loc in ("usa", "united states", "us", "remote usa", "remote us"):
        return False
    return any(kw in loc for kw in LOCATION_KEYWORDS)

def is_us_description(description: str) -> bool:
    """Check job description text for US-only hiring signals."""
    if not description:
        return False
    d = description.lower()
    return any(sig in d for sig in US_DESCRIPTION_SIGNALS)

def currency_flag(salary: str) -> str:
    """
    Returns 'usd', 'gbp', or '' based on salary string.
    Used to badge roles that are likely non-EU hires.
    """
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

# ── Age helpers ───────────────────────────────────────────────────────────────

def parse_age(posted_at) -> tuple[str, datetime.date | None]:
    """
    Accepts ISO string, Unix timestamp (int/float), or relative string.
    Returns (display_string, date_object_or_None).
    """
    if not posted_at:
        return "Date unknown", None

    date = None

    # Unix timestamp
    if isinstance(posted_at, (int, float)):
        try:
            date = datetime.datetime.utcfromtimestamp(posted_at).date()
        except Exception:
            pass

    # ISO string or human date string
    if date is None and isinstance(posted_at, str):
        # Relative strings ("3 days ago", "posted last week")
        lower = posted_at.lower().strip()
        today = TODAY
        relative_map = [
            (r"today|just now|less than a day", 0),
            (r"yesterday",                      1),
            (r"(\d+)\s*day",                    None),   # handled below
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
        label = date.strftime("%-d %b")   # "3 Apr"

    return label, date

def age_color(date: datetime.date | None) -> str:
    """Returns a hex colour for the age badge: green → amber → grey."""
    if date is None:
        return "#9ca3af"
    delta = (TODAY - date).days
    if delta <= 3:
        return "#059669"   # green — fresh
    if delta <= 10:
        return "#d97706"   # amber — getting older
    return "#9ca3af"       # grey — stale

# ── Fetch helper ──────────────────────────────────────────────────────────────

def fetch(url: str, timeout: int = 15) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ⚠ Could not fetch {url}: {e}")
        return None

# ── Scrapers ──────────────────────────────────────────────────────────────────

def scrape_remotive() -> list[dict]:
    """Remotive public API."""
    jobs = []
    try:
        r = requests.get(
            "https://remotive.com/api/remote-jobs",
            params={"category": "Design", "limit": 100},
            timeout=15,
        )
        for j in r.json().get("jobs", []):
            title = j.get("title", "")
            if not title_matches(title):
                continue
            location = j.get("candidate_required_location", "")
            if not location_ok(location):
                continue
            description = j.get("description", "") or ""
            if is_us_description(description):
                continue
            salary = j.get("salary", "") or ""
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
            })
    except Exception as e:
        print(f"  ⚠ Remotive error: {e}")
    return jobs


def scrape_4dayweek() -> list[dict]:
    """4DayWeek public API v2 — design / senior+lead / remote / EU-friendly."""
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
                if not title_matches(title):
                    continue

                # Check remote_allowed countries — skip if US-only
                remote_allowed = j.get("remote_allowed", [])
                if remote_allowed:
                    countries = [loc.get("country", "").lower() for loc in remote_allowed]
                    # If the only allowed country is US/Canada, skip
                    non_us = [c for c in countries if c not in (
                        "united states", "usa", "us", "canada"
                    )]
                    if countries and not non_us:
                        continue
                    country_display = [loc.get("country", "") for loc in remote_allowed]
                    location = "Remote – " + ", ".join(c for c in country_display if c) if country_display else "Remote"
                else:
                    location = "Remote"

                # Check description for US-only signals
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
    """Himalayas search API — replaces the old HTML scraper."""
    jobs = []
    offset = 0
    while True:
        try:
            r = requests.get(
                "https://himalayas.app/jobs/api/search",
                params={
                    "q":          "product designer",
                    "seniority":  "senior,lead",
                    "limit":      20,
                    "offset":     offset,
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
                if not title_matches(title):
                    continue
                restrictions = j.get("locationRestrictions", []) or []
                location = ", ".join(restrictions) if restrictions else "Remote"
                if not location_ok(location):
                    continue
                age_label, age_date = parse_age(j.get("pubDate"))
                jobs.append({
                    "title":      title,
                    "company":    j.get("companyName", ""),
                    "location":   location,
                    "salary":     _himalayas_salary(j),
                    "url":        j.get("applicationLink", ""),
                    "source":     "Himalayas",
                    "four_day":   False,
                    "spain_flag": is_spain_only(location),
                    "age_label":  age_label,
                    "age_date":   age_date,
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
    """Arbeitnow free API — pulls directly from ATS systems, EU-focused."""
    jobs = []
    page = 1
    while True:
        try:
            r = requests.get(
                "https://www.arbeitnow.com/api/job-board-api",
                params={"page": page},
                timeout=15,
            )
            data = r.json()
            items = data.get("data", [])
            if not items:
                break
            for j in items:
                title = j.get("title", "")
                if not title_matches(title):
                    continue
                if not j.get("remote", False):
                    continue
                location = j.get("location", "") or "Remote"
                if not location_ok(location):
                    continue
                age_label, age_date = parse_age(j.get("created_at") or j.get("date"))
                jobs.append({
                    "title":      title,
                    "company":    j.get("company_name", ""),
                    "location":   location,
                    "salary":     "",
                    "url":        j.get("url", ""),
                    "source":     "Arbeitnow",
                    "four_day":   False,
                    "spain_flag": is_spain_only(location),
                    "age_label":  age_label,
                    "age_date":   age_date,
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
    """WeWorkRemotely Design RSS feed."""
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
        if not title_matches(title):
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
    """Generic HTML scraper to reduce repetition across similar boards."""
    jobs = []
    soup = fetch(url)
    if not soup:
        return jobs
    for card in soup.select(card_sel):
        title_el   = card.select_one(title_sel)
        company_el = card.select_one(company_sel) if company_sel else None
        location_el= card.select_one(location_sel) if location_sel else None
        link_el    = card.select_one(link_sel) if link_sel else None
        date_el    = card.select_one(date_sel) if date_sel else None
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title_matches(title):
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
        })
    return jobs


def scrape_euremotejobs() -> list[dict]:
    return _html_scraper(
        url="https://euremotejobs.com/jobs/?search=product+designer&post_type=job_listing",
        source="EURemoteJobs",
        card_sel=".job_listing, article, [class*='job-listing']",
        title_sel="h2, h3, .job-title, [class*='title']",
        company_sel=".company, [class*='company']",
        location_sel="[class*='location']",
        link_sel="a[href]",
        default_location="Europe / Remote",
        date_sel="[class*='date'], time",
    )

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

def scrape_dailyremote() -> list[dict]:
    return _html_scraper(
        url="https://dailyremote.com/remote-design-jobs?search=product+design&location=europe",
        source="DailyRemote",
        card_sel=".card, article, [class*='job']",
        title_sel="h2, h3, [class*='title']",
        company_sel="[class*='company']",
        location_sel="[class*='location']",
        link_sel="a[href]",
        base_url="https://dailyremote.com",
        default_location="Europe / Remote",
        date_sel="time, [class*='date']",
    )

def scrape_remotify() -> list[dict]:
    return _html_scraper(
        url="https://remotifyeurope.com/jobsbycategory/design",
        source="RemotifyEurope",
        card_sel="article, .job, [class*='job-card'], li[class*='job']",
        title_sel="h2, h3, [class*='title']",
        company_sel="[class*='company']",
        location_sel="[class*='location']",
        link_sel="a[href]",
        base_url="https://remotifyeurope.com",
        default_location="Europe / Remote",
        date_sel="time, [class*='date']",
    )

def scrape_remoterocketship() -> list[dict]:
    return _html_scraper(
        url="https://www.remoterocketship.com/country/europe?locations=Europe&sort=DateAdded&keywords=product+designer",
        source="RemoteRocketship",
        card_sel="article, [class*='job-card'], [class*='JobCard'], li[class*='job']",
        title_sel="h2, h3, [class*='title']",
        company_sel="[class*='company']",
        location_sel="[class*='location']",
        link_sel="a[href]",
        base_url="https://www.remoterocketship.com",
        default_location="Europe / Remote",
        date_sel="time, [class*='date']",
    )


# ── Collect + health check ────────────────────────────────────────────────────

SCRAPERS = [
    ("Remotive",        scrape_remotive),
    ("4DayWeek",        scrape_4dayweek),
    ("Himalayas",       scrape_himalayas),
    ("Arbeitnow",       scrape_arbeitnow),
    ("WeWorkRemotely",  scrape_weworkremotely),
    ("EURemoteJobs",    scrape_euremotejobs),
    ("WorkingNomads",   scrape_workingnomads),
    ("Nodesk",          scrape_nodesk),
    ("DailyRemote",     scrape_dailyremote),
    ("RemotifyEurope",  scrape_remotify),
    ("RemoteRocketship",scrape_remoterocketship),
]

def collect_all_jobs(health: dict) -> tuple[list[dict], dict, list[str]]:
    all_jobs = []
    alerts = []
    today_str = TODAY.isoformat()

    for name, fn in SCRAPERS:
        print(f"→ {name}...")
        try:
            results = fn()
            count = len(results)
            print(f"  ✓ {count} matching jobs")
            all_jobs.extend(results)

            # Health check: track whether the scraper fetched successfully,
            # not whether jobs survived our filters. A scraper returning 0
            # after title/location filtering is fine — that's the filters
            # working. Only flag if the scraper itself is broken (exception
            # path below) or explicitly signals an empty page (count stays
            # at 0 AND the scraper didn't raise, meaning the page returned
            # no parseable content at all — different from "no matches").
            h = health.setdefault(name, {"last_fetch_date": None, "error_streak": 0})
            h["last_fetch_date"] = today_str
            h["error_streak"] = 0  # successful run resets streak

        except Exception as e:
            print(f"  ✗ {name} failed: {e}")
            h = health.setdefault(name, {"last_fetch_date": None, "error_streak": 0})
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
    """
    Returns:
      new_jobs    — never seen before
      repost_jobs — seen before but reposted after REPOST_DAYS
      updated_seen
    """
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
                # Resurface as repost
                job["repost_days"] = (
                    TODAY - datetime.date.fromisoformat(record["first_seen"])
                ).days
                repost_jobs.append(job)

            # Always update last_seen and count
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
) -> str:
    today_str = TODAY.strftime("%A, %d %B %Y")
    total = len(new_jobs) + len(repost_jobs)

    four_day_count = sum(1 for j in new_jobs + repost_jobs if j.get("four_day"))
    spain_count    = sum(1 for j in new_jobs + repost_jobs if j.get("spain_flag"))

    # Summary pills
    pills = f"""
    <span style="background:#f0fdf4;color:#166534;font-size:13px;font-weight:600;padding:4px 12px;border-radius:20px;">
      {len(new_jobs)} new role{"s" if len(new_jobs) != 1 else ""}
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

    # Job sections
    def source_section(jobs, label=None, is_repost=False):
        if not jobs:
            return ""
        # Group by source
        by_source: dict[str, list] = {}
        for j in sorted(jobs, key=lambda x: (not x.get("four_day"), x.get("spain_flag", False))):
            by_source.setdefault(j["source"], []).append(j)

        html = ""
        if label:
            html += f"""
            <tr><td style="padding:20px 0 4px;">
              <p style="margin:0;font-size:13px;font-weight:700;color:#374151;
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

    new_section    = source_section(new_jobs)
    repost_section = source_section(repost_jobs, label="Reposted roles", is_repost=True)

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
        {new_section}
        {repost_section}
      </table>
    </td></tr>

    <!-- Footer -->
    <tr><td style="background:#f9fafb;padding:16px 32px;border-top:1px solid #f3f4f6;">
      <p style="margin:0;font-size:11px;color:#9ca3af;">
        11 sources &nbsp;|&nbsp; Remote · Spain · Europe &nbsp;|&nbsp;
        Senior &amp; Lead Product Designer only
      </p>
    </td></tr>

  </table>
  </td></tr>
</table>
</body>
</html>"""


# ── Send ──────────────────────────────────────────────────────────────────────

def send_email(html: str, new_count: int, repost_count: int):
    today_str = TODAY.strftime("%d %b %Y")
    parts = [f"{new_count} new"]
    if repost_count:
        parts.append(f"{repost_count} reposted")
    subject = f"🎨 {' · '.join(parts)} · {today_str}"

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
        print(f"✅ Email sent — {new_count} new, {repost_count} reposted")
    else:
        print(f"✗ Email failed: {r.status_code} {r.text}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*52}")
    print(f"Job Scraper – {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*52}\n")

    seen   = load_seen()
    health = load_health()
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

    print(f"New: {len(new_jobs)} · Reposted: {len(repost_jobs)}")

    if not new_jobs and not repost_jobs and not alerts:
        print("Nothing to report — no email sent.")
        return

    html = build_email(new_jobs, repost_jobs, alerts)
    send_email(html, len(new_jobs), len(repost_jobs))


if __name__ == "__main__":
    main()
