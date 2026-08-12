#!/usr/bin/env python3
"""
Job Scraper – Senior / Lead Product Designer
Runs daily via GitHub Actions and sends a digest email via Resend.

Sources (4 APIs + 9 HTML scrapers + watchlist):
  APIs:    Remotive, 4DayWeek, Himalayas, Arbeitnow
  Scrapers: WeWorkRemotely, WorkingNomads, Nodesk,
            TrulyRemote, UXJobs, DynamiteJobs,
            RemoteRebellion, UIUXDesignerJobs, RemoteInEurope
  Watchlist: 23 pre-vetted companies via Lever / Ashby / Greenhouse / HTML

Email footer includes manual check links:
  LinkedIn, Wellfound, Welcome to the Jungle, Glassdoor,
  Flexa, WeLoveProduct, DesignJobs.World
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
MAX_JOB_AGE_DAYS = 21   # hard-drop jobs older than this — safety net for date parsing failures
ERROR_ALERT_DAYS = 3    # consecutive fetch errors before health alert fires
ZERO_RESULT_ALERT_DAYS = 5  # consecutive 0-result days (fetch OK, no matches) before health alert fires

# Primary roles — shown in main section
TITLE_KEYWORDS = [
    "lead product designer",
    "senior product designer",
    "lead designer",
]

# Stretch roles — shown in a separate section, lower priority
STRETCH_TITLE_KEYWORDS = [
    "principal product designer",
    "staff product designer",
    "principal designer",
    "staff designer",
    "head of product design",
]

# Leadership/C-level — excluded entirely, never shown
EXCLUDE_TITLE_KEYWORDS = [
    "vp ", "vp,", "vice president",
    "director of design", "design director",
    "chief design officer", "cdo",
    "head of design",
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
    "remote · canada", "remote - canada", "remote, canada",
    # North America
    "north america only", "na only",
    "remote · north america", "remote - north america", "remote, north america",
    "north america",
    # UK
    "uk only", "united kingdom only",
    "remote · united kingdom", "remote - united kingdom", "remote, united kingdom",
    "remote · uk", "remote - uk", "remote, uk",
    "united kingdom",
]

US_DESCRIPTION_SIGNALS = [
    # Benefits
    "401(k)", "401k",
    "health, dental, and vision",
    "medical, dental, and vision",
    "medical, dental & vision",
    "health, dental & vision",
    "employee stock purchase plan", "espp",
    # Hiring eligibility
    "must be authorized to work in the us",
    "must be authorized to work in the united states",
    "us work authorization",
    "authorized to work in the us",
    "eligible to work in the us",
    "must be based in the us",
    "must reside in the us",
    "must be located in the us",
    "candidates must be in the united states",
    # Compensation signals
    "base salary range: $", "base pay: $",
    "salary range: $",
    "ote: $",
]

USD_SIGNALS = ["usd", "$ ", "us$"]
GBP_SIGNALS = ["gbp", "£"]

# Countries that hard-exclude a job even when other structured signals (e.g. a
# "Remote" API field) would otherwise let it through. Used where a source gives
# us real country data (not just free text) — e.g. 4DayWeek's remote_allowed.
NON_EU_HARD_EXCLUDE_COUNTRIES = {
    "united states", "usa", "us",
    "canada",
    "united kingdom", "uk",
    "australia", "india", "brazil", "argentina", "mexico",
    "colombia", "peru", "philippines", "singapore", "south korea",
    "china", "hong kong", "indonesia", "vietnam", "south africa",
    "nigeria", "bangladesh", "belize", "el salvador", "costa rica",
    "new zealand", "japan", "thailand",
}


def location_country_ok(location: str) -> bool:
    """Hard-exclude check for sources that give a bare 'Remote <Country>'
    string (e.g. "Remote US", "Remote Thailand"). location_ok() alone would
    wave these through just because they contain the word "remote" — this
    strips that prefix and checks the actual place name underneath."""
    loc = location.lower().strip()
    bare = re.sub(r'^(remote|worldwide)\s*[-–,]?\s*', '', loc).strip()
    if not bare:
        return True
    aliases = {"namer": "united states", "na": "united states", "usa": "united states"}
    bare = aliases.get(bare, bare)
    return bare not in NON_EU_HARD_EXCLUDE_COUNTRIES

# Companies known to hire US-only despite listing "Remote" or "Anywhere in the World".
# Add to this list as more slip through — lowercase, matched as substring of company name.
US_COMPANY_BLOCKLIST = [
    "logicgate",
    "twilio",
    "gusto",
    "rippling",
    "brex",
    "deel",
    "lattice",
    "retool",
    "loom",
    "figma",
    "mercury",
    "ramp",
    "zip recruiter", "ziprecruiter",
    "samsara",
    "ocrolus",
    "thumbtack",
    "owner.com",
    "deepgram",
    "pinterest",
    "clickup",
    "happyco",
    "vercel",
]

def is_blocked_company(company: str) -> bool:
    c = company.lower()
    return any(blocked in c for blocked in US_COMPANY_BLOCKLIST)


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

def title_is_excluded(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in EXCLUDE_TITLE_KEYWORDS)

def title_matches(title: str) -> bool:
    if title_is_excluded(title):
        return False
    t = title.lower()
    return any(kw in t for kw in TITLE_KEYWORDS)

def title_is_stretch(title: str) -> bool:
    if title_is_excluded(title):
        return False
    t = title.lower()
    return (not title_matches(title)) and any(kw in t for kw in STRETCH_TITLE_KEYWORDS)

def title_matches_any(title: str) -> bool:
    return title_matches(title) or title_is_stretch(title)

def location_ok(location: str) -> bool:
    loc = location.lower().strip()
    if not loc:
        return True
    if any(ex in loc for ex in EXCLUDE_LOCATION):
        return False
    if loc in ("usa", "united states", "us", "remote usa", "remote us",
               "uk", "united kingdom", "remote uk",
               "north america", "canada"):
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
                parsed = dateparser.parse(posted_at, dayfirst=False)
                candidate = parsed.date()
                # If parsed date is in the future, the format is likely day/month
                # (EU-style) misread as month/day. Retry with dayfirst=True.
                if candidate > today:
                    parsed_alt = dateparser.parse(posted_at, dayfirst=True)
                    candidate_alt = parsed_alt.date()
                    if candidate_alt <= today:
                        candidate = candidate_alt
                date = candidate
            except Exception:
                pass

    if date is None:
        return "Date unknown", None

    delta = (TODAY - date).days

    # Never show a negative age — if date is still in the future after retry,
    # the format wasn't recognised. Show as unknown rather than "-N days ago".
    if delta < 0:
        return "Date unknown", None

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
            company_name = j.get("company_name", "")
            if is_blocked_company(company_name):
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
                "company":       company_name,
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
                company_name = j.get("company", {}).get("name", "") if isinstance(j.get("company"), dict) else ""
                if not title_matches_any(title):
                    continue


                if is_blocked_company(company_name):
                    continue

                remote_allowed = j.get("remote_allowed", [])
                if remote_allowed:
                    countries = [loc.get("country", "").lower().strip() for loc in remote_allowed]
                    countries = [c for c in countries if c]
                    # Exclude only if EVERY listed country is a hard-exclude one.
                    # NOTE: this decision is made directly from the structured
                    # country list, not routed through location_ok() — the old
                    # "Remote – " display prefix always contained the word
                    # "remote", which trivially passed location_ok() no matter
                    # which countries were actually listed (e.g. "Remote –
                    # Australia, India" slipped through unfiltered).
                    if countries and all(c in NON_EU_HARD_EXCLUDE_COUNTRIES for c in countries):
                        continue
                    country_display = [loc.get("country", "") for loc in remote_allowed]
                    location = ", ".join(c for c in country_display if c) or "Worldwide"
                else:
                    # No country data — use salary currency as a proxy.
                    # USD + no location = almost certainly a US-only role on 4DayWeek.
                    # If currency is also missing, check description then let it through.
                    cur_raw = (j.get("salary_currency", "") or "").upper().strip()
                    if cur_raw == "USD":
                        continue
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
    """Himalayas' seniority filter silently returns an empty response for a
    comma-joined value like "senior,lead" — confirmed by direct testing.
    It only accepts one value per request, so we run one paginated pass per
    seniority level instead. Global job_id dedup handles any overlap."""
    jobs = []
    for seniority in ("Senior", "Lead"):
        offset = 0
        while True:
            try:
                r = requests.get(
                    "https://himalayas.app/jobs/api/search",
                    params={
                        "q":         "product designer",
                        "seniority": seniority,
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
                    company_name = j.get("companyName", "")
                    if is_blocked_company(company_name):
                        continue
                    restrictions = j.get("locationRestrictions", []) or []
                    location = ", ".join(restrictions) if restrictions else "Remote"
                    if not location_ok(location):
                        continue
                    salary = sanitise_salary(_himalayas_salary(j))
                    age_label, age_date = parse_age(j.get("pubDate"))
                    jobs.append({
                        "title":         title,
                        "company":       company_name,
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
                print(f"  ⚠ Himalayas error (seniority={seniority}, offset {offset}): {e}")
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
                company_name = j.get("company_name", "")
                if is_blocked_company(company_name):
                    continue
                if not j.get("remote", False):
                    continue
                location = j.get("location", "") or "Remote"
                if not location_ok(location):
                    continue
                age_label, age_date = parse_age(j.get("created_at") or j.get("date"))
                jobs.append({
                    "title":         title,
                    "company":       company_name,
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
        if is_blocked_company(company):
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
        company = company_el.get_text(strip=True) if company_el else ""
        if is_blocked_company(company):
            continue
        location = location_el.get_text(strip=True) if location_el else default_location
        if not location_ok(location):
            continue
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

def scrape_trulyremote() -> list[dict]:
    return _html_scraper(
        url="https://trulyremote.co/?search=senior+product+designer",
        source="TrulyRemote",
        card_sel="[class*='job'], article, [class*='listing']",
        title_sel="h2, h3, [class*='title']",
        company_sel="[class*='company'], [class*='employer']",
        location_sel="[class*='location']",
        link_sel="a[href]",
        default_location="Remote",
        date_sel="time, [class*='date']",
    )

def scrape_uxjobs() -> list[dict]:
    """jobs.uxjobs.io — remote product designer jobs, aggregated daily.
    Each listing is an <article class="card"> with clean sub-elements:
      .card-loc   -> flag emoji + real location text, e.g. "🌍 Anywhere in the World"
      .card-title -> "Title - Company"
      .card-time  -> age, e.g. "18h ago" / "3 days ago"
      .card-link[href] -> the outbound job link

    IMPORTANT: UXJobs uses a generic 🌍 globe icon for ANY multi-region or
    ambiguous-location posting — it does not mean "EU-safe". Country flags
    (e.g. 🇺🇸, 🇩🇪) are reliable, but 🌍 covers everything from "Anywhere in
    the World" to "Multiple locations" to roles that are explicitly US-only
    in the title. So the flag is only used as a fast hard-exclude pre-check;
    the real decision always runs the actual location TEXT through the same
    location_ok() hard-exclude logic every other source uses.
    """
    # Flags that are immediate hard excludes — reliable when present
    EXCLUDE_FLAGS = {"🇺🇸", "🇨🇦", "🇦🇺", "🇮🇳", "🇧🇷", "🇦🇷",
                     "🇲🇽", "🇨🇴", "🇵🇪", "🇵🇭", "🇸🇬", "🇰🇷",
                     "🇨🇳", "🇭🇰", "🇮🇩", "🇻🇳", "🇿🇦", "🇳🇬",
                     "🇧🇩", "🇧🇿", "🇸🇻", "🇨🇷"}

    jobs = []
    soup = fetch("https://jobs.uxjobs.io/remote-product-designer-jobs/")
    if not soup:
        return jobs

    seen_urls = set()
    for card in soup.select("article.card"):
        link_el = card.select_one("a.card-link[href]") or card.select_one("a[href]")
        loc_el = card.select_one(".card-loc")
        title_el = card.select_one(".card-title")
        if not link_el or not loc_el or not title_el:
            continue

        href = link_el["href"]
        if href in seen_urls:
            continue
        seen_urls.add(href)

        # Split flag from the real location text
        loc_raw = loc_el.get_text(separator=" ", strip=True)
        chars = list(loc_raw)
        if (len(chars) >= 2
                and 0x1F1E0 <= ord(chars[0]) <= 0x1F1FF
                and 0x1F1E0 <= ord(chars[1]) <= 0x1F1FF):
            flag = chars[0] + chars[1]
        elif chars and ord(chars[0]) > 127:
            flag = chars[0]
        else:
            flag = ""
        location = loc_raw[len(flag):].strip() if flag else loc_raw

        if flag in EXCLUDE_FLAGS:
            continue
        if not location_ok(location):
            continue

        # Title/company from the clean "Title - Company" text
        title_company = title_el.get_text(separator=" ", strip=True)
        if " - " in title_company:
            title, company = title_company.rsplit(" - ", 1)
            title, company = title.strip(), company.strip()
        else:
            title, company = title_company, ""

        if not title_matches_any(title):
            continue
        if is_blocked_company(company):
            continue

        time_el = card.select_one(".card-time")
        raw_time = time_el.get_text(strip=True) if time_el else ""
        age_label, age_date = parse_age(raw_time)
        if age_date is None:
            hour_match = re.search(r'(\d+)\s*h\s*ago', raw_time, re.I)
            if hour_match:
                delta = datetime.timedelta(hours=int(hour_match.group(1)))
                age_date = TODAY - delta
                age_label, age_date = parse_age(str(age_date))

        jobs.append({
            "title":         title,
            "company":       company,
            "location":      location,
            "salary":        "",
            "url":           href if href.startswith("http") else f"https://jobs.uxjobs.io{href}",
            "source":        "UXJobs",
            "four_day":      False,
            "spain_flag":    is_spain_only(location),
            "currency_flag": "",
            "age_label":     age_label,
            "age_date":      age_date,
            "is_stretch":    title_is_stretch(title),
        })

    return jobs

def scrape_dynamitejobs() -> list[dict]:
    return _html_scraper(
        url="https://dynamitejobs.com/remote-jobs/design/ux-web-design",
        source="DynamiteJobs",
        card_sel="[class*='job'], article, [class*='listing']",
        title_sel="h2, h3, [class*='title']",
        company_sel="[class*='company'], [class*='employer']",
        location_sel="[class*='location']",
        link_sel="a[href]",
        base_url="https://dynamitejobs.com",
        default_location="Remote",
        date_sel="time, [class*='date']",
    )

def scrape_remoterebellion() -> list[dict]:
    """remoterebellion.com/remote-design-jobs is a Squarespace rich-text
    block — postings are plain <a>Title (Location)</a> links with no
    separate company/location markup. Company is inferred from the ATS
    URL slug (Ashby/Lever/Greenhouse/etc.) since it isn't given as text.
    """
    jobs = []
    soup = fetch("https://remoterebellion.com/remote-design-jobs")
    if not soup:
        return jobs

    seen_urls = set()
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        m = re.match(r'^(.*)\(([^)]+)\)\s*$', text)
        if not m:
            continue
        title, location = m.group(1).strip(), m.group(2).strip()
        if not title_matches_any(title):
            continue

        href = a["href"]
        if href in seen_urls:
            continue
        seen_urls.add(href)

        slug_match = re.search(
            r'(?:ashbyhq\.com|jobs\.lever\.co|greenhouse\.io|smartrecruiters\.com|teamtailor\.com|workable\.com)'
            r'/([a-zA-Z0-9\-\.]+)',
            href,
        )
        company = slug_match.group(1).replace("-", " ").replace(".", " ").title() if slug_match else ""

        if is_blocked_company(company):
            continue
        if not location_ok(location) or not location_country_ok(location):
            continue

        jobs.append({
            "title":         title,
            "company":       company,
            "location":      location,
            "salary":        "",
            "url":           href,
            "source":        "RemoteRebellion",
            "four_day":      False,
            "spain_flag":    is_spain_only(location),
            "currency_flag": "",
            "age_label":     "Date unknown",
            "age_date":      None,
            "is_stretch":    title_is_stretch(title),
        })

    return jobs


def scrape_remoteineurope() -> list[dict]:
    """remoteineurope.com — each posting is an <a class="card job"> itself
    (not a wrapper around a link), with clean sub-elements for title,
    company, and location. The site is EU-scoped by design (its whole
    premise is "remote jobs in Europe"), so the location tag is often just
    the continent "Europe" rather than a specific country.
    """
    jobs = []
    seen_urls = set()
    for category in ("design", "product"):
        soup = fetch(f"https://remoteineurope.com/categories/{category}")
        if not soup:
            continue

        for card in soup.select("a.card.job"):
            href = card.get("href", "")
            if href in seen_urls:
                continue

            title_el = card.select_one("h3.title.card-job, .title.card-job")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title_matches_any(title):
                continue

            company_el = card.select_one(".job-content .card-link.homepage")
            company = company_el.get_text(strip=True) if company_el else ""
            if is_blocked_company(company):
                continue

            loc_el = card.select_one(".card-short-location-wrapper .short-location")
            location = loc_el.get_text(strip=True) if loc_el else "Europe"
            if not location_ok(location) or not location_country_ok(location):
                continue

            seen_urls.add(href)
            url_full = href if href.startswith("http") else f"https://remoteineurope.com{href}"
            date_el = card.select_one(".date-text")
            raw_date = date_el.get_text(strip=True) if date_el else None
            age_label, age_date = parse_age(raw_date)

            jobs.append({
                "title":         title,
                "company":       company,
                "location":      location,
                "salary":        "",
                "url":           url_full,
                "source":        "RemoteInEurope",
                "four_day":      False,
                "spain_flag":    is_spain_only(location),
                "currency_flag": "",
                "age_label":     age_label,
                "age_date":      age_date,
                "is_stretch":    title_is_stretch(title),
            })
        time.sleep(0.5)

    return jobs


# ── Watchlist scrapers ────────────────────────────────────────────────────────
#
# Pre-vetted companies monitored directly. All confirmed remote-EU or
# Spain-friendly. Location filter relaxed (defaults to "Remote / EU").

WATCHLIST = [
    # ── Tier 1 — primary targets, close domain match ──────────────────────────
    {"name": "Hostaway",       "url": "https://careers.hostaway.com",                         "ats": "html",       "tier": 1},
    {"name": "Pennylane",      "url": "https://jobs.ashbyhq.com/pennylane",                   "ats": "ashby",      "tier": 1},
    {"name": "Dovetail",       "url": "https://jobs.ashbyhq.com/dovetail",                    "ats": "ashby",      "tier": 1},
    {"name": "Too Good To Go", "url": "https://job-boards.greenhouse.io/toogoodtogo",         "ats": "greenhouse", "tier": 1},
    {"name": "Doctolib",       "url": "https://careers.doctolib.com",                         "ats": "html",       "tier": 1},
    {"name": "Pleo",           "url": "https://jobs.ashbyhq.com/pleo",                        "ats": "ashby",      "tier": 1},
    {"name": "Hopper",         "url": "https://jobs.ashbyhq.com/hopper",                      "ats": "ashby",      "tier": 1},
    {"name": "OLX",            "url": "https://jobs.eu.lever.co/olx",                         "ats": "lever",      "tier": 1},
    {"name": "Vanta",          "url": "https://jobs.ashbyhq.com/vanta",                       "ats": "ashby",      "tier": 1},
    {"name": "n8n",            "url": "https://jobs.ashbyhq.com/n8n",                         "ats": "ashby",      "tier": 1},
    # ── Tier 2 — good fit, monitor for openings ───────────────────────────────
    {"name": "Productboard",   "url": "https://www.productboard.com/careers/open-positions/", "ats": "html",       "tier": 2},
    {"name": "Automattic",     "url": "https://automattic.com/work-with-us/",                 "ats": "html",       "tier": 2},
    {"name": "Synthesia",      "url": "https://jobs.ashbyhq.com/synthesia",                   "ats": "ashby",      "tier": 2},
    {"name": "Qonto",          "url": "https://jobs.lever.co/qonto",                          "ats": "lever",      "tier": 2},
    {"name": "Alan",           "url": "https://jobs.ashbyhq.com/alan",                        "ats": "ashby",      "tier": 2},
    {"name": "Attio",          "url": "https://jobs.ashbyhq.com/attio",                       "ats": "ashby",      "tier": 2},
    {"name": "Intercom",       "url": "https://www.intercom.com/careers",                     "ats": "html",       "tier": 2},
    {"name": "Maze",           "url": "https://jobs.ashbyhq.com/mazedesign",                  "ats": "ashby",      "tier": 2},
    {"name": "TheyDo",         "url": "https://jobs.ashbyhq.com/theydo",                      "ats": "ashby",      "tier": 2},
    {"name": "Contentsquare",  "url": "https://jobs.lever.co/contentsquare",                  "ats": "lever",      "tier": 2},
    {"name": "PostHog",        "url": "https://jobs.ashbyhq.com/posthog",                     "ats": "ashby",      "tier": 2},
    {"name": "Apaleo",         "url": "https://job-boards.greenhouse.io/apaleo",              "ats": "greenhouse", "tier": 2},
    {"name": "Notion",         "url": "https://jobs.ashbyhq.com/notion",                      "ats": "ashby",      "tier": 2},
    {"name": "Linear",         "url": "https://jobs.ashbyhq.com/Linear",                      "ats": "ashby",      "tier": 2},
    {"name": "Superhuman",     "url": "https://jobs.ashbyhq.com/superhuman",                  "ats": "ashby",      "tier": 2},
    # ── Tier 3 — speculative / small teams / rare openings ───────────────────
    # Rows removed — 404 on two URL attempts
    {"name": "Raycast",        "url": "https://www.raycast.com/careers",                      "ats": "html",       "tier": 3},
    {"name": "Readdle",        "url": "https://readdle.com/careers",                          "ats": "html",       "tier": 3},
    {"name": "Pitch",          "url": "https://pitch.com/jobs",                               "ats": "html",       "tier": 3},
    {"name": "Granola",        "url": "https://www.granola.ai/jobs",                          "ats": "html",       "tier": 3},
]

WATCHLIST_TIER_LABELS = {1: "⭐ Tier 1", 2: "📌 Tier 2", 3: "🔍 Tier 3"}


def _watchlist_job(title, company, url, location, salary, tier, posted_at=None) -> dict:
    loc = location or "Remote / EU"
    salary = sanitise_salary(salary or "")
    age_label, age_date = parse_age(posted_at)
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
            # Lever's createdAt is epoch milliseconds
            created_ms = p.get("createdAt")
            posted_at = created_ms / 1000 if isinstance(created_ms, (int, float)) else None
            jobs.append(_watchlist_job(
                title, company_name,
                p.get("hostedUrl", base_url),
                location, "", tier,
                posted_at=posted_at,
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
                posted_at=p.get("publishedDate"),
            ))
        return jobs
    except Exception as e:
        print(f"  ⚠ Watchlist Ashby ({company_name}): {e}")
        return []


def _scrape_greenhouse_watchlist(base_url: str, company_name: str, tier: int) -> list[dict]:
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
                posted_at=p.get("first_published") or p.get("updated_at"),
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

# Removed from active rotation:
#   Remotive         — free API gutted by a 2026 paywall change ("0.4% of
#                       available roles" without a paid account). No code fix
#                       possible; moved to the manual-check footer.
#   WorkingNomads,
#   Nodesk,
#   TrulyRemote,
#   DynamiteJobs      — confirmed JS-rendered SPAs. requests+BeautifulSoup only
#                       ever sees the page shell (filters/nav), never the
#                       actual job listings, no matter the selectors. Would
#                       need a headless browser to scrape; moved to manual-check.
#   UIUXDesignerJobs  — domain appears dead (empty page, no content at all).
#                       Dropped entirely, not worth a manual-check link.
SCRAPERS = [
    ("4DayWeek",         scrape_4dayweek),
    ("Himalayas",        scrape_himalayas),
    ("Arbeitnow",        scrape_arbeitnow),
    ("WeWorkRemotely",   scrape_weworkremotely),
    ("UXJobs",           scrape_uxjobs),
    ("RemoteRebellion",  scrape_remoterebellion),
    ("RemoteInEurope",   scrape_remoteineurope),
    ("Watchlist",        scrape_watchlist),
]


def collect_all_jobs(health: dict) -> tuple[list[dict], dict, list[str]]:
    all_jobs = []
    alerts   = []
    today_str = TODAY.isoformat()

    for name, fn in SCRAPERS:
        print(f"→ {name}...")
        try:
            results = fn()
            print(f"  ✓ {len(results)} matching jobs")
            all_jobs.extend(results)

            h = health.setdefault(name, {"last_fetch_date": None, "error_streak": 0, "zero_result_streak": 0})
            h["last_fetch_date"] = today_str
            h["error_streak"]    = 0

            # Fetch succeeded but matched nothing — track separately from fetch
            # errors. A source whose CSS selectors silently stop matching after
            # a site redesign will "succeed" with 0 results forever and never
            # trip the error-streak alert, so this is the only signal that catches it.
            if len(results) == 0:
                h["zero_result_streak"] = h.get("zero_result_streak", 0) + 1
                if h["zero_result_streak"] >= ZERO_RESULT_ALERT_DAYS:
                    alerts.append(
                        f"{name} — 0 matching jobs for {h['zero_result_streak']} consecutive days "
                        f"(fetch succeeds — selectors may be stale)"
                    )
            else:
                h["zero_result_streak"] = 0

        except Exception as e:
            print(f"  ✗ {name} failed: {e}")
            h = health.setdefault(name, {"last_fetch_date": None, "error_streak": 0, "zero_result_streak": 0})
            h["error_streak"] = h.get("error_streak", 0) + 1
            if h["error_streak"] >= ERROR_ALERT_DAYS:
                alerts.append(
                    f"{name} — fetch error for {h['error_streak']} consecutive days: {e}"
                )

        time.sleep(1)

    return all_jobs, health, alerts


# ── Staleness filter ─────────────────────────────────────────────────────────

def filter_stale_jobs(jobs: list[dict]) -> tuple[list[dict], int]:
    """Drop jobs older than MAX_JOB_AGE_DAYS. Safety net for sources with
    unreliable date parsing. Jobs with no age_date pass through."""
    kept, dropped = [], 0
    for j in jobs:
        age_date = j.get("age_date")
        if age_date is None:
            kept.append(j)
            continue
        if (TODAY - age_date).days > MAX_JOB_AGE_DAYS:
            dropped += 1
        else:
            kept.append(j)
    return kept, dropped


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
    new_primary = [j for j in new_jobs    if not j.get("is_stretch")]
    new_stretch = [j for j in new_jobs    if j.get("is_stretch")]
    rep_primary = [j for j in repost_jobs if not j.get("is_stretch")]
    rep_stretch = [j for j in repost_jobs if j.get("is_stretch")]

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
    repost_section  = source_section(rep_primary, label="Reposted roles", is_repost=True)
    stretch_section = source_section(
        new_stretch + rep_stretch,
        label="🔭 Stretch roles — worth checking at smaller companies",
        is_stretch_section=True,
    )

    silence_note = ""
    if is_silence_breaker:
        silence_note = """
        <tr><td style="padding:16px 0 8px;">
          <p style="margin:0;font-size:13px;color:#6b7280;">
            No new roles today, but the scraper ran without errors.
            You'll hear from it again when something new surfaces.
          </p>
        </td></tr>"""

    source_count    = len(SCRAPERS) - 1  # exclude Watchlist
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

    <!-- Manual checks -->
    <tr><td style="padding:0 32px 24px;">
      <p style="margin:0 0 10px;font-size:11px;font-weight:700;color:#374151;
                text-transform:uppercase;letter-spacing:0.06em;">Also check manually</p>
      <table cellpadding="0" cellspacing="0">
        <tr>
          <td style="padding:0 8px 8px 0;">
            <a href="https://www.linkedin.com/jobs/search-results/?currentJobId=4393461497&keywords=%E2%80%98Lead%20product%20designer%E2%80%99&origin=JOB_SEARCH_PAGE_JOB_FILTER&referralSearchId=drPy10xnXjltv1HQD%2FkLdg%3D%3D&geoId=90009761&distance=0.0&f_TPR=r604800&f_SAL=f_SA_id_225001%3A272001"
               style="display:inline-block;padding:5px 12px;background:#0a66c2;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              LinkedIn
            </a>
          </td>
          <td style="padding:0 8px 8px 0;">
            <a href="https://wellfound.com/jobs"
               style="display:inline-block;padding:5px 12px;background:#111827;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              Wellfound
            </a>
          </td>
          <td style="padding:0 8px 8px 0;">
            <a href="https://app.welcometothejungle.com/jobs"
               style="display:inline-block;padding:5px 12px;background:#3d1f8c;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              Welcome to the Jungle
            </a>
          </td>
          <td style="padding:0 8px 8px 0;">
            <a href="https://www.glassdoor.es/Empleo/barcelona-senior-product-designer-empleos-SRCH_IL.0,9_IC2547194_KO10,33.htm?sortBy=date_desc"
               style="display:inline-block;padding:5px 12px;background:#0caa41;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              Glassdoor
            </a>
          </td>
        </tr>
        <tr>
          <td style="padding:0 8px 8px 0;">
            <a href="https://flexa.careers/jobs?q=senior+product+designer"
               style="display:inline-block;padding:5px 12px;background:#6d28d9;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              Flexa
            </a>
          </td>
          <td style="padding:0 8px 8px 0;">
            <a href="https://weloveproduct.co/"
               style="display:inline-block;padding:5px 12px;background:#e11d48;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              WeLoveProduct
            </a>
          </td>
          <td style="padding:0 8px 8px 0;">
            <a href="https://designjobs.world/jobs?q=senior+product+designer&seniority=senior&location_regions%5B%5D=Europe"
               style="display:inline-block;padding:5px 12px;background:#1e293b;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              DesignJobs.World
            </a>
          </td>
        </tr>
        <tr>
          <td style="padding:0 8px 8px 0;">
            <a href="https://remotive.com/remote-jobs/design"
               style="display:inline-block;padding:5px 12px;background:#f97316;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              Remotive
            </a>
          </td>
          <td style="padding:0 8px 8px 0;">
            <a href="https://www.workingnomads.com/jobs?tag=product-design&location=europe"
               style="display:inline-block;padding:5px 12px;background:#0891b2;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              WorkingNomads
            </a>
          </td>
          <td style="padding:0 8px 8px 0;">
            <a href="https://nodesk.co/remote-jobs/design/"
               style="display:inline-block;padding:5px 12px;background:#4c63b6;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              Nodesk
            </a>
          </td>
          <td style="padding:0 8px 8px 0;">
            <a href="https://trulyremote.co/design"
               style="display:inline-block;padding:5px 12px;background:#16003d;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              TrulyRemote
            </a>
          </td>
          <td style="padding:0 8px 8px 0;">
            <a href="https://dynamitejobs.com/remote-jobs/design/ux-web-design"
               style="display:inline-block;padding:5px 12px;background:#ea580c;color:#fff;
                      font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;">
              DynamiteJobs
            </a>
          </td>
        </tr>
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

    all_jobs, stale_dropped = filter_stale_jobs(all_jobs)
    if stale_dropped:
        print(f"🧹 Dropped {stale_dropped} job(s) older than {MAX_JOB_AGE_DAYS} days")

    if alerts:
        print("\n⚠ Health alerts:")
        for a in alerts:
            print(f"  · {a}")

    print(f"\nTotal matching jobs: {len(all_jobs)}")
    new_jobs, repost_jobs, seen = process_jobs(all_jobs, seen)
    save_seen(seen)

    new_primary = [j for j in new_jobs if not j.get("is_stretch")]
    new_stretch = [j for j in new_jobs if j.get("is_stretch")]
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
