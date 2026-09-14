#!/usr/bin/env python3
"""
Job Scraper – Senior / Lead Product Designer
Runs daily via GitHub Actions and sends a digest email via Resend.

Sources (4 APIs + 5 HTML scrapers + watchlist):
  APIs:    4DayWeek, Himalayas, Arbeitnow, RemoteOK
  Scrapers: WeWorkRemotely, UXJobs, RemoteRebellion,
            RemoteInEurope, EURemoteJobs
  Watchlist: 30 pre-vetted companies via Lever / Ashby / Greenhouse / HTML

  Retired (see comment above SCRAPERS below for why): Remotive,
  WorkingNomads, Nodesk, TrulyRemote, DynamiteJobs, UIUXDesignerJobs

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

# Optional outside watchdog (healthchecks.io or similar). The scraper pings it
# on every completed run; if the ping stops arriving, THEY email you.
#
# This exists because the silence-breaker below cannot do that job. It is
# computed inside the run, so it can only report silence when a run happens —
# and the failure it most needs to report is the run not happening at all.
# Confirmed live: the scheduled run was dropped on 10, 11 and 13 Sep 2026 (the
# zero-result streak counters advanced by one between digests instead of three,
# which is only possible if the days in between never ran). Leave unset and
# nothing breaks; the ping is simply skipped.
HEALTHCHECK_URL = os.environ.get("HEALTHCHECK_URL", "")

REPOST_DAYS     = 14    # resurface a seen job if reposted after this many days
PRUNE_DAYS      = 30    # remove seen_jobs entries not seen for this many days
SILENCE_DAYS    = 3     # send a health ping if no email sent for this many days
SALARY_MAX      = 500_000  # sanity cap — values above this are display bugs
MAX_JOB_AGE_DAYS = 21   # hard-drop jobs older than this — safety net for date parsing failures
ERROR_ALERT_DAYS = 3    # consecutive fetch errors before health alert fires
ZERO_RESULT_ALERT_DAYS = 5  # consecutive 0-result days (fetch OK, no matches) before health alert fires

# A watchlist company whose job board returns NO postings at all — not "no
# matching postings", none whatsoever — for this many days is a dead slug or a
# moved ATS. Deliberately not "no matches": a company can legitimately have no
# design role open for months, and alerting on that would be constant noise.
WATCHLIST_DEAD_DAYS = 14

# What to do with a role that is remote but restricted to ONE European country
# other than Spain ("Remote, Poland" · "Portugal Remote" · "Germany (Remote)").
# These are EU-remote in the abstract but need residency in that country, so
# they are not applicable from Barcelona.
#   "flag" — show them, badged and sorted to the bottom (default)
#   "drop" — hard-exclude them, same as a US role
COUNTRY_RESTRICTED_MODE = "flag"

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

# Leadership/C-level — excluded entirely, never shown.
# Matched as substrings, so every entry here must be long enough to be
# unambiguous — see EXCLUDE_TITLE_WORDS below for the short ones.
EXCLUDE_TITLE_KEYWORDS = [
    "vice president",
    "director of design", "design director",
    "chief design officer",
    "head of design",
]

# Short abbreviations, matched as whole words only. "cdo" as a substring also
# fires inside ordinary words (mcdonald, and anything else with those three
# letters in a row) — the same class of bug as the "Remote- UK" leak, caught
# before it cost anything.
EXCLUDE_TITLE_WORDS = {"vp", "cdo", "svp", "evp"}

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

# Location strings arrive with wildly inconsistent separators — "Remote - UK",
# "Remote · UK", "Remote, UK", "Remote- UK", "Remote (USA)", "Remote/US",
# "Remote — United States". Matching raw text meant every new punctuation
# variant was a fresh hole (confirmed live: "Remote- UK" reached the 4 Sep 2026
# digest because only the spaced hyphen was listed). Normalising both sides
# first collapses all of them onto one form, so the rules below are about
# places, not punctuation.
_LOC_SEPARATORS = re.compile(r"[\-–—·•|/\\(),;:\[\]{}]+")

def _norm_loc(text: str) -> str:
    """Lowercase, flatten every separator to a single space, collapse runs."""
    if not text:
        return ""
    return " ".join(_LOC_SEPARATORS.sub(" ", text.lower()).split())


# Tokens that mean "this is not restricted to one country" — presence of any of
# these stops the single-country check below from firing.
BROAD_REGION_TOKENS = {
    "europe", "european", "eu", "eea", "emea", "worldwide", "anywhere",
    "global", "globally", "international", "internationally",
}

# Tokens that mean Spain is in scope — she can take the role, so never flag it.
SPAIN_TOKENS = {
    "spain", "spanish", "es", "barcelona", "madrid", "valencia", "sevilla",
    "seville", "bilbao", "malaga", "zaragoza", "murcia", "palma", "mallorca",
    "canarias", "canary", "catalonia", "catalunya", "cataluna",
}

# European countries that are fine in principle but, named alone, mean
# "you must live here". Matched as whole tokens after normalisation.
EU_COUNTRY_NAMES = {
    "poland": "Poland", "polska": "Poland", "polish": "Poland",
    "portugal": "Portugal", "portuguese": "Portugal",
    "germany": "Germany", "deutschland": "Germany", "german": "Germany",
    "france": "France", "french": "France",
    "italy": "Italy", "italian": "Italy",
    "netherlands": "Netherlands", "holland": "Netherlands", "dutch": "Netherlands",
    "ireland": "Ireland", "irish": "Ireland",
    "belgium": "Belgium", "belgian": "Belgium",
    "austria": "Austria", "austrian": "Austria",
    "sweden": "Sweden", "swedish": "Sweden",
    "denmark": "Denmark", "danish": "Denmark",
    "finland": "Finland", "finnish": "Finland",
    "norway": "Norway", "norwegian": "Norway",
    "switzerland": "Switzerland", "swiss": "Switzerland",
    "czechia": "Czechia", "czech": "Czechia",
    "romania": "Romania", "romanian": "Romania",
    "bulgaria": "Bulgaria", "bulgarian": "Bulgaria",
    "greece": "Greece", "greek": "Greece",
    "hungary": "Hungary", "hungarian": "Hungary",
    "croatia": "Croatia", "croatian": "Croatia",
    "slovakia": "Slovakia", "slovak": "Slovakia",
    "slovenia": "Slovenia", "slovenian": "Slovenia",
    "lithuania": "Lithuania", "lithuanian": "Lithuania",
    "latvia": "Latvia", "latvian": "Latvia",
    "estonia": "Estonia", "estonian": "Estonia",
    "ukraine": "Ukraine", "ukrainian": "Ukraine",
    "serbia": "Serbia", "serbian": "Serbia",
    "turkey": "Turkey", "turkish": "Turkey", "turkiye": "Turkey",
    "cyprus": "Cyprus", "malta": "Malta", "maltese": "Malta",
    "luxembourg": "Luxembourg", "iceland": "Iceland", "albania": "Albania",
    "moldova": "Moldova", "georgia": "Georgia", "armenia": "Armenia",
}

# ISO-2 codes, deliberately only the ones that are not also ordinary words or
# fragments of place names. "de", "fr", "it", "be", "es", "ca" are left out on
# purpose — "Palma de Mallorca" must not read as Germany.
EU_COUNTRY_CODES = {
    "pl": "Poland", "pt": "Portugal", "nl": "Netherlands", "cz": "Czechia",
    "ro": "Romania", "bg": "Bulgaria", "hu": "Hungary", "hr": "Croatia",
    "sk": "Slovakia", "si": "Slovenia", "lt": "Lithuania", "lv": "Latvia",
    "ee": "Estonia", "ua": "Ukraine", "rs": "Serbia", "tr": "Turkey",
    "cy": "Cyprus", "mt": "Malta", "lu": "Luxembourg", "ie": "Ireland",
    "dk": "Denmark", "fi": "Finland", "se": "Sweden", "at": "Austria",
    "ch": "Switzerland", "gr": "Greece",
}


def country_restriction(location: str) -> str:
    """Return the country a remote role is restricted to, or "" if it isn't.

    Fires on "Remote, Poland" · "Remote, PL" · "Portugal Remote" ·
    "Germany (Remote)". Stays silent on anything naming Spain, and on anything
    naming a broad region ("EMEA", "Europe", "Remote International")."""
    norm = _norm_loc(location)
    if not norm:
        return ""
    toks = set(norm.split())
    if toks & SPAIN_TOKENS or toks & BROAD_REGION_TOKENS:
        return ""

    found = []
    for tok in norm.split():
        name = EU_COUNTRY_NAMES.get(tok) or (
            EU_COUNTRY_CODES.get(tok) if len(tok) == 2 else None
        )
        if name and name not in found:
            found.append(name)
    return " / ".join(found)


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


# Filler words that carry no place information, stripped from either end
# before the remainder is read as a country name.
_LOC_FILLER = {"remote", "worldwide", "only", "based", "job", "jobs",
               "hybrid", "onsite", "on", "site", "work", "from", "home",
               "anywhere", "in", "the", "full", "time", "fulltime"}

# ISO-2 and shorthand for the hard-excluded countries above. Same rule as
# EU_COUNTRY_CODES: nothing that doubles as an ordinary word.
_NON_EU_CODE_ALIASES = {
    "namer": "united states", "na": "united states", "usa": "united states",
    "us": "united states", "u s": "united states", "u s a": "united states",
    "uk": "united kingdom", "gb": "united kingdom", "gbr": "united kingdom",
    "britain": "united kingdom", "great britain": "united kingdom",
    "england": "united kingdom", "scotland": "united kingdom",
    "wales": "united kingdom",
    "can": "canada", "aus": "australia", "nz": "new zealand",
    "ind": "india", "bra": "brazil", "sg": "singapore", "ph": "philippines",
    "za": "south africa", "jp": "japan", "kr": "south korea",
}


def location_country_ok(location: str) -> bool:
    """Hard-exclude check for sources that give a bare 'Remote <Country>'
    string (e.g. "Remote US", "Remote Thailand"). location_ok() alone would
    wave these through just because they contain the word "remote" — this
    strips the filler and checks the actual place name underneath.

    Handles the country on either side of the filler: "Remote US" and
    "US Remote" and "Remote (US)" all reduce to "united states"."""
    norm = _norm_loc(location)
    if not norm:
        return True

    bare = " ".join(t for t in norm.split() if t not in _LOC_FILLER).strip()
    if not bare:
        return True

    bare = _NON_EU_CODE_ALIASES.get(bare, bare)
    return bare not in NON_EU_HARD_EXCLUDE_COUNTRIES

# ── Geography hidden in the job title ────────────────────────────────────────
#
# US/UK timezone shorthand. Standalone tokens only — these are abbreviations,
# never fragments of other words, so whole-word matching is both safe and
# necessary ("est" inside "greatest" must not fire).
TITLE_TIMEZONE_TOKENS = {
    "pst", "pdt", "est", "edt", "cst", "cdt", "mst", "mdt", "akst", "hst",
}

# Multi-word timezone and region phrases, matched as substrings of the
# normalised title.
TITLE_EXCLUDE_PHRASES = [
    "pacific time", "eastern time", "central time", "mountain time",
    "pacific timezone", "eastern timezone",
    "united states", "united kingdom", "north america",
    "us based", "usa based", "uk based", "us remote", "uk remote",
    "us only", "uk only", "usa only",
]

# Country and region tokens. Same whole-word rule.
TITLE_EXCLUDE_TOKENS = {
    "us", "usa", "uk", "gb", "canada", "canadian", "england", "britain",
    "american", "americas", "latam", "apac",
}

# Cities, kept deliberately short. Each one here costs a genuine role if it is
# ever ambiguous, and volume is already thin — so this holds only places that
# cannot plausibly mean anything else in a job title. Easy to extend later.
TITLE_EXCLUDE_CITIES = [
    "new york", "nyc", "san francisco", "london", "toronto",
]


def title_location_excluded(title: str) -> bool:
    """True when the TITLE names a place or timezone outside scope.

    Hard-exclude only — deliberately not location_ok(), which requires a
    positive signal ("remote", "europe") to be present. Most titles contain no
    location language at all, so running them through location_ok() would
    reject nearly everything."""
    norm = _norm_loc(title)
    if not norm:
        return False
    if any(p in norm for p in TITLE_EXCLUDE_PHRASES):
        return True
    if any(c in norm for c in TITLE_EXCLUDE_CITIES):
        return True
    words = set(norm.split())
    return bool(words & TITLE_TIMEZONE_TOKENS or words & TITLE_EXCLUDE_TOKENS)


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

def is_blocked_company(company: str, url: str = "") -> bool:
    """Blocked by company name OR by the URL the posting sits on.

    The URL half matters: "rippling" has been on this list all along, but a
    role posted through ats.rippling.com carries the client's name, not
    Rippling's, so the name check never fired. Two roles reached the digest
    that way (21 Aug and 14 Sep 2026)."""
    haystack = company.lower()
    if url:
        haystack = f"{haystack} {url.lower()}"
    return any(blocked in haystack for blocked in US_COMPANY_BLOCKLIST)


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

TODAY = datetime.date.today()

# ── Raw listing counts, per source ───────────────────────────────────────────
#
# How many listings a source handed us BEFORE any filtering. Without this,
# "the site changed and we can no longer read it" and "the site is fine,
# nothing matched your filters" produce identical output — zero jobs — and the
# health alert cannot tell you which. Three sources spent over a week alerting
# with nobody able to say which of the two it was.
#
# Each scraper records its count as soon as it has parsed the listings; the
# alert builder reads it. A source that never records one reports "unknown".
SOURCE_RAW_COUNTS: dict[str, int] = {}

# Per-company watchlist detail: postings returned and the last error, if any.
# The watchlist reports as a single source, so one dead company is invisible
# behind twenty-nine working ones.
WATCHLIST_RAW: dict[str, dict] = {}


def _record_raw(source: str, count: int, add: bool = False):
    if add:
        SOURCE_RAW_COUNTS[source] = SOURCE_RAW_COUNTS.get(source, 0) + count
    else:
        SOURCE_RAW_COUNTS[source] = count

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
    """Excluded for seniority (VP/Director/CDO) or for geography.

    Geography in the title is a real and separate hole: every location rule in
    this file reads the location FIELD, and postings increasingly put the place
    in the title while leaving the field as a bare "Remote". Both of these
    reached the digest with location == "Remote":
      · "Senior Product Designer, London, UK"                  (14 Sep 2026)
      · "Lead Product Designer ... Remote, PST Time Zone"      (12 Sep 2026)
    """
    t = title.lower()
    if any(kw in t for kw in EXCLUDE_TITLE_KEYWORDS):
        return True
    words = set(_norm_loc(title).split())
    if words & EXCLUDE_TITLE_WORDS:
        return True
    return title_location_excluded(title)

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

# Exclusion phrases, pre-normalised once at import so that punctuation variants
# ("remote · usa", "remote - usa", "remote, usa") all collapse to the same
# thing and the raw list above stays human-readable.
_EXCLUDE_LOCATION_NORM = sorted({_norm_loc(x) for x in EXCLUDE_LOCATION if _norm_loc(x)})

# Standalone tokens that mean a hard-excluded country wherever they appear.
_EXCLUDE_LOCATION_TOKENS = {"usa", "us", "uk", "gb", "canada", "britain", "england"}


def location_ok(location: str) -> bool:
    loc = _norm_loc(location)
    if not loc:
        return True
    if any(ex in loc for ex in _EXCLUDE_LOCATION_NORM):
        return False
    if _EXCLUDE_LOCATION_TOKENS & set(loc.split()):
        return False
    if not any(kw in loc for kw in LOCATION_KEYWORDS):
        return False
    # "Remote <Country>" needs the country read, not just the word "remote".
    # Previously only the three sources added on 4 Sep 2026 called this; every
    # other source waved "Remote, PL" through. Folding it in here means one
    # rule for all of them.
    return location_country_ok(location)

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
    # Normalised like every other location check — this was the one function
    # left reading raw text after the 10 Sep 2026 cleanup.
    loc = _norm_loc(location)
    if any(_norm_loc(sig) in loc for sig in SPAIN_ONLY_SIGNALS):
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
            _record_raw("4DayWeek", len(items), add=True)
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
                _record_raw("Himalayas", len(items), add=True)
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
            _record_raw("Arbeitnow", len(items), add=True)
            if not items:
                break
            for j in items:
                title = j.get("title", "")
                if not title_matches_any(title):
                    continue
                company_name = j.get("company_name", "")
                if is_blocked_company(company_name):
                    continue
                # Arbeitnow's `remote` boolean is unreliable — confirmed live
                # (Sep 2026): explicitly remote-labeled titles like "Full Remote
                # - UK" come back with remote=false, while fixed-city onsite
                # roles (Cologne, Stuttgart) come back remote=true. Trusting
                # this flag alone silently discarded every genuine match,
                # which is why this source sat at a 0-result health alert for
                # weeks despite matching titles existing in the raw feed. Use
                # a broader remote signal instead — API flag OR "remote"
                # appearing in the title/location text — then let
                # location_ok() below do the real EU/non-EU filtering.
                location = j.get("location", "") or "Remote"
                remote_signal = (
                    j.get("remote", False)
                    or "remote" in title.lower()
                    or "remote" in location.lower()
                )
                if not remote_signal:
                    continue
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
    items = soup.find_all("item")
    _record_raw("WeWorkRemotely", len(items))
    for item in items:
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

    cards = soup.select("article.card")
    _record_raw("UXJobs", len(cards))

    seen_urls = set()
    for card in cards:
        link_el = card.select_one("a.card-link[href]") or card.select_one("a[href]")
        loc_el = card.select_one(".card-loc")
        title_el = card.select_one(".card-title")
        if not link_el or not loc_el or not title_el:
            continue

        href = link_el["href"]
        if href in seen_urls:
            continue
        seen_urls.add(href)

        # UXJobs aggregates a chunk of listings from jobs.dou.ua — Ukraine's
        # IT jobs board. Confirmed live (Sep 2026): those cards show only a
        # generic "🌍 Remote" location with no country signal, but the actual
        # dou.ua posting underneath is a Ukrainian outsourcing agency hiring
        # for specific Ukrainian cities ("remote" meaning remote-within-
        # Ukraine, not EU-remote). Neither the flag check nor location_ok()
        # can catch this from the card text alone — the domain itself is the
        # only reliable signal, so hard-exclude it here.
        if "dou.ua" in href:
            continue

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

    # Raw count is "anchors shaped like a posting", since this page has no job
    # markup to count — that shape going to zero is the signal the Squarespace
    # block changed.
    anchors = soup.find_all("a", href=True)
    _record_raw("RemoteRebellion", sum(
        1 for a in anchors if re.match(r'^(.*)\(([^)]+)\)\s*$', a.get_text(strip=True))
    ))

    seen_urls = set()
    for a in anchors:
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

        cards = soup.select("a.card.job")
        _record_raw("RemoteInEurope", len(cards), add=True)

        for card in cards:
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


def scrape_remoteok() -> list[dict]:
    """remoteok.com/api — free public JSON feed, no auth, capped at ~100
    most-recent results per request (no pagination on the free tier).
    Scoped with ?tags=design to stay relevant, but tags are NOT trusted as
    the real filter — confirmed live (Sep 2026) that tagging is noisy (a
    "Residential Technician" posting was tagged "design" alongside a dozen
    unrelated tags). Every posting still runs through the same
    title_matches_any() every other source uses.
    Attribution: Remote OK's API terms ask for a link back to the original
    posting URL and to be credited as the source — both already happen here
    (url field + "RemoteOK" source label).
    """
    jobs = []
    try:
        r = requests.get(
            "https://remoteok.com/api",
            params={"tags": "design"},
            headers=HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  ⚠ RemoteOK error: {e}")
        return jobs

    _record_raw("RemoteOK", sum(
        1 for j in data if isinstance(j, dict) and "position" in j
    ))

    for j in data:
        if not isinstance(j, dict) or "position" not in j:
            continue  # first element is a legal/terms blob, not a job
        title = j.get("position", "")
        if not title_matches_any(title):
            continue
        company_name = j.get("company", "")
        if is_blocked_company(company_name):
            continue
        location = (j.get("location", "") or "").strip().rstrip(",").strip() or "Remote"
        if not location_ok(location):
            continue
        description = j.get("description", "") or ""
        if is_us_description(description):
            continue
        sal_min = j.get("salary_min") or 0
        sal_max = j.get("salary_max") or 0
        salary = f"${sal_min:,} – ${sal_max:,}" if sal_min and sal_max else ""
        salary = sanitise_salary(salary)
        url = j.get("url") or j.get("apply_url") or ""
        age_label, age_date = parse_age(j.get("epoch") or j.get("date"))
        jobs.append({
            "title":         title,
            "company":       company_name,
            "location":      location,
            "salary":        salary,
            "url":           url,
            "source":        "RemoteOK",
            "four_day":      False,
            "spain_flag":    is_spain_only(location),
            "currency_flag": currency_flag(salary),
            "age_label":     age_label,
            "age_date":      age_date,
            "is_stretch":    title_is_stretch(title),
        })
    return jobs


def scrape_euremotejobs() -> list[dict]:
    """euremotejobs.com — confirmed server-rendered (job data is present in
    the raw HTML, not JS-injected). Each posting is a <div class="job-card">
    wrapped in a parent <a href>. Paginated via ?paged=N, capped at 3 pages
    (~120 postings) per run to keep the request count sane.
    """
    jobs = []
    seen_urls = set()
    for page in range(1, 4):
        soup = fetch(f"https://euremotejobs.com/jobs/?search_keywords=design&paged={page}")
        if not soup:
            break

        cards = soup.select(".job-card")
        _record_raw("EURemoteJobs", len(cards), add=True)
        if not cards:
            break

        for card in cards:
            title_el = card.select_one(".job-title")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title_matches_any(title):
                continue

            link_el = card.find_parent("a", href=True)
            href = link_el["href"] if link_el else ""
            if not href or href in seen_urls:
                continue
            seen_urls.add(href)

            company_el = card.select_one(".company-name")
            company = company_el.get_text(strip=True) if company_el else ""
            if is_blocked_company(company):
                continue

            loc_el = card.select_one(".meta-location")
            location = loc_el.get_text(strip=True) if loc_el else "Europe"
            if not location_ok(location) or not location_country_ok(location):
                continue

            time_el = card.select_one("time[datetime]")
            raw_date = time_el["datetime"] if time_el and time_el.has_attr("datetime") else None
            age_label, age_date = parse_age(raw_date)

            jobs.append({
                "title":         title,
                "company":       company,
                "location":      location,
                "salary":        "",
                "url":           href,
                "source":        "EURemoteJobs",
                "four_day":      False,
                "spain_flag":    is_spain_only(location),
                "currency_flag": "",
                "age_label":     age_label,
                "age_date":      age_date,
                "is_stretch":    title_is_stretch(title),
            })

        if len(cards) < 40:
            break
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
    {"name": "Octopus Energy", "url": "https://jobs.lever.co/octoenergy",                     "ats": "lever",      "tier": 2},
    # ── Tier 3 — speculative / small teams / rare openings ───────────────────
    # Rows removed — 404 on two URL attempts
    {"name": "Raycast",        "url": "https://www.raycast.com/careers",                      "ats": "html",       "tier": 3},
    {"name": "Readdle",        "url": "https://readdle.com/careers",                          "ats": "html",       "tier": 3},
    {"name": "Pitch",          "url": "https://pitch.com/jobs",                               "ats": "html",       "tier": 3},
    {"name": "Granola",        "url": "https://www.granola.ai/jobs",                          "ats": "html",       "tier": 3},
]

WATCHLIST_TIER_LABELS = {1: "⭐ Tier 1", 2: "📌 Tier 2", 3: "🔍 Tier 3"}


def _watchlist_job(title, company, url, location, salary, tier, posted_at=None) -> dict:
    """Build a watchlist row.

    Note what this no longer does. It used to substitute "Remote / EU" whenever
    a location was missing — which is most of the HTML-scraped companies, since
    a careers page rarely marks one up. That string was not read from anywhere;
    it was invented and then shown as fact. A missing location is now shown as
    missing and badged, so a US-only role at a watchlist company reads as
    unverified rather than as European."""
    loc = (location or "").strip()
    salary = sanitise_salary(salary or "")
    age_label, age_date = parse_age(posted_at)
    return {
        "title":            title,
        "company":          company,
        "location":         loc or "Location not listed",
        "salary":           salary,
        "url":              url,
        "source":           f"Watchlist · {company}",
        "four_day":         False,
        "spain_flag":       is_spain_only(loc),
        "currency_flag":    currency_flag(salary),
        "age_label":        age_label,
        "age_date":         age_date,
        "watchlist":        True,
        "watchlist_tier":   tier,
        "location_unknown": not loc,
        "is_stretch":       title_is_stretch(title),
    }


def _watchlist_location_ok(location: str) -> bool:
    """Location gate for watchlist companies.

    Watchlist roles used to skip location filtering entirely — the helpers
    checked the title and nothing else, so a "Senior Product Designer, San
    Francisco" at a watchlist company would have gone straight into the digest.

    An EMPTY location still passes. These are thirty hand-picked companies and
    a missing location is not evidence of anything; the row is badged instead
    so the gap is visible. A location that is present must clear the same bar
    as every other source."""
    if not (location or "").strip():
        return True
    return location_ok(location)


def _scrape_lever_watchlist(base_url: str, company_name: str, tier: int) -> list[dict]:
    slug = base_url.rstrip("/").split("/")[-1]
    try:
        r = requests.get(
            f"https://api.lever.co/v0/postings/{slug}?mode=json",
            timeout=15,
        )
        r.raise_for_status()
        postings = r.json()
        WATCHLIST_RAW[company_name] = {"postings": len(postings), "error": None}
        jobs = []
        for p in postings:
            title = p.get("text", "")
            if not title_matches_any(title):
                continue
            location = p.get("categories", {}).get("location", "")
            if not _watchlist_location_ok(location):
                continue
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
        WATCHLIST_RAW[company_name] = {"postings": 0, "error": str(e)[:120]}
        return []


def _scrape_ashby_watchlist(base_url: str, company_name: str, tier: int) -> list[dict]:
    slug = base_url.rstrip("/").split("/")[-1]
    try:
        r = requests.get(
            f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
            timeout=15,
        )
        r.raise_for_status()
        postings = r.json().get("jobs", [])
        WATCHLIST_RAW[company_name] = {"postings": len(postings), "error": None}
        jobs = []
        for p in postings:
            title = p.get("title", "")
            if not title_matches_any(title):
                continue
            loc = p.get("location") or p.get("locationName") or ""
            if isinstance(loc, list):
                loc = ", ".join(loc)
            if not _watchlist_location_ok(loc):
                continue
            jobs.append(_watchlist_job(
                title, company_name,
                p.get("jobUrl", base_url),
                loc, "", tier,
                posted_at=p.get("publishedDate"),
            ))
        return jobs
    except Exception as e:
        print(f"  ⚠ Watchlist Ashby ({company_name}): {e}")
        WATCHLIST_RAW[company_name] = {"postings": 0, "error": str(e)[:120]}
        return []


def _scrape_greenhouse_watchlist(base_url: str, company_name: str, tier: int) -> list[dict]:
    slug = base_url.rstrip("/").split("/")[-1]
    try:
        r = requests.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
            timeout=15,
        )
        r.raise_for_status()
        postings = r.json().get("jobs", [])
        WATCHLIST_RAW[company_name] = {"postings": len(postings), "error": None}
        jobs = []
        for p in postings:
            title = p.get("title", "")
            if not title_matches_any(title):
                continue
            loc = p.get("location", {}).get("name", "") if isinstance(p.get("location"), dict) else ""
            if not _watchlist_location_ok(loc):
                continue
            jobs.append(_watchlist_job(
                title, company_name,
                p.get("absolute_url", base_url),
                loc, "", tier,
                posted_at=p.get("first_published") or p.get("updated_at"),
            ))
        return jobs
    except Exception as e:
        print(f"  ⚠ Watchlist Greenhouse ({company_name}): {e}")
        WATCHLIST_RAW[company_name] = {"postings": 0, "error": str(e)[:120]}
        return []


def _scrape_html_watchlist(url: str, company_name: str, tier: int) -> list[dict]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        anchors = soup.find_all("a", href=True)
        # A careers page has no posting markup to count, so "did the page come
        # back with links at all" is the only available liveness signal. Zero
        # anchors means the page is empty or blocked, not that nothing is open.
        WATCHLIST_RAW[company_name] = {"postings": len(anchors), "error": None}
        jobs = []
        seen_hrefs = set()
        for a in anchors:
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
        WATCHLIST_RAW[company_name] = {"postings": 0, "error": str(e)[:120]}
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
        raw = WATCHLIST_RAW.get(name, {})
        print(f"  · {name}: {len(jobs)} match(es) of {raw.get('postings', '?')} posting(s)"
              + (f" [ERROR: {raw['error']}]" if raw.get("error") else ""))
        all_jobs.extend(jobs)
        time.sleep(0.5)

    _record_raw("Watchlist", sum(c.get("postings", 0) for c in WATCHLIST_RAW.values()))
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
    ("RemoteOK",         scrape_remoteok),
    ("EURemoteJobs",     scrape_euremotejobs),
    ("Watchlist",        scrape_watchlist),
]

# Sources that return structured data over an API. A 0-result day here cannot
# be stale CSS selectors — there are no selectors — so the health alert says
# something different. (Arbeitnow spent Aug–Sep 2026 alerting about selectors
# it does not have; the actual bug was its `remote` boolean returning
# backwards.) Everything not listed here is an HTML scrape.
API_SOURCES = {"4DayWeek", "Himalayas", "Arbeitnow", "RemoteOK"}


def _zero_result_reason(name: str) -> str:
    """Say which of the two zero-result cases this is.

    Reading nothing and matching nothing are completely different problems and
    used to produce identical text, which is why three of these alerts ran for
    over a week with no way to act on them. The raw count separates them."""
    raw = SOURCE_RAW_COUNTS.get(name)

    if raw is None:
        return "no listing count recorded — the source failed before parsing"

    if raw == 0:
        if name in API_SOURCES:
            return ("BROKEN — the API returned 0 listings at all. The endpoint, "
                    "its parameters or its response shape has changed")
        if name == "Watchlist":
            return "BROKEN — no company job board returned a single posting"
        return ("BROKEN — 0 listings parsed from the page. The site's HTML "
                "changed and the selectors no longer match")

    return (f"source is HEALTHY — {raw} listings read, none matched the title "
            f"and location filters. Nothing to fix unless you expected a match")


def _watchlist_company_alerts(health: dict) -> list[str]:
    """Per-company watchlist health.

    The watchlist reports as one source, so a company whose board has gone dead
    is invisible behind the twenty-nine that still work. The trigger is zero
    postings of ANY kind — not zero matches, which is the normal state for a
    company with no design role open."""
    alerts = []
    companies = health.setdefault("Watchlist", {}).setdefault("companies", {})
    today_str = TODAY.isoformat()

    for name, raw in WATCHLIST_RAW.items():
        rec = companies.setdefault(name, {"dead_streak": 0, "last_ok": None, "last_error": None})
        if raw.get("postings", 0) > 0 and not raw.get("error"):
            rec["dead_streak"] = 0
            rec["last_ok"] = today_str
            rec["last_error"] = None
            continue

        rec["dead_streak"] = rec.get("dead_streak", 0) + 1
        rec["last_error"] = raw.get("error")
        if rec["dead_streak"] >= WATCHLIST_DEAD_DAYS:
            detail = f": {raw['error']}" if raw.get("error") else " (board responded, but with no postings)"
            alerts.append(
                f"Watchlist · {name} — no postings for {rec['dead_streak']} days{detail}. "
                f"Check the slug, or whether they moved ATS"
            )
    return alerts


def apply_country_restriction(jobs: list[dict]) -> list[dict]:
    """Final pass over every job, whatever source it came from.

    Applied here rather than inside each scraper so there is one rule for all
    ten sources instead of ten copies of it:
      · drop anything blocked by the URL it sits on (the company-name check
        inside each scraper cannot see ats.rippling.com)
      · tag or drop roles restricted to one European country other than Spain
        (see COUNTRY_RESTRICTED_MODE)"""
    out = []
    for j in jobs:
        if is_blocked_company(j.get("company", ""), j.get("url", "")):
            continue
        country = country_restriction(j.get("location", ""))
        if country and COUNTRY_RESTRICTED_MODE == "drop":
            continue
        j["country_flag"] = country
        out.append(j)
    return out


def collect_all_jobs(health: dict) -> tuple[list[dict], dict, list[str]]:
    all_jobs = []
    alerts   = []
    today_str = TODAY.isoformat()

    for name, fn in SCRAPERS:
        print(f"→ {name}...")
        try:
            results = fn()
            results = apply_country_restriction(results)
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
                        f"({_zero_result_reason(name)})"
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

    alerts.extend(_watchlist_company_alerts(health))

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
    if j.get("country_flag"):
        badges += (f'<span style="display:inline-block;background:#fef2f2;color:#9f1239;'
                   f'font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;'
                   f'margin-right:5px;">📍 {j["country_flag"]} only — needs residency</span>')
    if j.get("location_unknown"):
        badges += ('<span style="display:inline-block;background:#f3f4f6;color:#4b5563;'
                   'font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;'
                   'margin-right:5px;">❓ Location not stated — check the posting</span>')
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
    country_count  = sum(1 for j in new_jobs + repost_jobs if j.get("country_flag"))

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
        if country_count:
            pills += f"""
        &nbsp;<span style="background:#fef2f2;color:#9f1239;font-size:13px;font-weight:600;padding:4px 12px;border-radius:20px;">
          📍 {country_count} × single-country
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
        for j in sorted(jobs, key=lambda x: (not x.get("four_day"),
                                             bool(x.get("country_flag")),
                                             x.get("spain_flag", False))):
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
        📍 Single country — needs residency there &nbsp;|&nbsp;
        ❓ Location not stated on the listing &nbsp;|&nbsp;
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
      <div style="line-height:2.4;">
        <a href="https://www.linkedin.com/jobs/search-results/?currentJobId=4393461497&keywords=%E2%80%98Lead%20product%20designer%E2%80%99&origin=JOB_SEARCH_PAGE_JOB_FILTER&referralSearchId=drPy10xnXjltv1HQD%2FkLdg%3D%3D&geoId=90009761&distance=0.0&f_TPR=r604800&f_SAL=f_SA_id_225001%3A272001"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#0a66c2;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">LinkedIn</a>
        <a href="https://wellfound.com/jobs"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#111827;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">Wellfound</a>
        <a href="https://app.welcometothejungle.com/jobs"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#3d1f8c;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">WTTJ</a>
        <a href="https://www.glassdoor.es/Empleo/barcelona-senior-product-designer-empleos-SRCH_IL.0,9_IC2547194_KO10,33.htm?sortBy=date_desc"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#0caa41;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">Glassdoor</a>
        <a href="https://flexa.careers/jobs?q=senior+product+designer"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#6d28d9;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">Flexa</a>
        <a href="https://weloveproduct.co/"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#e11d48;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">WeLoveProduct</a>
        <a href="https://designjobs.world/jobs?q=senior+product+designer&seniority=senior&location_regions%5B%5D=Europe"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#1e293b;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">DesignJobs.World</a>
        <a href="https://remotive.com/remote-jobs/design"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#f97316;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">Remotive</a>
        <a href="https://www.workingnomads.com/jobs?tag=product-design&location=europe"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#0891b2;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">WorkingNomads</a>
        <a href="https://nodesk.co/remote-jobs/design/"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#4c63b6;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">Nodesk</a>
        <a href="https://trulyremote.co/design"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#16003d;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">TrulyRemote</a>
        <a href="https://dynamitejobs.com/remote-jobs/design/ux-web-design"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#ea580c;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">DynamiteJobs</a>
        <a href="https://funded-drop.app/inbox"
           style="display:inline-block;margin:0 6px 6px 0;padding:5px 12px;background:#059669;color:#fff;
                  font-size:12px;font-weight:600;text-decoration:none;border-radius:6px;white-space:nowrap;">Funded Drop</a>
      </div>
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

def ping_watchdog(status: str = ""):
    """Tell the outside watchdog this run completed. Never fails the run."""
    if not HEALTHCHECK_URL:
        return
    try:
        requests.get(HEALTHCHECK_URL.rstrip("/") + (f"/{status}" if status else ""), timeout=10)
        print("📡 Watchdog pinged.")
    except Exception as e:
        print(f"  ⚠ Watchdog ping failed (ignored): {e}")


def main():
    print(f"\n{'='*52}")
    print(f"Job Scraper – {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*52}\n")

    seen   = load_seen()
    health = load_health()

    # Two scheduled runs a day now, because GitHub's free scheduler drops a
    # large share of them (10, 11 and 13 Sep 2026 never ran). Whichever fires
    # first does the work; this stops the second one sending a duplicate.
    if health.get("last_email_date") == TODAY.isoformat():
        print("✅ Already sent today — nothing to do. (Second scheduled run.)")
        ping_watchdog()
        return

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
        ping_watchdog()
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

    ping_watchdog()


if __name__ == "__main__":
    main()
