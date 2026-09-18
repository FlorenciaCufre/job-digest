# Job Scraper – Project Context

_Last updated: 18 September 2026. Verified against the code in this commit._

**This file lives in the repo, next to `scraper.py`.** It used to live only in
`Documents/Claude New/Scrapper/`, where it went four days stale without anyone
noticing — the document describing the source of truth did not travel with it.
If you change the scraper, change this in the same commit.

---

## What it does

Runs from GitHub Actions and emails a filtered digest of Senior/Lead Product
Designer roles.

**Total sources:** 5 APIs (4DayWeek, Himalayas, Arbeitnow, RemoteOK, Jobicy)
+ 3 HTML scrapers (WeWorkRemotely, UXJobs, RemoteRebellion) + 30 watchlist
companies.

---

## Infrastructure

| Piece | Detail |
|---|---|
| **Runner** | GitHub Actions — free tier |
| **Email** | Resend API — `jobs@florcufre.com` as FROM address |
| **Domain** | `florcufre.com` via GoDaddy, DNS managed by Squarespace |
| **Schedule** | **Twice daily — `30 6 * * *` and `30 11 * * *` (UTC)** |
| **Watchdog** | healthchecks.io, via the `HEALTHCHECK_URL` secret |
| **Secrets** | `RESEND_API_KEY`, `EMAIL_TO`, `EMAIL_FROM`, `HEALTHCHECK_URL` |
| **Cache** | `seen_jobs.json` + `source_health.json` via GitHub Actions cache |
| **Repo** | `github.com/FlorenciaCufre/job-digest` |

**To reset deduplication:** GitHub → Actions → Caches → delete all
`scraper-state-*` entries.

### Why it runs twice a day

GitHub's free scheduler does not guarantee a scheduled run happens at all.
Measured on this repo: **the run did not happen on 10, 11 or 13 September 2026
— three days out of six.** Proven by the zero-result streak counters, which
advanced by one step between digests instead of three; days that ran counted,
days that did not, did not. Delivery had also drifted from ~08:00 UTC to as
late as 18:06 UTC.

Two attempts give each day two chances. Whichever fires first does the work —
`main()` exits immediately if `health["last_email_date"]` is already today, so
the second run costs nothing. **This buys more chances, not punctuality.** A
reliably-timed digest means leaving GitHub Actions, which is not worth it.

### Why every source has a time limit

**On 15 September 2026 a run started at 11:58 UTC, printed nothing, and was
still hanging 3h27m later.** No digest that day. The same signature — starts,
never finishes — most likely explains 10, 11 and 13 September as well; a hung
run never emails, never saves its counters and never increments a streak, which
is indistinguishable from a run that never happened.

The cause: `timeout` in `requests` is **not** a cap on total time. It limits the
gap between bytes. A server that accepts the connection and then says nothing,
or dribbles a byte every few seconds, holds the scraper forever. Sites that
throttle datacenter IPs — which is what a GitHub runner is — behave exactly
that way, and it also explains sources that return an empty challenge page on
other days (a "healthy, 0 matched" alert).

Three defences, innermost first:

| Layer | Setting | What it does |
|---|---|---|
| Per request | `HTTP_TIMEOUT = (10, 20)` | connect and read limits |
| Per source | `SOURCE_TIMEOUT_SECONDS = 90`, `WATCHLIST_TIMEOUT_SECONDS = 300` | SIGALRM interrupts a blocked socket read; the source raises, lands in the existing handler, is recorded as a fetch error, **and the other sources still run** |
| Per run | `RUN_BUDGET_SECONDS = 600` | once spent, remaining sources are skipped and the digest is sent from what was collected — a partial digest beats none |

The watchlist also keeps its own deadline across the 30 companies, so one
stalling company cannot spend the whole budget. Companies skipped that way are
**not** recorded in `WATCHLIST_RAW` — a company never asked is unknown, not
dead, and recording a zero would feed the 14-day dead-slug alert a false
negative.

Line buffering is switched on at the top of `scraper.py` itself
(`sys.stdout.reconfigure`). Without it Python buffers its output and a hang
produces a completely empty log — which is why the 15 Sep failure gave no clue
about which source was stuck.

**Both of these live in `scraper.py` on purpose, not in the workflow file.**
The workflow sits in a hidden folder that is awkward to edit and cannot be
written to remotely, so keeping the knobs in the Python file means routine
changes only ever touch one file.

### Why there is an outside watchdog

The silence-breaker (`SILENCE_DAYS`) is computed *inside* the run, so it can
only report silence when a run happens — and the failure it most needs to
report is the run not happening. A watchdog cannot live inside the thing it
watches.

The scraper pings `HEALTHCHECK_URL` on every completed run, including the
early-exit second run of the day. healthchecks.io is set to **period 1 day,
grace 12 hours**, so 36 hours of silence triggers an email from them. Leave the
secret unset and the ping is skipped; nothing else changes.

---

## Sources

### APIs (reliable — structured data)

| Source | Notes |
|---|---|
| **4DayWeek** | Public API v2 — 4-day week companies only. When `remote_allowed` is empty, drops jobs with USD salary currency |
| **Himalayas** | Search API, paginated 20/page. Must run `Senior` and `Lead` as two separate requests — a comma-joined `seniority=senior,lead` silently returns empty (confirmed live, Aug 2026) |
| **Arbeitnow** | Free API. Its `remote` boolean is unreliable — confirmed live Sep 2026 that explicitly remote-labeled titles come back `remote:false` while fixed-city onsite roles come back `remote:true`. Filter uses a broader signal instead: API flag OR "remote" in the title/location text, then `location_ok()` does the real filtering |
| **RemoteOK** | Free public JSON feed (`remoteok.com/api?tags=design`), no auth, capped ~100 most-recent results, no pagination. Tags are noisy, so every posting still runs through the normal title filter |
| **Jobicy** | Added 16 Sep 2026. `jobicy.com/api/v2/remote-jobs?industry=design-multimedia&geo=europe` — no key, no auth. Returns structured `jobTitle`, `companyName`, `jobGeo`, `jobLevel`, `pubDate`, optional salary. Replaced RemoteInEurope and EURemoteJobs |

### HTML scrapers (best-effort — break when sites redesign)

WeWorkRemotely (RSS) · UXJobs · RemoteRebellion

**UXJobs:** scrapes `jobs.uxjobs.io/remote-product-designer-jobs/`. Country flag
emoji are a fast pre-check only — the real location text always runs through
`location_ok()`. Hard-excludes any card linking to `jobs.dou.ua` (Ukraine's IT
board): those show a generic "🌍 Remote" but are Ukrainian agencies hiring
remote-within-Ukraine. Confirmed four times, 17 Aug – 3 Sep 2026; the exclude
landed 4 Sep and nothing from that domain has appeared since.

### Sources removed (confirmed, not guessed)

- **Remotive** — free API gutted by a 2026 paywall change
- **WorkingNomads, Nodesk, TrulyRemote, DynamiteJobs** — JS-rendered SPAs;
  requests+BeautifulSoup only ever sees the page shell
- **UIUXDesignerJobs** — domain dead
- **RemoteInEurope, EURemoteJobs** — removed 16 Sep 2026. Both reported
  **BROKEN** by the health check in the 15 Sep digest: "0 listings parsed from
  the page" for 10 and 8 consecutive days running. Their HTML changed and the
  selectors match nothing. This is the health alert working as designed — it
  named which two were dead and cleared RemoteOK in the same email
- Earlier round: DailyRemote · RemotifyEurope · RemoteRocketship · FreshRemote ·
  StartupJobs · SmoothRemote · Rows (403/404/DNS)

Their scraper functions were **deleted from the file on 14 Sep 2026** — about
150 lines that could not run and made the file confusing to edit. They are in
git history if ever needed.

**Evaluated, not added:** Landing.Jobs, Jobspresso, Tibber — client-rendered.
**EURES** — no confirmed public API, likely an Angular SPA. Worth a dedicated
look one day.

### Manual check links (email footer)

LinkedIn · Wellfound · WTTJ · Glassdoor · Flexa · WeLoveProduct ·
DesignJobs.World · Remotive · WorkingNomads · Nodesk · TrulyRemote ·
DynamiteJobs · Funded Drop

**Funded Drop** is a link, not a feed. Its data sits behind session-cookie auth,
and the product's value is its in-app 👍/👎 loop. Automating it would mean
storing a personal login as a CI secret and would bypass that loop.

---

## Company watchlist (30)

**ATS types:** Ashby (`api.ashbyhq.com/posting-api/job-board/{slug}`) · Lever
(`api.lever.co/v0/postings/{slug}?mode=json`) · Greenhouse
(`boards-api.greenhouse.io/v1/boards/{slug}/jobs`) · HTML (scrapes careers-page
anchor text — fragile, prefer ATS).

⭐ **Tier 1:** Hostaway (html) · Pennylane · Dovetail · Too Good To Go (gh) ·
Doctolib (html) · Pleo · Hopper · OLX (lever) · Vanta · n8n

📌 **Tier 2:** Productboard (html) · Automattic (html) · Synthesia · Qonto
(lever) · Alan · Attio · Intercom (html) · Maze (slug `mazedesign`) · TheyDo ·
Contentsquare (lever) · PostHog · Apaleo (gh) · Notion · Linear · Superhuman ·
Octopus Energy (lever, slug `octoenergy`)

🔍 **Tier 3:** Raycast · Readdle · Pitch · Granola (all html)

*(Unmarked = Ashby. Hotjar → acquired by Contentsquare, replaced.)*

### Watchlist roles are filtered like everything else — since 14 Sep 2026

Before that they were matched on **title alone**. No location check at all, so a
"Senior Product Designer, San Francisco" at a watchlist company went straight
into the digest. Worse, the nine HTML-scraped companies have no location markup,
and `_watchlist_job()` substituted **"Remote / EU"** — a string read from
nowhere, invented and shown as fact.

Now: `_watchlist_location_ok()` applies the normal rules. An **empty** location
still passes — these are hand-picked companies and a missing field proves
nothing — but the row is badged `❓ Location not stated` instead of being
dressed up as European.

### Per-company health

Thirty companies used to report as one source, so a dead slug was invisible
behind twenty-nine working ones. `WATCHLIST_RAW` now records postings returned
and any error per company, and `_watchlist_company_alerts()` fires after
**`WATCHLIST_DEAD_DAYS` (14) days with zero postings of any kind.**

**Deliberately not "zero matches"** — a company can legitimately have no design
role open for months, and alerting on that would be constant noise. Zero
postings *at all* means a dead slug or a moved ATS.

---

## Filtering logic

### Title matching

**Primary:** Lead Product Designer · Senior Product Designer · Lead Designer

**Stretch** (separate section): Principal/Staff Product Designer ·
Principal/Staff Designer · Head of Product Design

**Excluded outright:** Vice President · Director of Design · Design Director ·
Chief Design Officer · Head of Design — plus `vp`, `cdo`, `svp`, `evp` matched
as **whole words**. They used to be substrings, which also fired inside ordinary
words (`cdo` inside "mcdonald"). Same class of bug as the location one below.

⚠️ **Open question:** the role band says no Head-of and no Staff, but Staff,
Principal and Head of Product Design are routed to the stretch section. Both
cannot be right. Flor's call; unchanged pending it.

### Geography in the title — added 14 Sep 2026

Every location rule reads the location *field*. These reached the digest with
the field reading only "Remote":

- `Senior Product Designer, London, UK` — 14 Sep 2026
- `Lead Product Designer … Remote, PST Time Zone` — 12 Sep 2026

`title_location_excluded()` checks titles for US/UK timezone tokens (`pst`,
`est`, `cst`, `mst` …), country tokens (`us`, `uk`, `canada`, `americas`,
`latam`, `apac` …), and a **deliberately short** city list — New York, NYC, San
Francisco, London, Toronto. Short because each entry risks costing a real role
and volume is already thin. Easy to extend.

It is hard-exclude only. It is **not** `location_ok()`, which requires a
positive signal ("remote", "europe") — most titles have none, so that would
reject nearly everything.

### Location — normalised before matching

`_norm_loc()` flattens every separator (`- – — · • | / \ ( ) , ; : [ ] { }`) to
a single space on both sides of the comparison. This exists because
`Remote- UK` reached the 4 Sep 2026 digest — only the *spaced* hyphen was
listed. Rules are now about places, not punctuation.

**Pass if it contains:** `remote` · `spain` · `barcelona` · `europe` · `eu` ·
`worldwide` · `anywhere` · `global` · `emea`

**Hard excluded:** US, Canada, North America, UK — in all separator variants,
plus standalone tokens `usa` `us` `uk` `gb` `canada` `britain` `england`.

**`location_country_ok()` runs inside `location_ok()`** — so all ten sources get
it. It used to be called by only three, which is why `Remote, PL` sailed
through. It strips filler and reads the country on either side, so `Remote US`,
`US Remote` and `Remote (US)` all resolve the same.

### Single-country European roles — `COUNTRY_RESTRICTED_MODE`

`country_restriction()` catches roles that are EU-remote but need residency in
one country: `Remote, Poland` · `Remote, PL` · `Portugal Remote` ·
`Germany (Remote)`. All four reached real digests.

**Currently `"drop"` — set 18 September 2026.** Flagging was the right first
step: these roles had been vanishing silently and needed to be seen before being
judged. They were. On 18 Sep four of six roles were single-country ones Flor
cannot take, three of them the *same* EverAI job posted once each for Italy,
Germany and France. The badge had done its job; what remained was noise.

Set back to `"flag"` for a week if the digest ever looks too thin — the badge
and the header pill are still wired up and will simply start appearing again.

Never fires when the location names Spain, or a broad region (EMEA, Europe,
International). ISO-2 codes are matched only where unambiguous — `de`, `fr`,
`it`, `be`, `es`, `ca` are deliberately excluded so "Palma de Mallorca" does not
read as Germany.

### Description signals (hard exclude)

`401(k)` · `401k` · `medical, dental, and vision` · `espp` ·
`must be authorized to work in the us` · `base salary range: $` · `ote: $` …

Only applies where a source gives a description — the HTML scrapers have none.

### Company blocklist

`US_COMPANY_BLOCKLIST` matches against the company name **and the posting URL**.
The URL half was added 14 Sep 2026: "rippling" had been on the list all along,
but a role posted *through* `ats.rippling.com` carries the client's name, so the
check never fired. Two roles got through that way (21 Aug, 14 Sep).

Current: logicgate · twilio · gusto · rippling · brex · deel · lattice · retool ·
loom · figma · mercury · ramp · ziprecruiter · samsara · ocrolus · thumbtack ·
owner.com · deepgram · pinterest · clickup · happyco · vercel

---

## Email features

**The company line.** It reads `**Company** · Location`, company bold and dark,
location lighter. Both halves used to be styled identically, which is unreadable
when the company is named after a place: on 18 Sep 2026 remote.com hiring for
EMEA rendered as "Remote · EMEA", which looks like the location field saying
"Remote" and then contradicting itself. Nothing was wrong with the data. A name
in `AMBIGUOUS_COMPANY_NAMES` is now also spelled out — "Remote (the company)".

| Badge | Meaning |
|---|---|
| 🟢 4-day week | Confirmed 4-day week company |
| ⚠️ Verify location | Spain/Barcelona without a remote signal |
| 📍 `<Country>` only | Remote, but needs residency in that country |
| ❓ Location not stated | The listing gave no location — check the posting |
| 🇺🇸 USD / 🇬🇧 GBP | Salary currency — soft flag |
| 🔄 Reposted | Resurfaced after 14+ days |
| 🔭 Stretch | Staff/Principal/Head roles |
| ⭐📌🔍 Watchlist | Tier badge |
| 🔴 Health alert | See below |

**Sorting:** 4-day week → top · country-restricted and Spain-flagged → bottom.

**Age colours:** 🟢 ≤3 days · 🟡 4–10 days · ⚫ older/unknown. Jobs older than
`MAX_JOB_AGE_DAYS` (21) are hard-dropped before the email.

---

## Health checks

Three independent tracks in `source_health.json`.

**1. `error_streak`** — fetch exceptions. Alerts after `ERROR_ALERT_DAYS` (3).

**2. `zero_result_streak`** — fetch succeeds, nothing matches. Alerts after
`ZERO_RESULT_ALERT_DAYS` (5).

**3. Watchlist per-company `dead_streak`** — see above, 14 days.

### The alert says which problem it is — since 14 Sep 2026

Reading nothing and matching nothing are completely different, and used to
produce identical text. Three sources alerted for over a week with no way to act
on them. `SOURCE_RAW_COUNTS` records how many listings each source handed over
**before** filtering — a number the code already computed and threw away.

- `raw == 0` → **BROKEN.** For an API: the endpoint or its response shape
  changed. For a scraper: the site's HTML changed and the selectors miss.
- `raw > 0` → **HEALTHY.** *N* listings read, none matched. Nothing to fix
  unless a match was expected.

The wording also branches by source type, so an API is never told its CSS
selectors are stale. Arbeitnow spent weeks being told exactly that.

---

## Deduplication

- Key: `MD5(title.lower() + company.lower())`
- Auto-prunes entries not seen in `PRUNE_DAYS` (30)
- New → email. Reappears after `REPOST_DAYS` (14) → "Reposted". Within 14 days →
  silently skipped.

⚠️ **Do not add location to the key.** It would fix a rare case — two openings
with the same title at one company collapsing into one — and break cross-source
de-duplication for every job, every day. Considered and rejected 14 Sep 2026.

---

## Files

- `scraper.py` — everything
- `test_location_filter.py` — 102 checks, built from strings that actually
  appeared in digests between 21 Aug and 14 Sep 2026. Run: `python3
  test_location_filter.py`
- `requirements.txt`
- `.github/workflows/daily-digest.yml` — the twice-daily schedule
- `.github/workflows/tests.yml` — runs the checks on every push that touches
  `scraper.py` or the test file. Deliberately **separate** from the digest
  workflow, so a failing test can never stop the email going out.
- `seen_jobs.json`, `source_health.json` — auto-generated, cached

---

## Common tasks

**Add a watchlist company:** add to `WATCHLIST` with `name`, `url`, `ats`,
`tier`. Prefer ashby/lever/greenhouse over html.

**Add to the blocklist:** add a lowercase string to `US_COMPANY_BLOCKLIST` — it
matches the company name and the URL.

**Change filters:** the keyword lists at the top of `scraper.py`. **Add a test
case to `test_location_filter.py` in the same commit.**

**Debug a silent source:** read the alert — it now says BROKEN or HEALTHY. If
BROKEN, the selectors or the endpoint changed. If a watchlist company, try
`jobs.ashbyhq.com/{slug}`; most EU design startups have moved to Ashby.

**Stop seeing single-country roles:** `COUNTRY_RESTRICTED_MODE = "drop"`.

**Reset dedup:** GitHub → Actions → Caches → delete `scraper-state-*`.

---

## Key decisions

- **North America hard-excluded** — "Remote - North America" was passing on the
  word "remote".
- **Head of Product Design → stretch; VP/Director/CDO excluded.** See the open
  question above.
- **4DayWeek: empty `remote_allowed` + USD = drop** — currency as a proxy.
- **Staleness cutoff at 21 days** — safety net for date-parsing failures.
- **Prefer ATS over HTML for watchlist** — HTML scrapes marketing pages.
- **Single-country roles are flagged, not dropped** — nothing disappears without
  Flor seeing it. One constant flips it.
- **Watchlist location filter passes an empty location** — a missing field is
  not evidence, so it is badged rather than dropped.
- **LinkedIn automation not attempted** — ToS risk. Use LinkedIn's own alerts.
- **Claude Routines not used** — evaluated July 2026; GitHub Actions is free and
  more controllable.
- **designleaderjobs.com not added** — Director/VP level, mostly US.

---

## Still open

1. **`DO NOT APPLY` list does not exist.** Glovo, Fever, Factorial and Bitpanda
   can still surface. `US_COMPANY_BLOCKLIST` is a different thing.
2. **No `ALREADY APPLIED` flag.** Should flag, not exclude — the tracker is the
   source, matched as substrings (`OLX (1st, cold apply)`, `Perk` = TravelPerk).
3. **Cabify not on the watchlist.**
4. **Funded Drop** — replace a source, supplement, or drop.
5. **The Staff / Head-of contradiction** above.
