# Tender Pipeline

[![CI](https://github.com/pardhu0201/tender-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/pardhu0201/tender-pipeline/actions/workflows/ci.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue)
![PostgreSQL + pgvector](https://img.shields.io/badge/postgresql-16%20%2B%20pgvector-336791)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Collects Indian government tenders from **GeM (Government e-Marketplace)** and
makes them searchable. Two parts, one database:

1. **`tender_pipeline`**: a fault-tolerant ingestion pipeline into PostgreSQL.
   Incremental and full crawls, idempotent upserts, data-quality gates, a
   quarantine for bad records, and replay of every stored record through a
   fixed parser without re-crawling.
2. **`tender_search`**: hybrid semantic + keyword search over those tenders.
   ONNX sentence embeddings in pgvector, fused with Postgres full-text search,
   served by a FastAPI app with a search page and "similar tenders".
   [Jump to it](#semantic-search-tender_search).

GeM is India's national public-procurement portal; at the time of writing
(October 2026) its listing carried **~45,600 open bids**. GeM publishes no API,
so this project reverse-engineers the JSON endpoint behind its public listing page.

```mermaid
flowchart LR
    subgraph GeM["bidplus.gem.gov.in"]
        R["robots.txt"]
        P["/all-bids page<br/>session cookie + CSRF token"]
        A["/all-bids-data<br/>JSON, 10 bids per page"]
    end
    subgraph Pipeline["tender_pipeline"]
        C["gem_client<br/>throttle, retries, re-login"]
        N["normalize<br/>IST fix, cleaning, validation"]
        Q["quality<br/>null rates, reject rate, coverage"]
        D["db<br/>hash-based idempotent upsert"]
    end
    subgraph PG["PostgreSQL"]
        T[("tenders<br/>raw jsonb + tsvector")]
        X[("rejected_records")]
        S[("scrape_runs")]
    end
    R --> C
    P --> C
    A --> C
    C --> N
    N -- valid --> D --> T
    N -- invalid --> X
    N --> Q --> S
```

## Quick start

Needs Docker (or Python 3.12 and PostgreSQL 16 with the pgvector extension;
the pipeline alone runs on any PostgreSQL 13+).

```bash
docker compose up -d db                                         # Postgres on 127.0.0.1:5433
docker compose run --rm pipeline init-db                        # create the schema
docker compose run --rm pipeline run --mode full --max-pages 5  # try it: 50 tenders
docker compose run --rm pipeline report                         # what did we get?
docker compose run --rm api init-db                             # then make it searchable:
docker compose run --rm api embed
docker compose up -d api                                        # open http://127.0.0.1:8000
```

A complete crawl is `run --mode full` (about 1.5 hours at a polite one request
per second). After that, `run` (incremental by default) fetches only what is new.

Without Docker:

```bash
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt   # Windows: .venv\Scripts\
cp .env.example .env            # then point DATABASE_URL at your Postgres
python -m tender_pipeline init-db
python -m tender_pipeline run --mode full --max-pages 5
```

| Command | What it does |
|---|---|
| `init-db` | Create or update the schema (idempotent; safe on every deploy). |
| `run [--mode incremental\|full] [--max-pages N]` | Crawl and upsert. Incremental is the default; the first ever run is full. |
| `status [--max-age-hours H]` | Recent runs and why any degraded; with `--max-age-hours`, exits 1 and alerts when data is stale. |
| `report` | Open bids by ministry and state, and what closes soonest. |
| `export [--format jsonl\|csv] [--open-only] [--output FILE]` | Write a clean dataset for analytics or ML, streamed in constant memory. |
| `reprocess` | Re-derive every column from stored raw records after a parser fix. |

Sample `report` output from a short live run:

```
Open bids by state
  state          open bids
  -------------  ---------
  Gujarat        2
  Uttar Pradesh  2
  Chhattisgarh   1
  Odisha         1

Closing soonest
  bid number         items                                             buyer                closes
  -----------------  ------------------------------------------------  -------------------  ----------------
  GEM/2026/R/752126  Glow Sign Board & Iron Flax Boards                Ministry of Defence  15 Oct 19:00 IST
  GEM/2026/R/752127  Television (V3),data wall display                 Odisha               15 Oct 19:00 IST
  GEM/2026/R/752133  AIR COMPRESSOR, SCREW, 7.5KW, TANK MOUNTED        Ministry of Steel    15 Oct 19:00 IST
```

## How it works

### Acquiring the data: replaying the page's own API call

The listing page at `bidplus.gem.gov.in/all-bids` renders nothing server-side;
its JavaScript POSTs to `/all-bids-data`. Reading that script showed the
contract:

1. `GET /all-bids` sets a session cookie and embeds a CSRF token
   (`csrf_bd_gem_nk`) in the page's JavaScript.
2. `POST /all-bids-data` with that cookie, the token, and a form field
   `payload` containing a JSON query (page, filters, sort). It returns a
   Solr-style response: 10 records per page, every value wrapped in a
   one-element list.

Calling the JSON endpoint directly is far cheaper and more robust than driving
a browser with Selenium: no rendering, no DOM selectors that break when the
page is restyled, and the payload is already structured.

The client ([gem_client.py](tender_pipeline/gem_client.py)) is built to survive a
long crawl against a government server:

- **Polite by construction.** At most one request per second (configurable, but
  the code refuses anything under 0.5 s), robots.txt is checked before the first
  request and a `Crawl-delay` is honoured, and the User-Agent identifies the
  project honestly. If GeM answers 429 or 503, the client halves its request
  rate for the rest of the run (down to one request every 30 s).
- **Retries that make sense.** Timeouts, connection resets, 429 and 5xx are
  retried with exponential backoff plus jitter, and `Retry-After` is respected.
  Other 4xx are not retried, because repeating a wrong request does not help.
- **Session expiry.** A full crawl outlives a session. If the endpoint answers
  with HTML instead of JSON, or with a non-200 `code`, the client throws the
  session away, fetches a fresh page and token, and retries.
- **Fail loudly on change.** If the response is JSON but no longer has the
  expected shape, the run fails immediately (`GemSchemaError`) rather than
  quietly writing nothing.

### Incremental vs full crawls

`run --mode incremental` reads the listing newest-first and stops after three
consecutive pages in which every bid started before *(the last complete run's
start − 6 h overlap)*. It fetches only the recent end of the listing instead of
all ~4,600 pages. Only a run that read its whole range can be that baseline: a
try-out capped with `--max-pages` never is, so the next run falls back to full.

The sort order is deliberate. Offset pagination over a live listing shifts as
you read it. Newest-first means newly published bids push records *down*, which
shows up as a harmless duplicate (the upsert is idempotent), never as a record
silently skipped. A weekly `--mode full` crawl refreshes every open bid (for
example, deadlines extended by corrigenda) and measures **coverage**: unique
records seen ÷ the total GeM reports.

### Storage: idempotent, change-aware, replayable

- `tenders` is keyed on `(source, source_id)`. Each record carries a SHA-256
  `content_hash` of its cleaned raw payload. Before writing a page, the pipeline
  fetches the existing hashes in one query and then
  - inserts new records,
  - rewrites only records whose hash changed, and
  - for unchanged records, only bumps `last_seen_at`. No payload is re-sent,
    `updated_at` keeps meaning "really changed", and because no indexed column
    changes, Postgres can apply these as HOT updates that skip index
    maintenance. The table's `fillfactor = 90` leaves room on each page for
    exactly that.
- **Every page is its own transaction**, so a crash loses at most one page, and
  re-running simply continues.
- The cleaned source record is kept in `raw jsonb`. Every other column is a pure
  function of it, so after fixing a parsing rule, `reprocess` re-derives every
  row (keyset-paginated, never `OFFSET`) without sending a single request to GeM.
- A generated `tsvector` column with a GIN index gives ranked full-text search
  over items, ministries, departments and states.
- `scrape_runs` records every run: mode, status, timings, counts, and a
  `quality` JSON that includes what the crawl cost the server (requests,
  retries, session refreshes, slowdowns).

The main columns of `tenders`:

| Column | Meaning |
|---|---|
| `bid_number` | GeM's public ID, e.g. `GEM/2026/B/8041403` (`/R/` for reverse auctions) |
| `bid_type` | `bid`, `reverse_auction` or `direct_reverse_auction` |
| `items`, `items_detail` | What is being bought: short (cut at ~100 chars by GeM) and long form |
| `quantity` | Total quantity across items |
| `ministry`, `department`, `state` | The buyer; `state` is derived for state-government buyers |
| `start_at`, `end_at` | Bid window as true instants (`timestamptz`), corrected from GeM's mislabelled IST |
| `document_url` | Link to the bid document PDF on GeM |
| `raw`, `content_hash` | The cleaned source record and its SHA-256, for change detection and replay |
| `first_seen_at`, `last_seen_at`, `updated_at` | When the pipeline first saw it, last saw it, and last saw it change |

See [schema.sql](tender_pipeline/sql/schema.sql) and the example queries in
[analytics.sql](sql/analytics.sql): window functions, CTEs, ranked search,
`unnest` over category arrays, and finding bids that vanished before their
deadline. `export` streams the table out as JSON Lines or CSV through a
server-side cursor, 2,000 rows at a time, so memory stays flat at any size.

### Operating it safely

- **One run at a time, wherever it starts.** Each run takes a Postgres
  advisory lock for its source. A second run, whether from cron, a terminal or
  another container, is refused before it touches anything. The lock belongs to
  the database session, so it is released even if the process is killed.
- **Exact crash detection.** While a run holds the lock, any other run still
  marked `running` must belong to a dead process, so it is marked `abandoned`
  immediately. No "older than N hours" guesswork.
- **Clean shutdown.** `docker stop` and systemd send SIGTERM. The pipeline turns
  it into an orderly stop: the current page is rolled back, the run is recorded
  as `terminated by SIGTERM`, and the process exits with status 143.

### Data quality

Each record passes through [normalize.py](tender_pipeline/normalize.py). Records
that fail validation (missing ID, malformed bid number, unparseable dates, end
before start, negative quantity) go to `rejected_records` with a reason code and
the payload, instead of crashing the run or polluting the table.

Each run computes null rates per field, a reject rate, in-run duplicates and
(for complete full crawls) coverage. A run is marked **degraded** and an alert
is sent when the reject rate exceeds 5% or coverage falls below 95%. Its data
is still committed, because a human should look at it rather than lose it.
Hard failures mark the run **failed** and alert too.

## What the data got wrong, and how the pipeline handles it

Found while building this, from evidence rather than assumption:

1. **The timestamps lie about their timezone.** GeM sends
   `"2026-10-13T20:00:00Z"`, but the `Z` is not UTC: these are Indian wall-clock
   times. The evidence: at 19:04 IST, the earliest-ending *open* bids were
   stamped `T20:00:00Z`. If that meant UTC (01:30 IST the next day), bids stamped
   `T15:00:00Z` (20:30 IST, still open) would have come first, and none
   existed. The site's own JavaScript also renders these values with UTC
   getters to show local time. Read naively, every deadline would be 5.5 hours
   late. The parser treats a trailing `Z` as IST and honours any real offset.
2. **Placeholders posing as data.** Departments of `"NA"` become NULL instead of
   a fake department called "NA", and stray whitespace is collapsed.
3. **State-government buyers have no ministry.** Between a fifth and a quarter
   of records in samples. That is not missing data: the state is the suffix of
   the department name ("Energy Department Uttar Pradesh"), so a `state` column
   is derived. It knows the 36 states and UTs plus old names such as Orissa.
4. **Silent truncation.** Item names arrive cut off mid-word at about 100
   characters, and the detail field at 1,024. Search uses the longer field.
5. **Ties make pagination unstable.** Many reverse auctions share an identical
   start time, and the endpoint offers no secondary sort key, so a tied record
   can move between pages mid-crawl. The first live run saw 30 rows but 29
   unique records. The overlap window, in-run duplicate counting and full-crawl
   coverage check exist for exactly this.
6. **Personal data minimisation.** Each record includes the GeM account ID of
   the official who created it. Nothing needs it, so it is dropped before
   storage.

## Semantic search (`tender_search`)

The second part of the project turns the collected tenders into a search
product. Ask in plain language and get the matching open bids, even when GeM
words them differently:

| Query | Top result | Found by |
|---|---|---|
| `fire safety equipment` | "FIRE **SAFTY** EQUIPMENTS" (GeM's typo) | semantic |
| `IS 15683` (a standard number) | "Portable Fire Extinguishers (V3) ISI Marked to IS 15683" | semantic + keyword |
| `medical oxygen cylinders` | "Medical Oxygen Gas Cylinders - Steel (Normalized) (V2)" | semantic + keyword |

Every result says which retriever found it, so the value of each is visible:
keyword search can never match a typo, and embeddings blur exact identifiers.

```mermaid
flowchart LR
    T[("tenders")] -- "new or changed rows" --> E["embed<br/>bge-small-en-v1.5 (ONNX)"]
    E --> V[("tender_embeddings<br/>vector(384), HNSW index")]
    Q["query"] --> S["semantic<br/>cosine nearest neighbours"]
    Q --> K["keyword<br/>Postgres full-text search"]
    V --> S
    T --> K
    S --> F["Reciprocal Rank Fusion"]
    K --> F
    F --> A["FastAPI<br/>/search, /similar, search page"]
```

- **Embeddings.** Each tender's "what is being bought, and by whom" is embedded
  with `BAAI/bge-small-en-v1.5` (384 dimensions) through `fastembed`, which runs
  the model on ONNX Runtime. No PyTorch: the API container uses about 250 MB of
  RAM, small enough for a free hosting tier.
- **Incremental, like the crawler.** Each embedding stores the content hash of
  the tender it came from, so `embed` touches only new or changed tenders (or
  everything, if the model changes). `deploy/run.sh` runs it after every crawl.
- **Hybrid retrieval.** pgvector finds the 50 nearest tenders by cosine
  distance (HNSW index), Postgres full-text search finds the 50 best keyword
  matches (GIN index), and **Reciprocal Rank Fusion** merges them by rank:
  `score = Σ 1 / (60 + rank)`. Ranks need no calibration between two very
  different scoring scales, and a tender both retrievers like rises furthest.
- **Filters that don't starve the index.** State, ministry, open-only and
  "closing within N hours" filters run inside both queries. pgvector 0.8's
  iterative HNSW scan keeps walking the graph until enough rows pass the
  filters, instead of filtering a fixed candidate set down to nothing.
- **"Similar tenders".** Any tender's own embedding finds its nearest
  neighbours: the "who else is buying this?" question a bidder asks.

Two measured problems shaped the indexer:

- **Padding blew up memory.** Embedding 256 real tenders per model call took
  6 GB of RAM: a transformer pads every text in a batch to the longest, and
  attention memory grows with batch × length². Model calls now take 32 texts,
  sorted by length so each batch pads to similar sizes: 277 MB.
- **Repetition in the source.** 27% of all item text is the same item listed
  again and again in one bid. Exact repeated segments are dropped before
  embedding: they add nothing to the meaning.

Measured on a 16-thread laptop CPU with 17,533 real tenders:

| | |
|---|---|
| Embedding throughput | 28 tenders/s (17,533 in 10.6 min, one-off; later runs embed only what's new) |
| Search, new query (embedding included) | p50 14 ms, p95 18 ms |
| Search, repeated query (cached embedding) | p50 7 ms, p95 9 ms |
| SQL alone | semantic 8 ms (HNSW index scan), keyword 0.2 ms (GIN index) |
| API container | ~250 MB RAM; image 663 MB with the model baked in |

```bash
docker compose run --rm api init-db       # enable pgvector, create the embeddings table
docker compose run --rm api embed         # embed new and changed tenders
docker compose up -d api                  # search page and API on http://127.0.0.1:8000
docker compose run --rm api search "laptops for schools" --state "Uttar Pradesh"
docker compose run --rm api bench         # latency: new vs repeated queries
```

| Endpoint | What it returns |
|---|---|
| `GET /search?q=&state=&ministry=&open_only=&closing_within_hours=&limit=` | Ranked tenders, with `matched_by` and timing |
| `GET /similar?bid=GEM/2026/B/...` | The tenders closest in meaning to that one |
| `GET /facets` | Open-bid counts per state and ministry |
| `GET /health` | Tender and embedding counts, model name |
| `GET /docs` | Interactive OpenAPI documentation |

If the text recipe in `tender_search/indexer.py` changes, run `embed --rebuild`:
content hashes only track changes in the source data.

## Deploying on a Linux server

```bash
git clone https://github.com/pardhu0201/tender-pipeline /opt/tender-pipeline
cd /opt/tender-pipeline
echo "POSTGRES_PASSWORD=$(openssl rand -hex 16)" > .env       # plus ALERT_WEBHOOK_URL=...
docker compose up -d db
docker compose run --rm pipeline init-db
docker compose run --rm api init-db
bash deploy/smoke.sh                                          # can we reach GeM, and is it unchanged?
docker compose up -d api                                      # search on 127.0.0.1:8000
crontab deploy/crontab.example
```

[deploy/run.sh](deploy/run.sh) wraps each job in `flock`, so a slow full crawl
and the next incremental slot don't even start a second container (the
advisory lock covers everything else). The crontab runs incremental crawls
every 6 hours, a full crawl weekly at a quiet hour, and an hourly
`status --max-age-hours 13` that alerts if data stops arriving. Alerts go to
any Slack-compatible webhook (for Discord, append `/slack` to its webhook URL).
Postgres is published on `127.0.0.1` only.

**Host it in India.** GeM refuses connections from outside India: from
GitHub's hosted runners every request fails with `connection refused`, while
the same requests work from an Indian network (verified October 2026). Use a
machine or VPS in India.

GeM's API is undocumented and can change without notice, so check it before
relying on a deploy: [deploy/smoke.sh](deploy/smoke.sh) crawls two live pages
into the scratch database and fails loudly if the client no longer understands
them. The **Live smoke test** workflow does the same from the Actions tab, on a
self-hosted runner in India.

## Tests

```bash
pytest                                    # offline unit tests
TEST_DATABASE_URL=postgresql://tender:tender@127.0.0.1:5433/tenders_test pytest   # + PostgreSQL
```

169 tests with 98% line coverage. CI runs ruff (lint and formatting), mypy,
and the whole suite against Postgres with pgvector, fails if coverage drops
below 95%, and builds both Docker images.

The search tests use a deterministic hashing embedder (texts that share words
land close together), so ranking, filters, fusion, the API and incremental
re-embedding are all tested without downloading a model. The real ONNX
embedder's own logic (length-sorted batching, the query cache) is tested
against a stand-in model object.

The pipeline's unit tests run against real GeM responses saved as fixtures (12 records
chosen to cover every edge case above), with a fake HTTP session and a fake
clock. That covers retries, backoff, `Retry-After`, adaptive slowdown, session
refresh, robots.txt and throttling deterministically and without network
access. The 30 database tests run the real SQL against PostgreSQL. They cover
idempotent re-runs, change detection, quarantine and quality gates, the
incremental stop rule, recovery after a mid-crawl failure or SIGTERM, the
concurrency lock, reprocessing, export, and every CLI command.

The database tests drop and recreate tables, so they only run when
`TEST_DATABASE_URL` is set explicitly. They never touch `DATABASE_URL`.

## Responsible use

This collects data the government publishes for public access, one request at
a time and never more than one per second. It checks robots.txt on every run
and stops if the listing is disallowed, and it identifies itself in its
User-Agent. It does not log in, solve CAPTCHAs, or circumvent access controls.
If you fork it, keep it that way.

## Limitations and next steps

- One source so far. The structure (source-tagged rows, a pure normalizer per
  source) is meant for adding the NIC/CPPP eProcurement portals next.
- The listing has no tender value or delivery location; those live in each
  bid's PDF document (`document_url`), a natural next extraction step.
- Bid results and awards (GeM's "Bid/RA Status" view) would enable win-rate
  and pricing analytics.
- Partitioning `tenders` by month makes sense past tens of millions of rows. At
  ~1–2 million rows a year, indexes are enough, and partitioning would force the
  partition key into the unique constraint.
- Semantic search with pgvector embeddings over `items_detail`.

## License

MIT
