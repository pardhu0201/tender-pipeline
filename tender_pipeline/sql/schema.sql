-- Schema for the tender pipeline. Idempotent: safe to run on every deploy.

-- One row per pipeline run: what it did, how long it took, and how clean the data was.
CREATE TABLE IF NOT EXISTS scrape_runs (
    id             bigserial PRIMARY KEY,
    source         text        NOT NULL,
    mode           text        NOT NULL CHECK (mode IN ('full', 'incremental')),
    status         text        NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'success', 'degraded', 'failed', 'abandoned')),
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    pages_fetched  integer     NOT NULL DEFAULT 0,
    records_seen   integer     NOT NULL DEFAULT 0,
    inserted       integer     NOT NULL DEFAULT 0,
    updated        integer     NOT NULL DEFAULT 0,
    unchanged      integer     NOT NULL DEFAULT 0,
    rejected       integer     NOT NULL DEFAULT 0,
    source_total   integer,                       -- record count GeM reported on page 1
    quality        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    error          text
);

CREATE INDEX IF NOT EXISTS scrape_runs_source_started_idx
    ON scrape_runs (source, started_at DESC);


-- One row per tender, keyed by the source's own ID so re-crawls are idempotent.
CREATE TABLE IF NOT EXISTS tenders (
    id                 bigserial   PRIMARY KEY,
    source             text        NOT NULL,
    source_id          text        NOT NULL,
    bid_number         text        NOT NULL,
    bid_type           text        NOT NULL,
    parent_bid_number  text,
    items              text,
    items_detail       text,
    quantity           bigint      CHECK (quantity >= 0),
    ministry           text,
    department         text,
    state              text,
    start_at           timestamptz NOT NULL,
    end_at             timestamptz NOT NULL,
    is_high_value      boolean     NOT NULL DEFAULT false,
    is_single_packet   boolean     NOT NULL DEFAULT false,
    is_global_tender   boolean     NOT NULL DEFAULT false,
    category_ids       text[]      NOT NULL DEFAULT '{}',
    document_url       text        NOT NULL,

    -- The cleaned source record, kept so every column above can be re-derived
    -- after a parser fix (`reprocess`) without re-crawling.
    raw                jsonb       NOT NULL,
    content_hash       text        NOT NULL,      -- sha256 of raw: detects real changes

    first_seen_at      timestamptz NOT NULL DEFAULT now(),
    last_seen_at       timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    last_run_id        bigint      REFERENCES scrape_runs (id),

    -- Full-text search over what is being bought and by whom.
    search_vector      tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(items_detail, items, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(ministry, '') || ' ' ||
                                         coalesce(department, '') || ' ' ||
                                         coalesce(state, '')), 'B')
    ) STORED,

    CONSTRAINT tenders_source_key UNIQUE (source, source_id),
    CONSTRAINT tenders_window_check CHECK (end_at >= start_at)
)
-- Every crawl bumps last_seen_at on every row it sees. Leaving 10% of each page
-- free lets those updates stay on the same page as HOT updates, which skip
-- index maintenance entirely (none of the indexed columns change).
WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS tenders_end_at_idx     ON tenders (end_at);
CREATE INDEX IF NOT EXISTS tenders_start_at_idx   ON tenders (start_at DESC);
CREATE INDEX IF NOT EXISTS tenders_ministry_idx   ON tenders (ministry, department);
CREATE INDEX IF NOT EXISTS tenders_state_idx      ON tenders (state) WHERE state IS NOT NULL;
CREATE INDEX IF NOT EXISTS tenders_bid_number_idx ON tenders (bid_number);
CREATE INDEX IF NOT EXISTS tenders_search_idx     ON tenders USING gin (search_vector);


-- Quarantine: records that failed validation, with the reason and the payload.
CREATE TABLE IF NOT EXISTS rejected_records (
    id           bigserial   PRIMARY KEY,
    run_id       bigint      NOT NULL REFERENCES scrape_runs (id) ON DELETE CASCADE,
    source       text        NOT NULL,
    source_id    text,
    reason       text        NOT NULL,
    detail       text,
    raw          jsonb       NOT NULL,
    rejected_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS rejected_records_run_idx ON rejected_records (run_id);


-- Convenience view: bids that are still open, with IST display times.
CREATE OR REPLACE VIEW open_tenders AS
SELECT id, source, bid_number, bid_type, items, quantity,
       ministry, department, state, start_at, end_at,
       (end_at AT TIME ZONE 'Asia/Kolkata') AS end_at_ist,
       is_high_value, document_url
FROM tenders
WHERE end_at > now();
