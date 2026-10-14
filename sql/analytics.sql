-- Example analytics over the pipeline's tables.
-- Run any block with:  psql "$DATABASE_URL" -f sql/analytics.sql

-- 1. Open bids per central ministry, with each ministry's share (window over the grouped rows).
SELECT ministry,
       count(*)                                            AS open_bids,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)  AS pct_of_central
FROM tenders
WHERE end_at > now() AND ministry IS NOT NULL
GROUP BY ministry
ORDER BY open_bids DESC
LIMIT 15;

-- 2. The three busiest departments inside each ministry (rank within a partition).
WITH per_department AS (
    SELECT ministry, department, count(*) AS open_bids,
           rank() OVER (PARTITION BY ministry ORDER BY count(*) DESC) AS rank_in_ministry
    FROM tenders
    WHERE end_at > now() AND ministry IS NOT NULL AND department IS NOT NULL
    GROUP BY ministry, department
)
SELECT ministry, department, open_bids
FROM per_department
WHERE rank_in_ministry <= 3
ORDER BY ministry, rank_in_ministry;

-- 3. New bids published per day (IST) with a 7-day moving average.
WITH daily AS (
    SELECT (start_at AT TIME ZONE 'Asia/Kolkata')::date AS day, count(*) AS new_bids
    FROM tenders
    WHERE bid_type = 'bid'
    GROUP BY 1
)
SELECT day, new_bids,
       round(avg(new_bids) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1)
           AS moving_avg_7d
FROM daily
ORDER BY day DESC
LIMIT 30;

-- 4. Closing in the next 48 hours, in Indian time.
SELECT bid_number, left(items, 60) AS items,
       coalesce(ministry, state, department) AS buyer,
       to_char(end_at AT TIME ZONE 'Asia/Kolkata', 'DD Mon HH24:MI') AS closes_ist
FROM tenders
WHERE end_at > now() AND end_at <= now() + interval '48 hours'
ORDER BY end_at;

-- 5. Full-text search, ranked: "who is buying laptops right now?"
SELECT bid_number, left(items_detail, 70) AS items, coalesce(ministry, state) AS buyer,
       round(ts_rank(search_vector, query)::numeric, 3) AS score
FROM tenders, websearch_to_tsquery('english', 'laptop') AS query
WHERE search_vector @@ query AND end_at > now()
ORDER BY score DESC, end_at
LIMIT 20;

-- 6. Most common product categories among open bids (unnest an array column).
SELECT category, count(*) AS open_bids
FROM tenders, unnest(category_ids) AS category
WHERE end_at > now()
GROUP BY category
ORDER BY open_bids DESC
LIMIT 15;

-- 7. Bids that vanished from the listing before their deadline (likely cancelled):
--    still "open" by date, but not seen by the latest complete full crawl.
WITH last_full AS (
    SELECT max(started_at) AS started_at
    FROM scrape_runs
    WHERE mode = 'full' AND status IN ('success', 'degraded')
      AND (quality ->> 'complete')::boolean
)
SELECT t.bid_number, left(t.items, 60) AS items, t.end_at, t.last_seen_at
FROM tenders t, last_full
WHERE t.end_at > now() AND t.last_seen_at < last_full.started_at
ORDER BY t.last_seen_at;

-- 8. Pipeline health: recent runs, throughput and data quality at a glance.
SELECT id, mode, status,
       to_char(started_at AT TIME ZONE 'Asia/Kolkata', 'DD Mon HH24:MI') AS started_ist,
       round(extract(epoch FROM finished_at - started_at) / 60, 1)        AS minutes,
       records_seen, inserted, updated, rejected,
       quality ->> 'reject_rate'          AS reject_rate,
       quality ->> 'duplicates_in_run'    AS duplicates,
       quality ->> 'coverage'             AS coverage,
       quality -> 'http' ->> 'requests'   AS requests,
       quality -> 'http' ->> 'retries'    AS retries,
       quality -> 'http' ->> 'slowdowns'  AS slowdowns
FROM scrape_runs
ORDER BY started_at DESC
LIMIT 20;
