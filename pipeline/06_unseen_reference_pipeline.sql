-- ============================================================================
-- Unseen-day (surprise) reference pipeline — DuckDB, run locally, no ClickHouse.
-- Mirrors the CURRENT deployed cnt_a/cnt_b model (04_ddl_annotated.sql), not
-- the older FG/ENG dual-definition model in 01_reference_pipeline.sql.
--
-- gap_ms = 60000 (1 minute): TAKEN DIRECTLY FROM THE SPEC, not derived or
-- assumed. dataset_details.md (event_type column, mirrored verbatim in
-- unseen_data/spec.md) states: "The heartbeat event type is a periodic
-- event which is currently passed every 1 minute." Read literally: if no
-- heartbeat (or any other event) has arrived within 1 minute of the last
-- one, the session is INACTIVE from that point until the next event
-- arrives -- that's exactly what the gap-inference logic below implements.
--
-- Cross-check only (not the basis for the value): the actual dominant
-- heartbeat spacing in the raw data is ~40s in BOTH datasets, and
-- P(gap contains a real labelled AppBackgrounded) jumps from ~0.01-0.16%
-- to 40-60%+ immediately after 41s in both. 60s sits safely inside that
-- high-probability zone -- it's a more conservative choice than the
-- tightest defensible one (~41s), but the spec's literal wording is
-- authoritative here, not the empirical minimum. Peak concurrency is not
-- very sensitive to the exact choice in this range: 50s->60s changes peak
-- cnt_a by well under 1% on both the original and unseen datasets.
-- ============================================================================
SET VARIABLE gap_ms = 60000;

-- ---- SILVER 1: typed, platform-normalized, joined to content ----
CREATE OR REPLACE TABLE silver_events AS
SELECT
    b.video_session_id AS session_id,
    b.user_id,
    b.content_id::BIGINT AS content_id,
    b.event_timestamp AS ts_ms,
    b.session_start_epoch AS session_start_ms,
    upper(trim(b.platform)) AS platform,   -- fix: Mweb/MWEB, Web/WEB duplicates
    b.country,
    coalesce(nullif(c.video_type,''),'unk') AS video_type,
    coalesce(nullif(c.show_name,''),'unk')  AS show_name,
    b.event, b.event_type,
    CASE WHEN b.event_type='VideoSessionStart' THEN 1
         WHEN b.event_type='VideoSessionEnd'   THEN -1
         WHEN b.event_type='AppBackgrounded'   THEN 0
         WHEN b.event_type='AppForegrounded'   THEN 1
         WHEN b.event='Play'                  THEN 1
         ELSE NULL END AS st
FROM bronze_events_raw b
LEFT JOIN bronze_content_raw c ON c.content_id = b.content_id;

-- ---- SILVER 2: session dims pinned (argMin at session start) ----
CREATE OR REPLACE TABLE silver_session_dim AS
SELECT session_id,
       arg_min(platform, ts_ms)   AS platform,
       arg_min(country, ts_ms)    AS country,
       arg_min(content_id, ts_ms) AS content_id,
       arg_min(video_type, ts_ms) AS video_type,
       arg_min(show_name, ts_ms)  AS show_name,
       arg_min(user_id, ts_ms)    AS user_id,
       min(ts_ms) AS session_start_ms,
       max(ts_ms) AS last_seen_ms,
       max(CASE WHEN event_type='VideoSessionEnd' THEN 1 ELSE 0 END) AS has_close
FROM silver_events GROUP BY session_id;

-- ---- SILVER 3: transitions (explicit + gap-inferred + watermark) ----
CREATE OR REPLACE TABLE gap_transitions AS
WITH pulses AS (SELECT DISTINCT session_id, ts_ms FROM silver_events),
     g AS (SELECT session_id, ts_ms,
                  lag(ts_ms) OVER (PARTITION BY session_id ORDER BY ts_ms) AS prev
           FROM pulses)
SELECT session_id, prev + getvariable('gap_ms') AS ts_ms, 0 AS st FROM g
  WHERE prev IS NOT NULL AND ts_ms - prev > getvariable('gap_ms')
UNION ALL
SELECT session_id, ts_ms, 1 FROM g
  WHERE prev IS NOT NULL AND ts_ms - prev > getvariable('gap_ms');

CREATE OR REPLACE TABLE raw_transitions AS
SELECT session_id, ts_ms, st FROM silver_events WHERE st IS NOT NULL
UNION ALL
SELECT session_id, ts_ms, st FROM gap_transitions;

CREATE OR REPLACE TABLE watermark_close AS
SELECT s.session_id, s.last_seen_ms + getvariable('gap_ms') AS ts_ms, -1 AS st
FROM silver_session_dim s WHERE s.has_close = 0;

-- watermark_open: symmetric to watermark_close, but for the START edge.
-- This dataset is a single-day snapshot, so a real fraction of sessions
-- (19,860 of 108,486, ~18.3%) were already active before the window opened
-- and never have a VideoSessionStart event in this file -- without this,
-- they'd silently produce zero intervals despite having real activity
-- (heartbeats/backgrounds/closes) inside the window, undercounting peak
-- concurrency by ~28%. For any session missing VideoSessionStart, insert a
-- synthetic "became active" transition at its earliest observed timestamp.
-- If that earliest event is itself an AppBackgrounded (st=0), the dedup
-- step's min(st) rule below keeps the more restrictive 0 automatically --
-- no extra CASE logic needed to handle "window opened mid-background".
CREATE OR REPLACE TABLE watermark_open AS
SELECT s.session_id, s.session_start_ms AS ts_ms, 1 AS st
FROM silver_session_dim s
LEFT JOIN silver_events e ON e.session_id = s.session_id AND e.event_type = 'VideoSessionStart'
WHERE e.session_id IS NULL;

-- ---- SILVER 4: active intervals (state machine) ----
CREATE OR REPLACE TABLE silver_active_intervals AS
WITH allt AS (
    SELECT session_id, ts_ms, st FROM raw_transitions
    UNION ALL SELECT session_id, ts_ms, st FROM watermark_close
    UNION ALL SELECT session_id, ts_ms, st FROM watermark_open),
dedup AS (SELECT session_id, ts_ms, min(st) AS st FROM allt GROUP BY 1,2),
marked AS (SELECT *, lag(st) OVER (PARTITION BY session_id ORDER BY ts_ms) AS prev FROM dedup),
tr AS (SELECT session_id, ts_ms, st FROM marked WHERE prev IS NULL OR st IS DISTINCT FROM prev),
paired AS (SELECT session_id, ts_ms AS start_ms, st,
                  lead(ts_ms) OVER (PARTITION BY session_id ORDER BY ts_ms) AS end_ms
           FROM tr)
SELECT p.session_id, p.start_ms, p.end_ms,
       d.platform, d.country, d.content_id, d.video_type, d.show_name,
       CASE WHEN d.has_close=0 THEN 1 ELSE 0 END AS is_open
FROM paired p JOIN silver_session_dim d USING (session_id)
WHERE p.st = 1 AND p.end_ms IS NOT NULL AND p.end_ms > p.start_ms;

-- ---- GOLD: minute grain, cnt_a (fully-covers) + cnt_b (touches) ----
CREATE OR REPLACE TABLE gold_concurrency_minute AS
WITH a AS (
  SELECT session_id, platform, country, content_id, video_type, show_name, m AS minute_id
  FROM silver_active_intervals, range(ceil(start_ms/60000.0)::BIGINT, ceil(end_ms/60000.0)::BIGINT) t(m)
  WHERE end_ms > start_ms
),
b AS (
  SELECT session_id, platform, country, content_id, video_type, show_name, m AS minute_id
  FROM silver_active_intervals, range((start_ms // 60000)::BIGINT, ((end_ms-1) // 60000 + 1)::BIGINT) t(m)
  WHERE end_ms > start_ms
)
SELECT minute_id, platform, country, video_type, show_name, content_id,
       count(DISTINCT session_id) FILTER (WHERE kind='a') AS cnt_a,
       count(DISTINCT session_id) FILTER (WHERE kind='b') AS cnt_b
FROM (
  SELECT *, 'a' AS kind FROM a
  UNION ALL
  SELECT *, 'b' AS kind FROM b
)
GROUP BY 1,2,3,4,5,6;
