-- ============================================================================
-- SonyLIV Foreground-Only Concurrency — REFERENCE PIPELINE (DuckDB)
-- Mirrors the ClickHouse medallion model 1:1. Used to generate ground truth
-- for diffing against the ClickHouse implementation.
-- ============================================================================
SET VARIABLE gap_ms = 90000;   -- heartbeat timeout

-- ================= BRONZE : exact mirror of source ==========================
CREATE OR REPLACE TABLE bronze_events AS
  SELECT * FROM read_csv_auto('../click-a-thon-2026/SonyLiv/data/ch-hackathon-raw-data.csv');
CREATE OR REPLACE TABLE bronze_content AS
  SELECT * FROM read_csv_auto('../click-a-thon-2026/SonyLiv/data/ch-hackathon-content-data.csv');

-- ================= SILVER 1 : typed, normalised, deduped, enriched =========
CREATE OR REPLACE TABLE silver_events AS
SELECT DISTINCT ON (b.video_session_id, b.event_timestamp, b.event)
    b.video_session_id                                   AS session_id,
    b.user_id                                            AS user_id,
    b.content_id::BIGINT                                 AS content_id,
    b.event_timestamp                                    AS ts_ms,
    b.session_start_epoch                                AS session_start_ms,
    b.event, b.event_type, b.platform, b.country,
    coalesce(c.video_type,'unk')                         AS video_type,
    coalesce(c.category  ,'unk')                         AS category,
    coalesce(nullif(split_part(lower(b.audio_language)   ,'-',1),''),'unk') AS audio_language,
    coalesce(nullif(split_part(lower(b.subtitle_language),'-',1),''),'unk') AS subtitle_language,
    CASE
      WHEN b.event IN ('pause','speed-pause','AdPause')                    THEN 'STATE_INACTIVE'
      WHEN b.event IN ('resume','speed-resume','AdResume','Play')          THEN 'STATE_ACTIVE'
      WHEN b.event_type = 'AppBackgrounded'                                THEN 'STATE_INACTIVE'
      WHEN b.event_type = 'AppForegrounded'                                THEN 'STATE_ACTIVE'
      WHEN b.event_type = 'VideoSessionStart'                              THEN 'SESSION_OPEN'
      WHEN b.event_type = 'VideoSessionEnd'                                THEN 'SESSION_CLOSE'
      ELSE 'LIVENESS' END                                AS signal,
    CASE WHEN b.event = 'BufferStart' THEN 1 WHEN b.event = 'BufferEnd' THEN 2 ELSE 0 END AS buf
FROM bronze_events b
LEFT JOIN bronze_content c ON c.content_id = b.content_id;

-- ================= SILVER 2 : one row per session, dims pinned =============
CREATE OR REPLACE TABLE silver_session_dim AS
SELECT session_id,
       arg_min(platform  , ts_ms) AS platform,
       arg_min(country   , ts_ms) AS country,
       arg_min(content_id, ts_ms) AS content_id,
       arg_min(video_type, ts_ms) AS video_type,
       arg_min(user_id   , ts_ms) AS user_id,
       min(ts_ms) AS session_start_ms,
       max(ts_ms) AS last_seen_ms,
       max(CASE WHEN signal='SESSION_CLOSE' THEN 1 ELSE 0 END) AS has_close
FROM silver_events GROUP BY session_id;

-- ================= SILVER 3 : transitions (explicit + gap-inferred) ========
-- 3a. gap-inferred transitions from liveness silence
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

-- 3b. explicit transitions, per definition (D1 ignores pause, D2 honours it)
CREATE OR REPLACE TABLE raw_transitions AS
SELECT 'D1' AS defn, session_id, ts_ms,
       CASE WHEN signal='SESSION_CLOSE' THEN -1
            WHEN signal='SESSION_OPEN'  THEN 1
            WHEN event_type='AppBackgrounded' THEN 0
            WHEN event_type='AppForegrounded' THEN 1
            WHEN event='Play' THEN 1 END AS st
FROM silver_events
WHERE signal<>'LIVENESS' AND (event_type IN ('AppBackgrounded','AppForegrounded') OR signal IN ('SESSION_OPEN','SESSION_CLOSE') OR event='Play')
UNION ALL
SELECT 'D2', session_id, ts_ms,
       CASE WHEN signal='SESSION_CLOSE' THEN -1
            WHEN signal='SESSION_OPEN'  THEN 1
            WHEN signal='STATE_ACTIVE'  THEN 1
            WHEN signal='STATE_INACTIVE' THEN 0 END
FROM silver_events WHERE signal<>'LIVENESS'
UNION ALL SELECT 'D1', session_id, ts_ms, st FROM gap_transitions
UNION ALL SELECT 'D2', session_id, ts_ms, st FROM gap_transitions;

-- 3c. watermark close for sessions with no SESSION_CLOSE (open sessions)
CREATE OR REPLACE TABLE watermark_close AS
SELECT d.defn, s.session_id, s.last_seen_ms + getvariable('gap_ms') AS ts_ms, -1 AS st
FROM silver_session_dim s CROSS JOIN (SELECT 'D1' defn UNION ALL SELECT 'D2') d
WHERE s.has_close = 0;

-- ================= SILVER 4 : ACTIVE INTERVALS (the state machine) =========
CREATE OR REPLACE TABLE silver_active_intervals AS
WITH allt AS (
    SELECT defn, session_id, ts_ms, st FROM raw_transitions WHERE st IS NOT NULL
    UNION ALL SELECT defn, session_id, ts_ms, st FROM watermark_close),
-- collapse same-timestamp conflicts: -1 (close) beats 0 (inactive) beats 1
dedup AS (SELECT defn, session_id, ts_ms, min(st) AS st FROM allt GROUP BY 1,2,3),
-- IDEMPOTENCY: drop a transition identical to its predecessor
marked AS (SELECT *, lag(st) OVER (PARTITION BY defn, session_id ORDER BY ts_ms) AS prev FROM dedup),
tr AS (SELECT defn, session_id, ts_ms, st FROM marked WHERE prev IS NULL OR st IS DISTINCT FROM prev),
-- pair each ACTIVE with the next transition
paired AS (SELECT defn, session_id, ts_ms AS start_ms, st,
                  lead(ts_ms) OVER (PARTITION BY defn, session_id ORDER BY ts_ms) AS end_ms
           FROM tr)
SELECT p.defn, p.session_id, p.start_ms, p.end_ms,
       d.platform, d.country, d.content_id, d.video_type,
       CASE WHEN d.has_close=0 THEN 1 ELSE 0 END AS is_open
FROM paired p JOIN silver_session_dim d USING (session_id)
WHERE p.st = 1 AND p.end_ms IS NOT NULL AND p.end_ms > p.start_ms;

-- ================= SILVER 5 : session-minutes, DEDUPED =====================
CREATE OR REPLACE TABLE silver_session_minutes AS
SELECT DISTINCT defn, session_id, platform, country, content_id, video_type, m AS minute_id
FROM silver_active_intervals,
     range(start_ms // 60000, (end_ms - 1) // 60000 + 1) t(m);

-- ================= GOLD 1 : minute occupancy counts ========================
CREATE OR REPLACE TABLE gold_concurrency_minute AS
SELECT defn, minute_id, platform, country, video_type, content_id,
       count(DISTINCT session_id) AS cnt
FROM silver_session_minutes
GROUP BY 1,2,3,4,5,6;

-- ================= GOLD 2 : ±1 deltas (update-friendly hot path) ===========
CREATE OR REPLACE TABLE gold_concurrency_delta AS
SELECT defn, minute_id, platform, country, video_type, content_id, sum(d) AS delta
FROM (
    SELECT defn, start_ms // 60000 AS minute_id, platform, country, video_type, content_id, 1 AS d
      FROM silver_active_intervals
    UNION ALL
    SELECT defn, (end_ms - 1) // 60000 + 1, platform, country, video_type, content_id, -1
      FROM silver_active_intervals)
GROUP BY 1,2,3,4,5,6;

-- ============================================================================
-- FIX: deltas MUST be derived from the deduped minute representation, not from
-- raw intervals. A session holding two intervals inside one minute otherwise
-- emits +1 twice (the +9.54% double-count bug). Gaps-and-islands merge below
-- makes the two gold tables consistent BY CONSTRUCTION.
-- ============================================================================
CREATE OR REPLACE TABLE silver_merged_runs AS
WITH s AS (
  SELECT defn, session_id, platform, country, video_type, content_id, minute_id,
         minute_id - row_number() OVER (
           PARTITION BY defn, session_id ORDER BY minute_id) AS grp
  FROM silver_session_minutes)
SELECT defn, session_id, platform, country, video_type, content_id,
       min(minute_id) AS start_minute, max(minute_id) + 1 AS end_minute
FROM s GROUP BY defn, session_id, platform, country, video_type, content_id, grp;

CREATE OR REPLACE TABLE gold_concurrency_delta AS
SELECT defn, minute_id, platform, country, video_type, content_id, sum(d) AS delta
FROM (
    SELECT defn, start_minute AS minute_id, platform, country, video_type, content_id, 1 AS d
      FROM silver_merged_runs
    UNION ALL
    SELECT defn, end_minute,   platform, country, video_type, content_id, -1
      FROM silver_merged_runs)
GROUP BY 1,2,3,4,5,6;
