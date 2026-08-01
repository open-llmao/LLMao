-- ============================================================================
-- CLICKHOUSE PORT of the 5-object DuckDB pipeline.
-- Semantics MUST match ref (9-obj) and five (5-obj) DuckDB byte-for-byte on gold.
--
-- Engine choices (see TABLES.md for full rationale):
--   bronze_events        MergeTree           partitioned by day, ORDER BY session for locality
--   bronze_content       Dictionary(HASHED)  in-memory catalog join
--   silver_session_state ReplacingMergeTree  one row per session, last-write-wins
--   silver_active_intervals MergeTree        append-only intervals
--   gold_concurrency_minute SummingMergeTree pre-aggregated (cnt_a, cnt_b) per dim tuple
--
-- Types: video_session_id / user_id are 64-char uppercase hex -> FixedString(64).
--        content_id is BIGINT (up to 10 digits, with negative values in catalog) -> Int64.
-- ============================================================================

DROP TABLE IF EXISTS bronze_events;
DROP TABLE IF EXISTS bronze_content;
DROP TABLE IF EXISTS silver_session_state;
DROP TABLE IF EXISTS silver_active_intervals;
DROP TABLE IF EXISTS gold_concurrency_minute;

-- ---------------------------------------------------------------------------
-- (1) BRONZE: raw event log.  Partitioned by event day for locality + pruning.
-- ---------------------------------------------------------------------------
CREATE TABLE bronze_events
(
    content_id           Int64,
    video_session_id     FixedString(64),
    user_id              FixedString(64),
    event_type           LowCardinality(String),
    event                LowCardinality(String),
    event_timestamp      Int64,                  -- epoch ms
    event_ts             DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(event_timestamp),
    platform             LowCardinality(String),
    app_version          LowCardinality(String),
    country              LowCardinality(String),
    audio_language       LowCardinality(String),
    subtitle_language    LowCardinality(String),
    player_version       LowCardinality(String),
    session_start_epoch  Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (video_session_id, event_timestamp);

INSERT INTO bronze_events
SELECT * FROM file('{data_dir}/ch-hackathon-raw-data.csv', CSVWithNames);

-- ---------------------------------------------------------------------------
-- (2) BRONZE_CONTENT: catalog.  Small (33k rows) — Dictionary for O(1) lookup.
-- ---------------------------------------------------------------------------
CREATE TABLE bronze_content
(
    content_id  Int64,
    title       String,
    video_type  LowCardinality(String),
    category    LowCardinality(String)
)
ENGINE = MergeTree ORDER BY content_id;

INSERT INTO bronze_content
SELECT * FROM file('{data_dir}/ch-hackathon-content-data.csv', CSVWithNames);

-- ---------------------------------------------------------------------------
-- (3) SILVER_SESSION_STATE: one row per session.
--     - argMin pins dims to earliest event (0.87% multi-platform sessions;
--       deterministic > accurate — see DESIGN.md).
--     - transitions[] = ALL state-changing events (Start=+1, End=-1, BG=0, FG=+1,
--       Play=+1). Heartbeats do NOT enter transitions; they only feed live_ts.
--     - live_ts[] = distinct timestamps of ANY event, used to detect gap-based
--       inferred background (>50s inactivity, empirically derived).
-- ---------------------------------------------------------------------------
CREATE TABLE silver_session_state
(
    session_id       FixedString(64),
    platform         LowCardinality(String),
    country          LowCardinality(String),
    content_id       Int64,
    video_type       LowCardinality(String),
    session_start_ms Int64,
    last_seen_ms     Int64,
    has_close        UInt8,
    live_ts          Array(Int64),
    tr_ts            Array(Int64),
    tr_st            Array(Int8)
)
ENGINE = ReplacingMergeTree
ORDER BY session_id;

INSERT INTO silver_session_state
WITH tagged AS (
    SELECT
        b.video_session_id                                 AS sid,
        b.event_timestamp                                  AS ts,
        b.platform, b.country, b.content_id,
        c.video_type                                       AS vt_raw,
        CASE b.event_type
             WHEN 'VideoSessionStart' THEN CAST(1  AS Int8)
             WHEN 'VideoSessionEnd'   THEN CAST(-1 AS Int8)
             WHEN 'AppBackgrounded'   THEN CAST(0  AS Int8)
             WHEN 'AppForegrounded'   THEN CAST(1  AS Int8)
             ELSE CASE WHEN b.event = 'Play' THEN CAST(1 AS Int8) ELSE NULL END
        END                                                AS st
    FROM bronze_events b
    LEFT JOIN bronze_content c ON c.content_id = b.content_id
)
SELECT
    sid                                                    AS session_id,
    argMin(platform, ts)                                   AS platform,
    argMin(country,  ts)                                   AS country,
    argMin(content_id, ts)                                 AS content_id,
    argMin(coalesce(vt_raw,'unk'), ts)                     AS video_type,
    min(ts)                                                AS session_start_ms,
    max(ts)                                                AS last_seen_ms,
    maxIf(1, st = -1)                                      AS has_close,
    arraySort(groupUniqArray(ts))                          AS live_ts,
    -- transitions: keep pairs sorted by ts; filter NULL st client-side via arrayFilter+arrayMap
    arrayMap(x -> x.1, arraySort(x -> x.1, groupArrayIf(tuple(ts, st), st IS NOT NULL))) AS tr_ts,
    arrayMap(x -> x.2, arraySort(x -> x.1, groupArrayIf(tuple(ts, st), st IS NOT NULL))) AS tr_st
FROM tagged
GROUP BY sid;

-- ---------------------------------------------------------------------------
-- (4) SILVER_ACTIVE_INTERVALS: state machine → [start, end) runs of state=1.
--     Algorithm:
--       events_all = transitions ∪ gap_boundaries ∪ open_session_watermark
--       gap_boundaries: for each live_ts pair with gap > 50s, emit
--         (prev+50s, st=0)  and  (ts, st=1)  — silently close then reopen.
--       For each ts take min(st) (deterministic tie-break).
--       Keep transitions where state changes vs prior.
--       Interval [a,b) is ACTIVE iff st_at_a = 1.
-- ---------------------------------------------------------------------------
CREATE TABLE silver_active_intervals
(
    session_id  FixedString(64),
    start_ms    Int64,
    end_ms      Int64,
    platform    LowCardinality(String),
    country     LowCardinality(String),
    content_id  Int64,
    video_type  LowCardinality(String),
    is_open     UInt8
)
ENGINE = MergeTree
ORDER BY (session_id, start_ms);

INSERT INTO silver_active_intervals
WITH
    50000 AS gap_ms,
    -- 1) transitions from silver_session_state
    tr AS (
        SELECT session_id, ts, st, platform, country, content_id, video_type,
               has_close, last_seen_ms
        FROM silver_session_state
        ARRAY JOIN tr_ts AS ts, tr_st AS st
    ),
    -- 2) gap-inferred BG/FG pairs from live_ts
    lp AS (
        SELECT session_id, ts,
               lagInFrame(ts) OVER (PARTITION BY session_id ORDER BY ts) AS prev
        FROM ( SELECT session_id, live_ts FROM silver_session_state )
        ARRAY JOIN live_ts AS ts
    ),
    gaps AS (
        SELECT session_id, prev + gap_ms AS ts, CAST(0  AS Int8) AS st FROM lp WHERE prev > 0 AND ts - prev > gap_ms
        UNION ALL
        SELECT session_id, ts,               CAST(1  AS Int8) AS st FROM lp WHERE prev > 0 AND ts - prev > gap_ms
    ),
    -- 3) open-session watermark: last_seen + 50s = synthetic close
    wm AS (
        SELECT session_id, last_seen_ms + gap_ms AS ts, CAST(-1 AS Int8) AS st
        FROM silver_session_state
        WHERE has_close = 0
    ),
    -- 4) union all state-carrying events
    allt AS (
        SELECT session_id, ts, min(st) AS st FROM (
            SELECT session_id, ts, st FROM tr
            UNION ALL SELECT session_id, ts, st FROM gaps
            UNION ALL SELECT session_id, ts, st FROM wm
        )
        GROUP BY session_id, ts
    ),
    -- 5) keep only rows where state changes (or first)
    changed AS (
        SELECT session_id, ts, st FROM (
            SELECT session_id, ts, st,
                   lagInFrame(toInt16(st)) OVER (PARTITION BY session_id ORDER BY ts) AS pv,
                   count() OVER (PARTITION BY session_id ORDER BY ts
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn
            FROM allt
        )
        WHERE rn = 1 OR toInt16(st) != pv
    ),
    -- 6) pair each row with next row's ts → interval
    ivl AS (
        SELECT session_id, ts AS a, st,
               leadInFrame(ts) OVER (PARTITION BY session_id ORDER BY ts) AS b
        FROM changed
    )
SELECT
    ivl.session_id, ivl.a AS start_ms, ivl.b AS end_ms,
    s.platform, s.country, s.content_id, s.video_type,
    CASE WHEN s.has_close = 0 THEN 1 ELSE 0 END AS is_open
FROM ivl
INNER JOIN silver_session_state s ON s.session_id = ivl.session_id
WHERE ivl.st = 1 AND ivl.b > 0 AND ivl.b > ivl.a;

-- ---------------------------------------------------------------------------
-- (5) GOLD: minute occupancy, SummingMergeTree.  Both cnt_a and cnt_b.
-- ---------------------------------------------------------------------------
CREATE TABLE gold_concurrency_minute
(
    minute      DateTime,
    platform    LowCardinality(String),
    country     LowCardinality(String),
    video_type  LowCardinality(String),
    content_id  Int64,
    cnt_a       SimpleAggregateFunction(sum, UInt64),
    cnt_b       SimpleAggregateFunction(sum, UInt64)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (country, video_type, platform, content_id, minute);

INSERT INTO gold_concurrency_minute
WITH
    a AS (
        SELECT DISTINCT session_id, platform, country, video_type, content_id, m AS minute_id
        FROM silver_active_intervals
        ARRAY JOIN range(
            toUInt64(ceil(start_ms / 60000.)),
            toUInt64(ceil(end_ms   / 60000.))
        ) AS m
    ),
    b AS (
        SELECT DISTINCT session_id, platform, country, video_type, content_id, m AS minute_id
        FROM silver_active_intervals
        ARRAY JOIN range(
            toUInt64(intDiv(start_ms, 60000)),
            toUInt64(intDiv(end_ms - 1, 60000) + 1)
        ) AS m
    ),
    joined AS (
        SELECT coalesce(a.minute_id, b.minute_id)   AS minute_id,
               coalesce(a.platform,  b.platform)    AS platform,
               coalesce(a.country,   b.country)     AS country,
               coalesce(a.video_type,b.video_type)  AS video_type,
               coalesce(a.content_id,b.content_id)  AS content_id,
               a.session_id AS sa,
               b.session_id AS sb
        FROM a FULL OUTER JOIN b
          ON  a.session_id = b.session_id AND a.minute_id = b.minute_id
          AND a.platform   = b.platform   AND a.country   = b.country
          AND a.video_type = b.video_type AND a.content_id= b.content_id
    )
SELECT
    toDateTime(minute_id * 60)          AS minute,
    platform, country, video_type, content_id,
    toUInt64(countIf(sa != ''))         AS cnt_a,
    toUInt64(countIf(sb != ''))         AS cnt_b
FROM joined
GROUP BY minute_id, platform, country, video_type, content_id;
