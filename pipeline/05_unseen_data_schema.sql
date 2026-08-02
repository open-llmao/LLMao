-- ============================================================================
-- 05_unseen_data_schema.sql — the exact, live DDL for the "_v2" pipeline
-- (unseen-day / surprise dataset: video_resolution + show_name columns),
-- deployed on the same ClickHouse Cloud instance as the "_v1" (non-suffixed)
-- pipeline in 04_ddl_annotated.sql, side by side, with zero shared objects
-- and zero modification to any non-"_v2" table.
--
-- STATUS: DEPLOYED. Every statement below has actually been run against the
-- live ClickHouse Cloud instance and is a 1:1 mirror of `SHOW CREATE TABLE`
-- output — same convention as 04_ddl_annotated.sql. If this file and the
-- live instance ever disagree, the live instance is correct and this file
-- is stale — regenerate it.
--
-- THIS FILE SUPERSEDES AN EARLIER DRAFT VERSION OF ITSELF: an earlier
-- version of this file used the OLD RMV-based architecture (a plain VIEW
-- for silver_session_state_current, a REFRESH EVERY 15 SECOND materialized
-- view reprocessing full session history for interval detection) — that
-- architecture was replaced project-wide by the incremental, JOIN-based
-- design in 04_ddl_annotated.sql (see that file's header for the full
-- "why", including 4 real bugs found and fixed while building it). This
-- file applies that SAME validated architecture to the v2 schema, not the
-- old one. See 04_ddl_annotated.sql for the full narrative on:
--   - why session_live_state (a small "current status per session" table)
--     replaces the old array-accumulator + VIEW pair entirely
--   - why interval detection is a single incremental MV writing to an
--     intermediate session_transition_log table, split by two trivial
--     downstream incremental MVs (NOT two independent MVs both reading
--     session_live_state directly — that was BUG-inducing: no guaranteed
--     ordering between sibling MVs on the same source table)
--   - the 4 concrete bugs this architecture avoids (alias shadowing in
--     WHERE, missing seed-row idempotency across block boundaries,
--     LEFT JOIN default-fill breaking coalesce(), MV-with-top-level-
--     UNION-ALL only propagating its first branch)
--
-- WHAT'S DIFFERENT HERE, SPECIFIC TO v2 (all decisions confirmed earlier
-- in the session, based on profiling both datasets, ~7.9M rows total):
--   1. Nullable narrowed on bronze_events_raw_v2: content_id,
--      video_session_id, user_id, event_type, event, platform,
--      app_version, country have ZERO nulls in EITHER dataset — not
--      Nullable. audio_language, subtitle_language, player_version, and
--      the new video_resolution ARE genuinely nullable (evidence in
--      04_ddl_annotated.sql's sibling commit history / chat record).
--   2. Partition grain: toYYYYMM(session_start_epoch), same as bronze_v1,
--      NOT toDate(event_timestamp) — event_timestamp has a long noisy
--      tail (192 distinct calendar days in the unseen file, most with a
--      handful of stray rows) that would create ~190 near-empty junk
--      partitions if partitioned on directly.
--   3. platform case-duplicates (Mweb/MWEB, Web/WEB — 394 rows in the
--      unseen file) fixed via upper(trim(...)) in the transition-log MV,
--      same "normalize downstream, keep bronze byte-for-byte" pattern
--      already used for the video_type empty-string fix.
--   4. video_resolution: adaptive-bitrate value, confirmed NOT tied to
--      foreground/background/active-state detection anywhere in
--      dataset_details.md or the problem statement — pinned session-level
--      via argMinIf (NOT argMin: video_resolution is NULL specifically on
--      VideoSessionStart, which is always a session's EARLIEST event, so
--      a plain argMin would always select that always-NULL row for every
--      session — argMinIf restricts candidates to non-null resolution
--      values first). The raw string (~2,070 distinct values) is kept
--      silver-only (bloom_filter skip index, for ad-hoc exact filtering) —
--      but a BUCKETED tier (4K/1080p/720p/540p/480p/360p/240p/below_240p/
--      unk — 9 values, derived from min(width,height)) IS added to gold's
--      GROUP BY grain, per a later revision in this same file (see step 8's
--      comment for the full reasoning and the 2 bugs found building it).
--      This supersedes an earlier version of this decision that excluded
--      video_resolution from gold entirely.
--   5. show_name: clean (0 nulls/empties, 360 distinct, many-content-ids-
--      per-show), added to gold's dimension grain right after video_type
--      — cheap, same treatment as video_type/category.
--
-- Build order (mirrors 04_ddl_annotated.sql's structure exactly):
--   1. bronze_events_raw_v2, bronze_content_raw_v2   (loaded directly, not ClickPipes-managed for this v2 path)
--   2. session_live_state_v2                            (table — current status per session)
--   3. session_transition_log_v2                           (table — intermediate log, kind='state'|'interval')
--   4. mv_session_transition_log_v2                           (incremental MV, TO #3, the whole state machine)
--   5. mv_to_session_live_state_v2                                (incremental MV, TO #2, trivial filter)
--   6. silver_active_intervals_v2                                    (table)
--   7. mv_to_silver_active_intervals_v2                                 (incremental MV, TO #6, trivial filter)
--   8. gold_concurrency_minute_v2                                          (table, G1)
--   9. mv_gold_concurrency_minute_v2                                          (incremental MV, TO #8)
--  10. gold_concurrency_delta_v2                                                (table, G2)
--  11. mv_gold_concurrency_delta_v2                                                (incremental MV, TO #10)
--
-- NONE of these are RMVs. The only place time-based logic exists at all is
-- the query-time watermark for currently-open/silent sessions, documented
-- in 04_ddl_annotated.sql (applies identically here, just against
-- session_live_state_v2 instead of session_live_state).
--
-- DATA LOADING NOTE: bronze_events_raw_v2/bronze_content_raw_v2 are loaded
-- directly from the unseen-day CSVs (ch-hackathon-raw-data_surprise.csv,
-- ch-hackathon-content-data_surprise.csv), not via ClickPipes — there is no
-- live Kafka/Kinesis source for the unseen-day file, so a one-time bulk
-- load into bronze is the ingestion mechanism here. This is NOT a
-- violation of "no manual INSERT past bronze": bronze is, by definition,
-- the raw ingestion boundary — every downstream table from
-- session_live_state_v2 onward is still populated exclusively by the
-- incremental MVs below, never by a hand-run INSERT ... SELECT.
-- ============================================================================


-- ============================================================================
-- 1. BRONZE
-- ============================================================================
CREATE TABLE bronze_events_raw_v2
(
    `content_id`         Int64,
    `video_session_id`   String,
    `user_id`            String,
    `event_type`         String,
    `event`              String,
    `event_timestamp`    Int64,
    `platform`           String,
    `app_version`        String,
    `country`            String,
    `audio_language`     Nullable(String),
    `subtitle_language`  Nullable(String),
    `player_version`     Nullable(String),
    `video_resolution`   Nullable(String),
    `session_start_epoch` Int64,

    INDEX idx_app_version       app_version       TYPE bloom_filter GRANULARITY 4,
    INDEX idx_audio_language    audio_language    TYPE bloom_filter GRANULARITY 4,
    INDEX idx_subtitle_language subtitle_language TYPE bloom_filter GRANULARITY 4,
    INDEX idx_player_version    player_version     TYPE bloom_filter GRANULARITY 4
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(toDateTime(intDiv(session_start_epoch, 1000)))
ORDER BY (video_session_id, event, event_timestamp);

CREATE TABLE bronze_content_raw_v2
(
    `content_id` Int64,
    `title`      String,
    `video_type` LowCardinality(String),
    `category`   LowCardinality(String),
    `show_name`  LowCardinality(String)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY content_id;


-- ============================================================================
-- 2. SILVER — session_live_state_v2
-- Same role and reasoning as session_live_state in 04_ddl_annotated.sql
-- step 2 — one row per session, current status, ReplacingMergeTree(version).
-- Extended with show_name and video_resolution (both pinned dims, carried
-- forward from `prior` for existing sessions, taken from the session's own
-- earliest evidence for brand-new ones — see mv_session_transition_log_v2's
-- `new_dims` CTE).
-- ============================================================================
CREATE TABLE session_live_state_v2
(
    session_id                 FixedString(64),
    is_active                  UInt8,
    current_interval_start_ms  Int64,
    last_seen_ms               Int64,
    has_close                  UInt8,
    platform                   LowCardinality(String),
    country                    LowCardinality(String),
    content_id                 Int64,
    video_type                 LowCardinality(String),
    show_name                  LowCardinality(String),
    video_resolution           LowCardinality(String),
    version                    Int64
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
ORDER BY session_id;


-- ============================================================================
-- 3. SILVER — session_transition_log_v2 (intermediate table, not queried directly)
-- Same role as session_transition_log in 04_ddl_annotated.sql step 3 — the
-- fix for the sibling-MV race condition (two MVs both reading
-- session_live_state_v2 on the same trigger, no guaranteed ordering
-- between them). One MV (step 4) computes the whole state machine ONCE
-- and writes both row kinds here; two trivial downstream MVs (steps 5, 7)
-- split by `kind` — safe, because by the time they fire, this table is
-- already fully and correctly written.
-- ============================================================================
CREATE TABLE session_transition_log_v2
(
    kind                       Enum8('state' = 1, 'interval' = 2),
    session_id                 FixedString(64),
    is_active                  UInt8,
    current_interval_start_ms  Int64,
    start_ms                   Int64,
    end_ms                     Int64,
    is_open                    UInt8,
    last_seen_ms               Int64,
    has_close                  UInt8,
    platform                   LowCardinality(String),
    country                    LowCardinality(String),
    content_id                 Int64,
    video_type                 LowCardinality(String),
    show_name                  LowCardinality(String),
    video_resolution           LowCardinality(String),
    version                    Int64
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (kind, session_id, version);


-- ============================================================================
-- 4. SILVER — mv_session_transition_log_v2 (INCREMENTAL MV — the whole state machine)
--
-- Identical algorithm to mv_session_transition_log in 04_ddl_annotated.sql
-- step 4 (same tagged/prior/gaps/seed/combined/dedup/marked/changed/paired
-- CTEs, same 4 bugfixes: gap_ts alias rename, seed-row idempotency,
-- join_use_nulls=1, outer SELECT * FROM (...) wrap around the UNION ALL).
-- Two v2-specific additions:
--   - platform: upper(trim(b.platform)) — fixes Mweb/MWEB, Web/WEB.
--   - video_resolution: argMinIf(video_resolution_raw, ts, has_resolution)
--     in `new_dims` — the session's earliest KNOWN (non-null) resolution,
--     not its earliest event (which is always the NULL-resolution
--     VideoSessionStart). `video_resolution_raw` is
--     coalesce(replace(b.video_resolution, ' ', ''), '') — strips the
--     space-vs-no-space formatting noise ("1920 * 1080" vs "1920*1080")
--     while leaving the Auto-/NA- prefixes intact (semantically distinct:
--     Auto means adaptive quality selection). The final
--     coalesce(nullIf(...,''),'unk') in `resolved` catches the residual
--     edge case of a session with zero non-null resolution readings ever.
-- ============================================================================
CREATE MATERIALIZED VIEW mv_session_transition_log_v2
TO session_transition_log_v2 AS
SELECT * FROM (
WITH
    60000 AS gap_ms,  -- 1 minute, per dataset_details.md's stated heartbeat cadence
    tagged AS (
        SELECT
            CAST(b.video_session_id, 'FixedString(64)') AS session_id,
            b.event_timestamp AS ts,
            upper(trim(b.platform)) AS platform,
            b.country, b.content_id,
            coalesce(nullIf(c.video_type, ''), 'unk') AS video_type,
            coalesce(nullIf(c.show_name, ''), 'unk') AS show_name,
            coalesce(replace(b.video_resolution, ' ', ''), '') AS video_resolution_raw,
            isNotNull(b.video_resolution) AS has_resolution,
            CASE b.event_type
                 WHEN 'VideoSessionStart' THEN CAST(1  AS Int8)
                 WHEN 'VideoSessionEnd'   THEN CAST(-1 AS Int8)
                 WHEN 'AppBackgrounded'   THEN CAST(0  AS Int8)
                 WHEN 'AppForegrounded'   THEN CAST(1  AS Int8)
                 ELSE CASE WHEN b.event = 'Play' THEN CAST(1 AS Int8) ELSE NULL END
            END AS st
        FROM bronze_events_raw_v2 b
        LEFT JOIN bronze_content_raw_v2 c ON c.content_id = b.content_id
    ),
    prior AS (
        SELECT session_id,
               argMax(is_active, version) AS is_active,
               argMax(current_interval_start_ms, version) AS current_interval_start_ms,
               argMax(last_seen_ms, version) AS last_seen_ms,
               argMax(has_close, version) AS has_close,
               argMax(platform, version) AS platform,
               argMax(country, version) AS country,
               argMax(content_id, version) AS content_id,
               argMax(video_type, version) AS video_type,
               argMax(show_name, version) AS show_name,
               argMax(video_resolution, version) AS video_resolution
        FROM session_live_state_v2
        WHERE session_id IN (SELECT DISTINCT session_id FROM tagged)
        GROUP BY session_id
    ),
    all_pulses AS (
        SELECT session_id, ts FROM tagged
        UNION ALL
        SELECT session_id, last_seen_ms AS ts FROM prior
    ),
    lp AS (
        SELECT session_id, ts, lagInFrame(ts) OVER (PARTITION BY session_id ORDER BY ts) AS prev
        FROM all_pulses
    ),
    gaps AS (
        -- alias renamed gap_ts (not ts) -- prevents ClickHouse substituting the
        -- SELECT-list alias into the WHERE clause, which silently zeroes out
        -- this branch (see 04_ddl_annotated.sql step 4, BUG 1)
        SELECT session_id, prev + gap_ms AS gap_ts, CAST(0 AS Int8) AS st FROM lp WHERE prev > 0 AND ts - prev > gap_ms
        UNION ALL
        SELECT session_id, ts AS gap_ts,             CAST(1 AS Int8) AS st FROM lp WHERE prev > 0 AND ts - prev > gap_ms
    ),
    seed AS (
        -- seeds idempotency across block boundaries (BUG 2)
        SELECT session_id, last_seen_ms AS gap_ts, toInt8(is_active) AS st, 1 AS is_seed
        FROM prior
    ),
    combined AS (
        SELECT session_id, ts AS gap_ts, st, 0 AS is_seed FROM tagged WHERE st IS NOT NULL
        UNION ALL
        SELECT session_id, gap_ts, st, 0 AS is_seed FROM gaps
        UNION ALL
        SELECT session_id, gap_ts, st, is_seed FROM seed
    ),
    dedup AS (SELECT session_id, gap_ts, min(st) AS st, max(is_seed) AS is_seed FROM combined GROUP BY 1, 2),
    marked AS (
        SELECT *, lagInFrame(st) OVER (PARTITION BY session_id ORDER BY gap_ts) AS pv,
               count() OVER (PARTITION BY session_id ORDER BY gap_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn
        FROM dedup
    ),
    changed AS (SELECT session_id, gap_ts, st FROM marked WHERE (rn = 1 OR st != pv) AND is_seed = 0),
    changed_with_seed AS (
        SELECT session_id, gap_ts, st FROM changed
        UNION ALL
        SELECT session_id, gap_ts, st FROM seed
    ),
    paired AS (
        SELECT session_id, gap_ts AS a, st,
               leadInFrame(gap_ts) OVER (PARTITION BY session_id ORDER BY gap_ts ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS b,
               row_number() OVER (PARTITION BY session_id ORDER BY gap_ts DESC) AS rn_desc
        FROM changed_with_seed
    ),
    new_dims AS (
        SELECT session_id,
               argMin(platform, ts) AS platform, argMin(country, ts) AS country,
               argMin(content_id, ts) AS content_id, argMin(video_type, ts) AS video_type,
               argMin(show_name, ts) AS show_name,
               argMinIf(video_resolution_raw, ts, has_resolution) AS video_resolution
        FROM tagged GROUP BY session_id
    ),
    last_pulse AS ( SELECT session_id, max(ts) AS last_seen_ms FROM all_pulses GROUP BY session_id ),
    resolved AS (
        SELECT
            p.session_id AS session_id, p.a AS a, p.st AS st, p.b AS b, p.rn_desc AS rn_desc,
            coalesce(pr.platform, nd.platform) AS platform,
            coalesce(pr.country, nd.country) AS country,
            coalesce(pr.content_id, nd.content_id) AS content_id,
            coalesce(pr.video_type, nd.video_type) AS video_type,
            coalesce(pr.show_name, nd.show_name) AS show_name,
            coalesce(nullIf(coalesce(pr.video_resolution, nd.video_resolution), ''), 'unk') AS video_resolution,
            coalesce(pr.has_close, 0) AS prior_has_close,
            lp.last_seen_ms AS last_seen_ms
        FROM paired p
        JOIN new_dims nd ON nd.session_id = p.session_id
        JOIN last_pulse lp ON lp.session_id = p.session_id
        LEFT JOIN prior pr ON pr.session_id = p.session_id
        -- BUG 3: without this, coalesce() never falls through to nd.* for
        -- brand-new sessions -- unmatched LEFT JOIN columns default-fill to
        -- '' / 0, not true NULL, unless this setting is on.
        SETTINGS join_use_nulls = 1
    )
SELECT
    CAST('state' AS Enum8('state' = 1, 'interval' = 2)) AS kind,
    session_id, toUInt8(st = 1) AS is_active, if(st = 1, a, 0) AS current_interval_start_ms,
    0 AS start_ms, 0 AS end_ms, 0 AS is_open,
    last_seen_ms, toUInt8(prior_has_close = 1 OR st = -1) AS has_close,
    platform, country, content_id, video_type, show_name, video_resolution, last_seen_ms AS version
FROM resolved WHERE rn_desc = 1

UNION ALL

SELECT
    CAST('interval' AS Enum8('state' = 1, 'interval' = 2)) AS kind,
    session_id, 0 AS is_active, 0 AS current_interval_start_ms,
    a AS start_ms, b AS end_ms, 0 AS is_open,
    last_seen_ms, 0 AS has_close,
    platform, country, content_id, video_type, show_name, video_resolution, last_seen_ms AS version
FROM resolved WHERE st = 1 AND rn_desc > 1 AND b > a
);  -- BUG 4: this outer wrap is load-bearing -- an MV with a top-level UNION
    -- ALL only propagates its first branch to the target table.


-- ============================================================================
-- 5. SILVER — mv_to_session_live_state_v2 (INCREMENTAL MV — trivial filter)
-- Safe for the same reason as mv_to_session_live_state in
-- 04_ddl_annotated.sql step 5: reads session_transition_log_v2, which is
-- fully and correctly written by step 4 before this MV ever fires.
-- ============================================================================
CREATE MATERIALIZED VIEW mv_to_session_live_state_v2
TO session_live_state_v2 AS
SELECT session_id, is_active, current_interval_start_ms, last_seen_ms, has_close,
       platform, country, content_id, video_type, show_name, video_resolution, version
FROM session_transition_log_v2
WHERE kind = 'state';


-- ============================================================================
-- 6. SILVER — silver_active_intervals_v2 (table)
--
-- Adds show_name and video_resolution columns vs. the v1 schema. The
-- bloom_filter skip index on video_resolution is for the RAW string
-- specifically: the raw value (~2,070 distinct) is still NOT part of
-- gold's GROUP BY grain (would multiply gold row count by hundreds), so
-- ad-hoc "concurrency filtered by an EXACT raw resolution string" queries
-- read this table directly with countDistinct(session_id), not the
-- pre-aggregated gold path — the skip index is what keeps that query
-- fast. A separate, low-cardinality BUCKETED tier of this same column
-- (4K/1080p/720p/.../unk, 9 values) IS in gold's grain (see gold_
-- concurrency_minute_v2's comment) — the two coexist: bucketed tier for
-- fast/common "which quality tier" filters via gold, raw string via this
-- skip index for the rarer "this exact resolution string" query. show_name
-- doesn't need its own index here since it IS in gold's grain (filtered
-- reads on it go through gold_concurrency_minute_v2, which prunes via its
-- own ORDER BY prefix).
-- ============================================================================
CREATE TABLE silver_active_intervals_v2
(
    session_id       FixedString(64),
    start_ms         Int64,
    end_ms           Int64,
    platform         LowCardinality(String),
    country          LowCardinality(String),
    content_id       Int64,
    video_type       LowCardinality(String),
    show_name        LowCardinality(String),
    video_resolution LowCardinality(String),
    is_open          UInt8,
    version          Int64,

    INDEX idx_video_resolution video_resolution TYPE bloom_filter GRANULARITY 4
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(start_ms))
ORDER BY (session_id, start_ms);


-- ============================================================================
-- 7. SILVER — mv_to_silver_active_intervals_v2 (INCREMENTAL MV — trivial filter)
-- Same safety argument as step 5.
-- ============================================================================
CREATE MATERIALIZED VIEW mv_to_silver_active_intervals_v2
TO silver_active_intervals_v2 AS
SELECT session_id, start_ms, end_ms, platform, country, content_id, video_type, show_name, video_resolution, is_open, version
FROM session_transition_log_v2
WHERE kind = 'interval';


-- ============================================================================
-- 8. GOLD — gold_concurrency_minute_v2 (G1, table)
-- Adds show_name AND video_resolution_tier to the dimension grain.
--
-- video_resolution_tier — SUPERSEDES an earlier decision in this project to
-- exclude video_resolution from gold entirely. Revisited because the raw
-- string's ~2,070 distinct values (confirmed unsafe for gold's GROUP BY)
-- are almost entirely FORMATTING noise, not real diversity: bucketing by
-- the SMALLER of the two parsed dimensions (the "p" convention -- height in
-- landscape, e.g. 640x360 -> 360p; width in portrait, e.g. 1080x1920 ->
-- 1080p, which also matches how portrait mobile video is commonly labeled)
-- collapses this to exactly 9 canonical tiers on the real unseen-day data:
-- 4K, 1080p, 720p, 540p, 480p, 360p, 240p, below_240p, unk. That's safe for
-- gold's GROUP BY (same order of magnitude as platform's own cardinality).
-- Computed at READ TIME in this MV's SELECT from silver's already-pinned
-- raw video_resolution column -- no changes needed upstream
-- (session_live_state_v2 / session_transition_log_v2 untouched), since the
-- raw value was already being carried through for the silver-level filter.
--
-- TWO REAL BUGS CAUGHT WHILE BUILDING THIS BUCKETING EXPRESSION (verified
-- against the real unseen-day dataset, not just inspection):
--   BUG 5 -- WRONG DIMENSION CHOICE: an initial version bucketed by the
--   LARGER of the two parsed dimensions (max(w,h)), reasoning "biggest
--   number = resolution". This is wrong: for a standard landscape
--   640x360 video, max(640,360)=640 lands in the "540p" bucket, but the
--   real/standard tier name is "360p" (based on height). Caught by testing
--   a concrete session with video_resolution='640*360' end-to-end and
--   noticing the output tier didn't match the commonly-understood name.
--   Fixed by using min(w,h) instead, which is correct for BOTH landscape
--   (height is the smaller number) and portrait (the meaningful "p" value
--   is still the smaller number) orientations.
--   BUG 6 -- LEADING-NOISE DIGITS CORRUPT min(): switching to min(w,h) by
--   extracting "every digit sequence found anywhere in the string"
--   (arrayMax/arrayMin over ALL digit groups) breaks on real values like
--   "0-1280*720" -- the leading "0-" is itself a digit group, so
--   min(0,1280,720)=0, incorrectly bucketing a real 720p video as "unk".
--   42,371+36,416 rows in the real dataset have exactly this "0-" prefix
--   pattern. Fixed by extracting ONLY the specific "digits*digits" pattern
--   via extractGroups(video_resolution, '(\d+)\s*\*\s*(\d+)') -- requiring
--   the literal '*' between the two numbers naturally excludes the
--   leading "0-" (or "NA-", "Auto-", "DataSaver-") noise without needing
--   any lookahead/lookbehind tricks, since ClickHouse's re2 regex engine
--   doesn't support those anyway.
-- ============================================================================
CREATE TABLE gold_concurrency_minute_v2
(
    minute                DateTime,
    platform              LowCardinality(String),
    country               LowCardinality(String),
    video_type            LowCardinality(String),
    show_name             LowCardinality(String),
    video_resolution_tier LowCardinality(String),
    content_id            Int64,
    cnt_a                 AggregateFunction(uniqExact, FixedString(64)),
    cnt_b                 AggregateFunction(uniqExact, FixedString(64))
)
ENGINE = SharedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(minute)
ORDER BY (country, video_type, show_name, video_resolution_tier, platform, content_id, minute);


-- ============================================================================
-- 9. GOLD — mv_gold_concurrency_minute_v2 (INCREMENTAL MV, TO #8)
-- Attaches directly to silver_active_intervals_v2 (itself incrementally
-- populated), fires per newly-inserted interval -- no RMV anywhere in this
-- pipeline. Same UNION ALL structure as v1 (cnt_a fully-covers, cnt_b
-- touches); safe from BUG 4 because it's wrapped inside a FROM subquery
-- consumed by an outer GROUP BY, not a raw top-level UNION ALL.
-- ============================================================================
CREATE MATERIALIZED VIEW mv_gold_concurrency_minute_v2
TO gold_concurrency_minute_v2 AS
SELECT
    minute, platform, country, video_type, show_name,
    multiIf(
        mindim >= 2160, '4K',
        mindim >= 1080, '1080p',
        mindim >= 720,  '720p',
        mindim >= 540,  '540p',
        mindim >= 480,  '480p',
        mindim >= 360,  '360p',
        mindim >= 240,  '240p',
        mindim > 0,     'below_240p',
        'unk'
    ) AS video_resolution_tier,
    content_id,
    uniqExactStateIf(session_id, kind = 'a') AS cnt_a,
    uniqExactStateIf(session_id, kind = 'b') AS cnt_b
FROM
(
    SELECT
        toDateTime(minute_id * 60) AS minute,
        platform, country, video_type, show_name, content_id, session_id, 'a' AS kind,
        least(
            nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[1]), 0),
            nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[2]), 0)
        ) AS mindim
    FROM silver_active_intervals_v2
    ARRAY JOIN range(
        toUInt32(ceil(start_ms / 60000.)),
        toUInt32(ceil(end_ms   / 60000.))
    ) AS minute_id
    WHERE end_ms > start_ms

    UNION ALL

    SELECT
        toDateTime(minute_id * 60) AS minute,
        platform, country, video_type, show_name, content_id, session_id, 'b' AS kind,
        least(
            nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[1]), 0),
            nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[2]), 0)
        ) AS mindim
    FROM silver_active_intervals_v2
    ARRAY JOIN range(
        toUInt32(intDiv(start_ms, 60000)),
        toUInt32(intDiv(end_ms - 1, 60000)) + 1
    ) AS minute_id
    WHERE end_ms > start_ms
)
GROUP BY minute, platform, country, video_type, show_name, video_resolution_tier, content_id;


-- ============================================================================
-- 10. GOLD — gold_concurrency_delta_v2 (G2, table) -- same show_name and
-- video_resolution_tier additions as G1.
-- ============================================================================
CREATE TABLE gold_concurrency_delta_v2
(
    minute                DateTime,
    platform              LowCardinality(String),
    country                LowCardinality(String),
    video_type             LowCardinality(String),
    show_name              LowCardinality(String),
    video_resolution_tier  LowCardinality(String),
    content_id             Int64,
    delta_kind             LowCardinality(FixedString(1)),
    delta                  Int64
)
ENGINE = SharedSummingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(minute)
ORDER BY (country, video_type, show_name, video_resolution_tier, platform, content_id, minute, delta_kind);


-- ============================================================================
-- 11. GOLD — mv_gold_concurrency_delta_v2 (INCREMENTAL MV, TO #10)
-- Same reasoning as step 9.
-- ============================================================================
CREATE MATERIALIZED VIEW mv_gold_concurrency_delta_v2
TO gold_concurrency_delta_v2 AS
SELECT minute, platform, country, video_type, show_name,
    multiIf(
        mindim >= 2160, '4K', mindim >= 1080, '1080p', mindim >= 720, '720p',
        mindim >= 540, '540p', mindim >= 480, '480p', mindim >= 360, '360p',
        mindim >= 240, '240p', mindim > 0, 'below_240p', 'unk'
    ) AS video_resolution_tier,
    content_id, delta_kind, sum(delta) AS delta
FROM
(
    SELECT toDateTime(toUInt32(ceil(start_ms / 60000.)) * 60) AS minute,
           platform, country, video_type, show_name, content_id,
           least(nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[1]), 0),
                 nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[2]), 0)) AS mindim,
           'a' AS delta_kind, toInt64(1) AS delta
    FROM silver_active_intervals_v2 WHERE end_ms > start_ms
    UNION ALL
    SELECT toDateTime(toUInt32(ceil(end_ms / 60000.)) * 60),
           platform, country, video_type, show_name, content_id,
           least(nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[1]), 0),
                 nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[2]), 0)),
           'a', toInt64(-1)
    FROM silver_active_intervals_v2 WHERE end_ms > start_ms

    UNION ALL

    SELECT toDateTime(toUInt32(intDiv(start_ms, 60000)) * 60),
           platform, country, video_type, show_name, content_id,
           least(nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[1]), 0),
                 nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[2]), 0)),
           'b', toInt64(1)
    FROM silver_active_intervals_v2 WHERE end_ms > start_ms
    UNION ALL
    SELECT toDateTime(toUInt32(intDiv(end_ms - 1, 60000) + 1) * 60),
           platform, country, video_type, show_name, content_id,
           least(nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[1]), 0),
                 nullIf(toUInt32OrZero(extractGroups(video_resolution, '(\\d+)\\s*\\*\\s*(\\d+)')[2]), 0)),
           'b', toInt64(-1)
    FROM silver_active_intervals_v2 WHERE end_ms > start_ms
)
GROUP BY minute, platform, country, video_type, show_name, video_resolution_tier, content_id, delta_kind;


-- ============================================================================
-- DEPLOYMENT STATUS (as of this file's last update):
--   All 12 objects above exist on the live ClickHouse Cloud instance, all
--   currently EMPTY (0 rows each) -- bronze_events_raw_v2/bronze_content_raw_v2
--   have not yet been loaded from the unseen-day CSVs
--   (ch-hackathon-raw-data_surprise.csv, ch-hackathon-content-data_surprise.csv).
--   Once loaded, every downstream table populates automatically via the
--   incremental MVs above -- no manual step needed past the initial bronze
--   load. The non-"_v2" pipeline (04_ddl_annotated.sql) is untouched by any
--   of this and was independently verified empty/consistent before this
--   file's objects were created.
--   Every statement in this file (including the video_resolution_tier
--   addition and its 2 bugfixes) was first validated end-to-end against a
--   local throwaway ClickHouse container with realistic multi-session test
--   data before being applied to the live instance -- same practice used
--   throughout this project.
-- ============================================================================
