-- ============================================================================
-- DDL.sql — the exact, live DDL for every table in the pipeline, in build order.
--
-- This file is a 1:1 mirror of `SHOW CREATE TABLE` output from the running
-- ClickHouse Cloud instance — every statement here is deployed and verified,
-- not aspirational. If this file and the live instance ever disagree, the
-- live instance is correct and this file is stale — regenerate it.
--
-- Full "why" narrative lives in ../PIPELINE_LOGIC.md. This file is the
-- "what was actually run" companion — read together.
--
-- Build order matters: each object depends on the one before it.
--   1. bronze_events_raw, bronze_content_raw        (ClickPipes-managed, not created by this file)
--   2. mv_silver_session_state                        (incremental MV, owns its storage)
--   3. silver_session_state_current                    (plain VIEW)
--   4. silver_active_intervals                          (table)
--   5. mv_silver_active_intervals                        (refreshable MV, TO #4)
--   6. gold_concurrency_minute                            (table, G1)
--   7. mv_gold_concurrency_minute                          (refreshable MV, TO #6, DEPENDS ON #5)
--   8. gold_concurrency_delta                                (table, G2)
--   9. mv_gold_concurrency_delta                              (refreshable MV, TO #8, DEPENDS ON #5)
-- ============================================================================


-- ============================================================================
-- 1. BRONZE — pre-existing, ClickPipes-managed. Included here for completeness
--    and so the whole pipeline can be reasoned about from one file, but these
--    two tables are NOT created by this script — ClickPipes owns their schema.
-- ============================================================================

-- WHAT:  raw, append-only event log. One row per event emission.
-- WHY:   immutable source of truth. Every silver/gold number must be
--        reproducible by replaying from here.
-- ENGINE: SharedMergeTree — ClickHouse Cloud's storage/compute-separated
--        default. Deliberately NOT ReplacingMergeTree: we want duplicate
--        events preserved (13 dup VideoSessionStart, 14 dup VideoSessionEnd
--        exist in this dataset) so downstream layers detect and handle them
--        deliberately, instead of ClickHouse silently picking a winner.
-- PARTITION: toStartOfHour(session_start_epoch) — prunes by session-start
--        hour; at petabyte scale switch to toYYYYMM(event_ts) to stay under
--        the ~100-partitions/table rule of thumb.
CREATE TABLE bronze_events_raw
(
    `content_id` Nullable(Int64),
    `video_session_id` String,
    `user_id` Nullable(String),
    `event_type` Nullable(String),
    `event` String,
    `event_timestamp` Int64,
    `platform` Nullable(String),
    `app_version` Nullable(String),
    `country` Nullable(String),
    `audio_language` Nullable(String),
    `subtitle_language` Nullable(String),
    `player_version` Nullable(String),
    `session_start_epoch` Int64,
    `_key` Nullable(String),
    `_timestamp` Nullable(DateTime64(3)),
    `_partition` Nullable(Int32),
    `_offset` Nullable(Int64),
    `_topic` Nullable(String),
    `_header_keys` Array(String),
    `_header_values` Array(String),
    `_raw_message` Nullable(String)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toStartOfHour(toDateTime(intDiv(session_start_epoch, 1000)))
ORDER BY (video_session_id, event, event_timestamp);


-- WHAT:  content catalog, content_id -> (title, video_type, category).
-- WHY:   video_type is a required filter dimension but lives in the catalog,
--        not the event stream. Joined in once per session, not re-joined
--        on every query.
-- ENGINE: SharedMergeTree, ORDER BY content_id — small table (33,469 rows),
--        point-lookup shaped. A Dictionary(HASHED) layer on top is the
--        natural next step at higher scale for O(1) lookup.
-- NOTE:  1,089 rows here have video_type = '' (empty string, not NULL) —
--        a genuine data-quality gap in the source catalog. Handled
--        downstream in mv_silver_session_state (see step 2).
CREATE TABLE bronze_content_raw
(
    `content_id` Int64,
    `title` String,
    `video_type` LowCardinality(String),
    `category` LowCardinality(String)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY content_id;


-- ============================================================================
-- 2. SILVER — mv_silver_session_state
--
-- WHAT:  one row per video_session_id, but every column is an AGGREGATE
--        FUNCTION STATE (argMinState, minState, maxState, groupUniqArrayState,
--        groupArrayIfState), not a plain value.
--
-- WHY STATES, NOT ARRAYS:
--        A session's heartbeats arrive across many separate bronze INSERT
--        blocks over its lifetime — sometimes minutes apart. An incremental
--        MV only ever sees the block that just landed, never the session's
--        full history. If this table stored plain arrays, each MV firing
--        would only capture that block's fragment — the array would never
--        represent everything known about the session.
--
--        AggregateFunction states solve this because they are associative
--        and mergeable: AggregatingMergeTree correctly combines partial
--        states from any number of parts in the background, and a reader
--        calling the matching -Merge combinator (argMinMerge,
--        groupUniqArrayMerge, ...) gets the fully-combined result regardless
--        of how many separate blocks contributed to it, whether or not
--        those parts have physically merged yet.
--
-- WHY THIS IS AN INCREMENTAL MV WITH ITS OWN STORAGE (not CREATE TABLE + TO):
--        `POPULATE` cannot be combined with an explicit `TO target_table`
--        clause in this ClickHouse version (fails with Code: 62). Giving the
--        MV its own `ENGINE = ...` clause instead lets it own its backing
--        table AND support POPULATE. POPULATE ran this SELECT once, at
--        creation time, over all 905,558 rows already in bronze_events_raw —
--        a DDL-native backfill, not a hand-run INSERT. Every new bronze
--        block fires the same query from that point on: true zero-gap
--        incremental update.
--
-- THE TRANSITION LOGIC (the `st` CASE expression) — this is where Q1
-- ("how do you define an active interval") is actually decided:
--        VideoSessionStart -> +1 (ACTIVE)      VideoSessionEnd -> -1 (CLOSE)
--        AppBackgrounded    ->  0 (INACTIVE)    AppForegrounded -> +1 (ACTIVE)
--        event = 'Play'     -> +1 (ACTIVE)      everything else -> NULL (no
--                                                 state change — this is
--                                                 where pause/resume/
--                                                 speed-pause/AdPause fall
--                                                 through as liveness-only
--                                                 signals, per the "pause is
--                                                 ACTIVE" assumption)
--
-- THE video_type FIX: coalesce(nullIf(vt_raw, ''), 'unk') — catches BOTH
--        NULL (join miss; there are none here, verified) AND empty string
--        (the real cause: 1,089 catalog rows with video_type=''). A plain
--        coalesce(vt_raw, 'unk') only catches NULL and was the original bug.
--
-- ENGINE: SharedAggregatingMergeTree, ORDER BY session_id — same locality
--        argument as any point-lookup table, but here the merge behavior is
--        what makes streaming correctness possible at all. Plain
--        ReplacingMergeTree cannot merge partial arrays across blocks; it
--        can only pick one whole row as the winner.
-- ============================================================================
CREATE MATERIALIZED VIEW mv_silver_session_state
(
    `session_id` FixedString(64),
    `platform_state` AggregateFunction(argMin, String, Int64),
    `country_state` AggregateFunction(argMin, String, Int64),
    `content_id_state` AggregateFunction(argMin, Int64, Int64),
    `video_type_state` AggregateFunction(argMin, String, Int64),
    `session_start_state` AggregateFunction(min, Int64),
    `last_seen_state` AggregateFunction(max, Int64),
    `has_close_state` AggregateFunction(max, UInt8),
    `live_ts_state` AggregateFunction(groupUniqArray, Int64),
    `tr_state` AggregateFunction(groupArrayIf, Tuple(Int64, Nullable(Int8)), UInt8)
)
ENGINE = SharedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY session_id
POPULATE AS
WITH tagged AS
(
    SELECT
        CAST(b.video_session_id, 'FixedString(64)') AS sid,
        b.event_timestamp AS ts,
        b.platform, b.country, b.content_id,
        c.video_type AS vt_raw,
        CASE b.event_type
             WHEN 'VideoSessionStart' THEN CAST(1  AS Int8)
             WHEN 'VideoSessionEnd'   THEN CAST(-1 AS Int8)
             WHEN 'AppBackgrounded'   THEN CAST(0  AS Int8)
             WHEN 'AppForegrounded'   THEN CAST(1  AS Int8)
             ELSE CASE WHEN b.event = 'Play' THEN CAST(1 AS Int8) ELSE NULL END
        END AS st
    FROM bronze_events_raw b
    LEFT JOIN bronze_content_raw c ON c.content_id = b.content_id
)
SELECT
    sid AS session_id,
    argMinState(platform, ts)                            AS platform_state,
    argMinState(country, ts)                             AS country_state,
    argMinState(content_id, ts)                           AS content_id_state,
    argMinState(coalesce(nullIf(vt_raw, ''), 'unk'), ts)   AS video_type_state,
    minState(ts)                                           AS session_start_state,
    maxState(ts)                                           AS last_seen_state,
    maxState(toUInt8(coalesce(st, toInt8(0)) = -1))         AS has_close_state,
    groupUniqArrayState(ts)                                  AS live_ts_state,
    groupArrayIfState(tuple(ts, st), st IS NOT NULL)          AS tr_state
FROM tagged
GROUP BY sid;


-- ============================================================================
-- 3. SILVER — silver_session_state_current (plain VIEW, zero storage)
--
-- WHAT:  reads mv_silver_session_state and applies every -Merge combinator,
--        producing the final arrays and scalars the interval algorithm needs.
-- WHY A VIEW, NOT A TABLE: costs nothing to store, always current — the
--        moment mv_silver_session_state gets a new partial state, this view
--        reflects it on the very next query. It exists purely to bridge the
--        "aggregate states" representation back to the "full arrays" shape
--        step 4's window-function algorithm needs, without ever
--        materializing a second copy of the data.
-- ============================================================================
CREATE VIEW silver_session_state_current AS
SELECT
    session_id,
    argMinMerge(platform_state)   AS platform,
    argMinMerge(country_state)    AS country,
    argMinMerge(content_id_state) AS content_id,
    argMinMerge(video_type_state) AS video_type,
    minMerge(session_start_state) AS session_start_ms,
    maxMerge(last_seen_state)     AS last_seen_ms,
    maxMerge(has_close_state)     AS has_close,
    arraySort(groupUniqArrayMerge(live_ts_state)) AS live_ts,
    arrayMap(x -> x.1, arraySort(x -> x.1, groupArrayIfMerge(tr_state))) AS tr_ts,
    arrayMap(x -> x.2, arraySort(x -> x.1, groupArrayIfMerge(tr_state))) AS tr_st
FROM mv_silver_session_state
GROUP BY session_id;


-- ============================================================================
-- 4. SILVER — silver_active_intervals (table)
--
-- WHAT:  N rows per session — one per contiguous foreground stretch,
--        [start_ms, end_ms), plus dims, is_open, and version=last_seen_ms.
-- WHY:   concurrency at minute M = count of intervals overlapping M. Once
--        state is reduced to half-open intervals, everything downstream is
--        trivial arithmetic. This is the "normalized intervals" option named
--        in the problem statement's solution directions.
-- ENGINE: ReplacingMergeTree(version) — every 15s refresh (see step 5)
--        recomputes the full interval set. For a session whose last_seen_ms
--        hasn't changed, re-emitting the same interval is a no-op at merge
--        time (same key, same version -> collapses to one row). For a
--        session with new activity, its rows get a bumped version and win
--        at merge — no rebuild, no rescan of unaffected sessions.
-- PARTITION: toYYYYMM(start_ms) — groups intervals by calendar month.
-- ORDER BY: (session_id, start_ms) — same locality argument as bronze;
--        also colocates rows for the JOIN in step 5's population query.
-- ============================================================================
CREATE TABLE silver_active_intervals
(
    session_id  FixedString(64),
    start_ms    Int64,
    end_ms      Int64,
    platform    LowCardinality(String),
    country     LowCardinality(String),
    content_id  Int64,
    video_type  LowCardinality(String),
    is_open     UInt8,
    version     Int64
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(start_ms))
ORDER BY (session_id, start_ms);


-- ============================================================================
-- 5. SILVER — mv_silver_active_intervals (REFRESHABLE MV — the one accepted gap)
--
-- WHY REFRESHABLE, NOT INCREMENTAL:
--        The interval-detection algorithm needs lagInFrame/leadInFrame
--        window functions over ALL of a session's transitions at once — a
--        whole-history computation, not a per-event one. An incremental MV
--        only ever sees the block that just landed and cannot run a window
--        function over a fragment. A refreshable MV reads the FULLY MERGED
--        silver_session_state_current view and runs the complete algorithm
--        each cycle. Cost: any interval touched by activity in the last
--        cycle may lag by up to REFRESH EVERY 15 SECOND. This is the one
--        place in the whole pipeline we accept staleness, because it's the
--        one place ClickHouse structurally cannot give zero-gap automation
--        for a whole-session-history algorithm.
--
-- THE ALGORITHM (three sources of "state at time t", unioned):
--   tr    -- explicit transitions (ARRAY JOIN tr_ts, tr_st). BG=0, FG=1, close=-1.
--   gaps  -- for every consecutive live_ts pair > 50s apart, emit a synthetic
--            BG at prev+50s and a synthetic FG at the next live_ts. This is
--            what handles "heartbeat missing" without an explicit BG event.
--            The 50s threshold is empirically derived: the gap histogram
--            collapses 137x at 50s (100,934 gaps in the 40-50s bucket vs 737
--            in 50-60s), and P(gap contains a real labelled background)
--            jumps from 0.5% to 50.6% at the same point.
--   wm    -- for sessions with has_close=0, emit -1 at last_seen_ms + 50s.
--            The watermark closure — deterministic snapshot boundary for
--            open sessions (dead code on this dataset: 0 open sessions here,
--            load-bearing for the unseen day).
--
--   The three are deduped on (session_id, ts) by min(st) (transitions win
--   over gaps at the same ts), reduced to state-change rows via a windowed
--   lag (kills duplicate BG->BG / FG->FG idempotency violations), then
--   adjacent state-change rows (a, st) -> (b, next_st) form the interval
--   [a, b). Keep only st = 1 (foreground) rows.
--
-- TWO CLICKHOUSE-SPECIFIC FIXES (both silent failures if missed):
--   1. leadInFrame(ts) needs an explicit forward frame:
--      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING. ClickHouse's
--      default frame (RANGE UNBOUNDED PRECEDING..CURRENT ROW) excludes the
--      next row, so lead always returns the default (0). Missing this
--      produced 0 rows, no error.
--   2. CTEs (WITH x AS (...)) are inlined in the outer SELECT — x.column is
--      not a valid reference. Alias in FROM: `FROM ivl AS i`, use `i.column`.
-- ============================================================================
CREATE MATERIALIZED VIEW mv_silver_active_intervals
REFRESH EVERY 15 SECOND
TO silver_active_intervals AS
WITH
    50000 AS gap_ms,
    tr AS (
        SELECT session_id, ts, st
        FROM silver_session_state_current
        ARRAY JOIN tr_ts AS ts, tr_st AS st
    ),
    lp AS (
        SELECT session_id, ts,
               lagInFrame(ts) OVER (PARTITION BY session_id ORDER BY ts) AS prev
        FROM ( SELECT session_id, live_ts FROM silver_session_state_current )
        ARRAY JOIN live_ts AS ts
    ),
    gaps AS (
        SELECT session_id, prev + gap_ms AS ts, CAST(0 AS Int8) AS st FROM lp WHERE prev > 0 AND ts - prev > gap_ms
        UNION ALL
        SELECT session_id, ts,                  CAST(1 AS Int8) AS st FROM lp WHERE prev > 0 AND ts - prev > gap_ms
    ),
    wm AS (
        SELECT session_id, last_seen_ms + gap_ms AS ts, CAST(-1 AS Int8) AS st
        FROM silver_session_state_current
        WHERE has_close = 0
    ),
    allt AS (
        SELECT session_id, ts, min(st) AS st FROM (
            SELECT session_id, ts, st FROM tr
            UNION ALL SELECT session_id, ts, st FROM gaps
            UNION ALL SELECT session_id, ts, st FROM wm
        )
        GROUP BY session_id, ts
    ),
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
    ivl AS (
        SELECT session_id, ts AS a, st,
               leadInFrame(ts) OVER (PARTITION BY session_id ORDER BY ts
                                     ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS b
        FROM changed
    )
SELECT
    i.session_id, i.a AS start_ms, i.b AS end_ms,
    s.platform, s.country, s.content_id, s.video_type,
    CASE WHEN s.has_close = 0 THEN 1 ELSE 0 END AS is_open,
    s.last_seen_ms AS version
FROM ivl AS i
INNER JOIN silver_session_state_current s ON s.session_id = i.session_id
WHERE i.st = 1 AND i.b > 0 AND i.b > i.a;


-- ============================================================================
-- 6. GOLD — gold_concurrency_minute (G1, table)
--
-- WHAT:  one row per (minute, platform, country, video_type, content_id),
--        with cnt_a and cnt_b stored as AggregateFunction(uniqExact,
--        FixedString(64)) — a DISTINCT-SESSION-ID state, read via
--        uniqExactMerge(...), never as a raw counter.
-- WHY:   this is the primary serving table — the one nearly every benchmark
--        question reads. Pre-aggregated to minute x dimension grain so peak/
--        average queries never touch session-level data.
-- WHY AggregatingMergeTree + uniqExactState, NOT SummingMergeTree + sum(1):
--        a session that toggles FG->BG->FG inside the same minute produces
--        more than one interval touching that minute. A plain sum(1) counts
--        it twice — this was a real bug hit and fixed (inflated peak by
--        +3.4% on cnt_a, +23% on cnt_b before the fix). uniqExactState
--        counts the session once regardless of how many intervals it
--        contributes in that minute, by construction. Verified directly:
--        uniqExactMerge(cnt_a) at the peak minute equals countDistinct
--        (session_id) computed straight from silver_active_intervals for
--        that same minute.
-- ORDER BY: (country, video_type, platform, content_id, minute) —
--        highest-selectivity-per-byte dims first, so filtered benchmark
--        queries prune before touching most of the table; minute last since
--        time-range filters typically span many rows regardless.
-- ============================================================================
CREATE TABLE gold_concurrency_minute
(
    minute      DateTime,
    platform    LowCardinality(String),
    country     LowCardinality(String),
    video_type  LowCardinality(String),
    content_id  Int64,
    cnt_a       AggregateFunction(uniqExact, FixedString(64)),
    cnt_b       AggregateFunction(uniqExact, FixedString(64))
)
ENGINE = SharedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(minute)
ORDER BY (country, video_type, platform, content_id, minute);


-- ============================================================================
-- 7. GOLD — mv_gold_concurrency_minute (refreshable MV, TO #6)
--
-- WHY REFRESHABLE, DEPENDS ON, NOT INCREMENTAL:
--        this MV's source (silver_active_intervals) is itself refreshable.
--        A refreshable MV's refresh is an atomic full-result swap, not a
--        stream of ordinary INSERT blocks — a plain incremental MV chained
--        on top of it would not reliably fire. Making this MV refreshable
--        too, and sequencing it with DEPENDS ON mv_silver_active_intervals
--        (rather than a separate wall-clock schedule), guarantees gold
--        always recomputes right after intervals, never on a stale or
--        half-updated interval set.
--
-- cnt_a branch: ARRAY JOIN range(ceil(start/60), ceil(end/60)) — interval
--        FULLY COVERS the minute instant (concurrency convention).
-- cnt_b branch: ARRAY JOIN range(floor(start/60), floor((end-1)/60)+1) —
--        interval TOUCHES the minute at all (reach convention).
-- Both computed in one UNION ALL, deduped by uniqExactStateIf(session_id,
-- kind = 'a'|'b') in the outer GROUP BY.
-- ============================================================================
CREATE MATERIALIZED VIEW mv_gold_concurrency_minute
REFRESH EVERY 15 SECOND DEPENDS ON mv_silver_active_intervals
TO gold_concurrency_minute AS
SELECT
    minute, platform, country, video_type, content_id,
    uniqExactStateIf(session_id, kind = 'a') AS cnt_a,
    uniqExactStateIf(session_id, kind = 'b') AS cnt_b
FROM
(
    SELECT
        toDateTime(minute_id * 60) AS minute,
        platform, country, video_type, content_id, session_id, 'a' AS kind
    FROM silver_active_intervals
    ARRAY JOIN range(
        toUInt32(ceil(start_ms / 60000.)),
        toUInt32(ceil(end_ms   / 60000.))
    ) AS minute_id
    WHERE end_ms > start_ms

    UNION ALL

    SELECT
        toDateTime(minute_id * 60) AS minute,
        platform, country, video_type, content_id, session_id, 'b' AS kind
    FROM silver_active_intervals
    ARRAY JOIN range(
        toUInt32(intDiv(start_ms, 60000)),
        toUInt32(intDiv(end_ms - 1, 60000)) + 1
    ) AS minute_id
    WHERE end_ms > start_ms
)
GROUP BY minute, platform, country, video_type, content_id;


-- ============================================================================
-- 8. GOLD — gold_concurrency_delta (G2, table)
--
-- WHAT:  one row per (minute, platform, country, video_type, content_id,
--        delta_kind), with a single signed delta (+1/-1) — NO session_id in
--        the schema at all. Two rows per interval per boundary convention
--        (delta_kind='a' matches G1's cnt_a convention, 'b' matches cnt_b).
--        Concurrency at any minute = running cumsum of delta up to that
--        minute.
-- WHY:   the problem statement explicitly asks for a session-independent
--        representation AND asks that both approaches be compared. Building
--        G2 without a session key is the actual point — if the anonymous
--        +1/-1 cumsum matches G1's distinct-session count, that's the
--        strongest form of cross-validation the two models can offer each
--        other. This is also the "interval-to-delta model" the problem
--        statement names as a possible solution direction.
-- WHY SummingMergeTree, NOT ReplacingMergeTree: no distinct-counting is
--        needed here (that's G1's job) — G2 only ever sums signed integers,
--        exactly what SummingMergeTree folds for free at merge time.
--        Dropping session_id from the schema also removes the only reason
--        an earlier draft needed ReplacingMergeTree + FINAL reads.
-- ============================================================================
CREATE TABLE gold_concurrency_delta
(
    minute      DateTime,
    platform    LowCardinality(String),
    country     LowCardinality(String),
    video_type  LowCardinality(String),
    content_id  Int64,
    delta_kind  LowCardinality(FixedString(1)),
    delta       Int64
)
ENGINE = SharedSummingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(minute)
ORDER BY (country, video_type, platform, content_id, minute, delta_kind);


-- ============================================================================
-- 9. GOLD — mv_gold_concurrency_delta (refreshable MV, TO #8, DEPENDS ON #5)
--
-- Same DEPENDS ON reasoning as step 7. Four branches, one per (kind,
-- boundary) pair:
--   kind='a' open  -- ceil(start/60)         kind='a' close -- ceil(end/60)
--   kind='b' open  -- floor(start/60)        kind='b' close -- floor((end-1)/60)+1
-- SummingMergeTree folds identical (minute, dims, delta_kind) rows by
-- summing at merge time; each interval contributes exactly one +1 and one -1
-- per convention, so the sum is exact without ever carrying a session key.
-- ============================================================================
CREATE MATERIALIZED VIEW mv_gold_concurrency_delta
REFRESH EVERY 15 SECOND DEPENDS ON mv_silver_active_intervals
TO gold_concurrency_delta AS
SELECT minute, platform, country, video_type, content_id, delta_kind, sum(delta) AS delta
FROM
(
    SELECT toDateTime(toUInt32(ceil(start_ms / 60000.)) * 60) AS minute,
           platform, country, video_type, content_id,
           'a' AS delta_kind, toInt64(1) AS delta
    FROM silver_active_intervals WHERE end_ms > start_ms
    UNION ALL
    SELECT toDateTime(toUInt32(ceil(end_ms / 60000.)) * 60),
           platform, country, video_type, content_id,
           'a', toInt64(-1)
    FROM silver_active_intervals WHERE end_ms > start_ms

    UNION ALL

    SELECT toDateTime(toUInt32(intDiv(start_ms, 60000)) * 60),
           platform, country, video_type, content_id,
           'b', toInt64(1)
    FROM silver_active_intervals WHERE end_ms > start_ms
    UNION ALL
    SELECT toDateTime(toUInt32(intDiv(end_ms - 1, 60000) + 1) * 60),
           platform, country, video_type, content_id,
           'b', toInt64(-1)
    FROM silver_active_intervals WHERE end_ms > start_ms
)
GROUP BY minute, platform, country, video_type, content_id, delta_kind;
