# Pipeline Logic — SonyLIV Foreground-Only Concurrency

> **Companion doc.** This file explains *every* table in the pipeline: what it does, why it exists, how it is populated, which MergeTree engine it uses (and why), and what it contributes to the overall problem.
> **Grounds:** [`SonyLiv/PROBLEM_STATEMENT.md`](SonyLiv/PROBLEM_STATEMENT.md), [`SonyLiv/dataset_details.md`](SonyLiv/dataset_details.md).
> **Sister docs:** `README.md` (architecture diagram, assumptions), `pipeline/04_ddl_annotated.sql` (the exact live DDL — every `CREATE TABLE`/`CREATE MATERIALIZED VIEW` statement, one-to-one with `SHOW CREATE TABLE` on the running cloud instance, annotated inline with the same why-we-built-it-this-way reasoning as this file), `LLM_QUERY_GUIDE.md` (query templates for a conversational layer).

---

## 0. What we are actually solving

The problem statement is blunt about the failure mode: **"overcounting backgrounded time is the failure mode this whole problem exists to prevent."** A `video_session_id` being open is not enough — we must know the session was in the foreground *and* still emitting heartbeats. The pipeline is layered so each layer removes one class of noise:

```
bronze_events_raw  (Kafka → ClickPipes, SharedMergeTree)
        │
        │  ① INCREMENTAL MV, own storage, POPULATE backfilled once at CREATE.
        │     Fires per event block from this point on. Zero gap, ever.
        ▼
silver_session_state         AggregatingMergeTree  (argMin/min/max/groupArray STATES)
        │
        │  plain VIEW, zero storage — merges states with -Merge combinators on read
        ▼
silver_session_state_current
        │
        │  ② REFRESHABLE MV, REFRESH EVERY 15 SECOND.
        │     The one accepted staleness window — see §2.3 for why it's unavoidable here.
        ▼
silver_active_intervals      ReplacingMergeTree(version=last_seen_ms)
        │
        │  ③ REFRESHABLE MVs, DEPENDS ON ②  — sequenced, never read a half-updated interval set
        ▼
gold_concurrency_minute (G1) AggregatingMergeTree — uniqExactState(session_id)
gold_concurrency_delta  (G2) SummingMergeTree — slim, no session_id
        │
        ▼
                  Dashboards / MCP
```

Every table from `silver_session_state` onward is populated **entirely by ClickHouse's own MV mechanisms** — no manual `INSERT ... SELECT` anywhere in the running pipeline. Bronze arrives (via ClickPipes or replay) and the rest follows automatically, either instantly (①) or on a bounded 15-second refresh (②, ③).

Each layer addresses at least one of the five problem-statement questions (Q1: active-range definition, Q2: representation, Q3: peak+avg without rescanning, Q4: filter-friendly across dims, Q5: open-session updates).

---

## 1. Bronze layer — raw truth

### 1.1 `bronze_events_raw` (Kafka → ClickPipes → SharedMergeTree)

**What.** The append-only event log. Every row is one `event_type` emission from a video session: `VideoSessionStart`, `VideoPlay`, `VideoHeartbeat`, `AppBackgrounded`, `AppForegrounded`, `VideoSessionEnd`, `VideoError`. Plus Kafka meta (`_partition`, `_offset`, `_topic`, `_key`, `_timestamp`, `_raw_message`).

**Why.** Immutable truth. Every silver/gold number must be reproducible from this table. If a downstream table gives a wrong answer, we replay from bronze. The Kafka meta columns are load-bearing for observability — ClickStack reads `_timestamp` to compute ingestion lag.

**How populated.** Streaming ingest via ClickPipes. Producer key is `video_session_id`, so all events for one session land on the same Kafka partition (crucial for stateful downstream processing).

**Engine — `SharedMergeTree`.** ClickHouse Cloud's default replacement for `MergeTree`; separates storage from replication (S3-backed, coordinated via `{uuid}/{shard}`). No downside vs `MergeTree` for hackathon-scale writes.
Why not `ReplacingMergeTree` on bronze: we WANT duplicates preserved so gold can detect them (13 dup Starts, 14 dup Ends already found in this dataset).

**Partition — `toStartOfHour(session_start_epoch/1000)`.** Two reasons:
1. Prunes queries by session-start hour (a common filter for post-hoc analysis).
2. Hackathon dataset is entirely in ~2 days but 94% in a single hour — hourly partitioning gives ~50 partitions instead of 1, which speeds parallel merges. For production, switch to `toYYYYMM(event_ts)` to stay under 100 partitions/table.

**ORDER BY — `(video_session_id, event, event_timestamp)`.** Groups every session's events contiguously on disk, so the silver sessionizer reads a session's rows in one seek. Secondary key `event` lets us skip past irrelevant event types (e.g., `event_type='VideoHeartbeat' AND event='pause'`) without scanning.

**What it achieves.** Answers **nothing** on its own about concurrency. It just guarantees we can reconstruct any state at any time. Query latency here is not the point — layered gold is.

### 1.2 `bronze_content_raw` (catalog stream → SharedMergeTree)

**What.** The content dimension table: `content_id → (title, video_type, category)`.

**Why.** `video_type` is a filter dimension in the benchmark queries. Denormalising it into every silver interval avoids repeated joins.

**Engine — `SharedMergeTree`, `ORDER BY content_id`.** Table is small (~33K rows). Point lookup is O(log n) on the primary index. In pure query terms we would prefer a `Dictionary(HASHED)` layer on top for O(1) lookups — the design doc calls this out and it's the next step.

**Partition — none.** Nothing to prune; the table is one part.

**What it achieves.** Turns `content_id` into `video_type` in the session-state builder in one pass.

### 1.3 Fixture replicas — `ch_hackathon_raw_data`, `ch_hackathon_content_data`

**What.** One-shot CSV loads mirroring the Kafka-fed tables. `ORDER BY tuple()` means unsorted.

**Why.** Two independent sources of truth for the same rows:
- If bronze via ClickPipes shows fewer events than the CSV replica, we know the streaming path lost data.
- Lets us bootstrap silver even when ClickPipes is offline.

**Note.** `bronze_content_raw` has 33,469 rows but the CSV `ch_hackathon_content_data` has 33,464 — a 5-row delta worth auditing (likely dup content_ids landed twice through the Kafka path).

---

## 2. Silver layer — the actual model

This is where the problem is solved. Every design decision here maps to a problem-statement question, and every table is populated by a ClickHouse-native mechanism — no manual INSERT.

### 2.1 `silver_session_state` (AggregatingMergeTree, one row per session, incremental MV)

**What.** *One row per `video_session_id`*, but unlike a plain table, every column is an **aggregate function state**, not a final value:

| Column | Type | Meaning |
|---|---|---|
| `session_id` | FixedString(64) | ORDER BY key |
| `platform_state`, `country_state`, `content_id_state`, `video_type_state` | `AggregateFunction(argMin, ..., Int64)` | dims, pinned to earliest event by timestamp |
| `session_start_state` | `AggregateFunction(min, Int64)` | envelope start |
| `last_seen_state` | `AggregateFunction(max, Int64)` | envelope end |
| `has_close_state` | `AggregateFunction(max, UInt8)` | 1 iff a `VideoSessionEnd` was ever seen |
| `live_ts_state` | `AggregateFunction(groupUniqArray, Int64)` | all "alive" timestamps |
| `tr_state` | `AggregateFunction(groupArrayIf, Tuple(Int64,Int8), UInt8)` | explicit state transitions, filtered to non-null `st` |

**Why states, not arrays.** This is the crux of making the pipeline *actually* incremental instead of batch-rebuilt. A session's heartbeats arrive across **many separate bronze INSERT blocks** over its lifetime — sometimes minutes apart. An incremental MV only ever sees the block that just landed, never the session's full history. If the target table stored plain arrays, each MV firing would only have that block's fragment, and the array would never represent "everything we know about this session."

`AggregateFunction` states solve this by being **associative and mergeable**: `AggregatingMergeTree` merges partial states from different parts in the background, and any reader that calls the matching `-Merge` combinator (`argMinMerge`, `groupUniqArrayMerge`, etc.) gets the fully-combined result *regardless of how many separate blocks contributed to it* — whether those blocks have been physically merged yet or not. This is the one ClickHouse primitive built precisely for "many small updates over time, read as one coherent value."

**How populated — genuinely incremental, zero gap.** The MV is defined with its **own storage** (`ENGINE = AggregatingMergeTree ... POPULATE AS SELECT ...`), not a separate `CREATE TABLE` + `TO`. Two reasons:
1. **`POPULATE` cannot be combined with `TO target_table`** in this ClickHouse version (`Code: 62`) — only with an inline `ENGINE = ...` clause where the MV owns its backing table.
2. `POPULATE` runs the MV's `SELECT` once, at creation time, over every row already in `bronze_events_raw` — a **DDL-native backfill**, not a hand-run `INSERT ... SELECT`. From that moment forward, every new bronze block fires the same query incrementally.

The SELECT itself (unchanged from the original design):
```sql
CASE b.event_type
     WHEN 'VideoSessionStart' THEN CAST(1  AS Int8)
     WHEN 'VideoSessionEnd'   THEN CAST(-1 AS Int8)
     WHEN 'AppBackgrounded'   THEN CAST(0  AS Int8)
     WHEN 'AppForegrounded'   THEN CAST(1  AS Int8)
     ELSE CASE WHEN b.event = 'Play' THEN CAST(1 AS Int8) ELSE NULL END
END AS st
...
argMinState(coalesce(nullIf(vt_raw, ''), 'unk'), ts)  AS video_type_state,
...
groupArrayIfState(tuple(ts, st), st IS NOT NULL)      AS tr_state
```

**The `video_type` bug (found and fixed).** The first version used `coalesce(vt_raw, 'unk')`, which only replaces `NULL`. `bronze_content_raw` has **1,089 rows with `video_type = ''`** (empty string, a genuine data-quality issue in the catalog — confirmed by checking `bronze_content_raw` directly, and confirming **zero** join misses between events and catalog). An empty string is not `NULL`, so it passed through untouched, surfacing as blank `video_type` in every downstream table (250 sessions → 605 intervals → 3,691 gold rows). Fix: `coalesce(nullIf(vt_raw, ''), 'unk')` — normalize empty string to `NULL` first, then coalesce. Verified end-to-end after rebuild: all three layers now show `unk` instead of `''`.

**Engine — `AggregatingMergeTree` ORDER BY `session_id`.** Same locality argument as before (point lookups, full-scan rebuilds), but now the merge behaviour is what makes streaming correctness possible at all — plain `ReplacingMergeTree` cannot merge partial arrays across blocks; it can only pick one whole row as the winner.

**No partition.** Same as before — nothing to prune cleanly on this table.

**What it achieves.** Q1 (define active), Q2 (representation = aggregate states), **Q5 done properly this time** — updates absorbed with true zero gap, not "collapse on next batch."

### 2.2 `silver_session_state_current` (plain VIEW, zero storage)

**What.** A regular (non-materialized) `VIEW` that reads `silver_session_state` and applies every `-Merge` combinator, producing the final arrays and scalars — exactly the shape the old plain table used to have.

```sql
CREATE VIEW silver_session_state_current AS
SELECT
    session_id,
    argMinMerge(platform_state)   AS platform,
    ...
    arraySort(groupUniqArrayMerge(live_ts_state)) AS live_ts,
    arrayMap(x -> x.1, arraySort(x -> x.1, groupArrayIfMerge(tr_state))) AS tr_ts,
    arrayMap(x -> x.2, arraySort(x -> x.1, groupArrayIfMerge(tr_state))) AS tr_st
FROM silver_session_state
GROUP BY session_id;
```

**Why a VIEW and not a table.** It costs nothing to store and is always current — the moment `silver_session_state` gets a new partial state, this view reflects it on the very next query. It exists purely to give the next layer (which genuinely needs the *whole* session at once) a clean read surface.

**What it achieves.** Bridges Q2 (states) back to the shape Q1's window-function algorithm needs (full arrays), without ever materializing a second copy of the data.

### 2.3 `silver_active_intervals` (ReplacingMergeTree, refreshable MV — the one accepted gap)

**What.** *N rows per session*, one per contiguous foreground stretch: `[start_ms, end_ms)` plus dims, `is_open`, and a `version` column (`= last_seen_ms`).

**Why this is the one place we accept staleness.** The interval-detection algorithm needs `lagInFrame`/`leadInFrame` window functions running over **all of a session's transitions at once** — that's fundamentally a whole-history computation, not a per-event one. Two ClickHouse mechanisms exist for "keep a table in sync automatically":

| Mechanism | Sees | Works here? |
|---|---|---|
| Incremental MV | Only the just-inserted block | **No** — a window function over "all transitions" can't run on a fragment. |
| Refreshable MV | The full result of a query, re-run on a schedule | **Yes** — reads `silver_session_state_current` (already fully merged) and runs the complete window-function chain each time. |

The cost is honest: any interval affected by activity in the last refresh window is stale by up to that window's length. We set the window to **15 seconds** — the same cadence discussed for MV freshness earlier in this project, chosen because a live-event ramp-up settles within single-digit seconds, so 15s bounds worst-case staleness without meaningfully lagging a real dashboard.

**How populated — the algorithm** (unchanged from the original design, now reading from the view instead of a stored table):

The active-state timeline is built by unioning three sources of "state at time t":

1. **`tr`** — explicit transitions (`ARRAY JOIN tr_ts, tr_st` on `silver_session_state_current`). Values: BG=0, FG=1, close=-1.
2. **`gaps`** — for every consecutive pair of `live_ts` more than 50s apart, emit a synthetic BG at `prev+50s` and a synthetic FG at the next live_ts. This is what handles **"heartbeat missing"** without an explicit BG event (Q1). The 50s threshold was empirically derived (labelled-BG probability jumps from 0.5% below 50s to 50.6% above).
3. **`wm`** — for sessions with `has_close=0`, emit `-1` at `last_seen_ms + 50s`. This is the watermark closure — deterministic snapshot boundary. (In this dataset every session has `has_close=1`, so `wm` is empty; the code path is load-bearing for the unseen day.)

The three sources are UNION-ed, deduped on `(session_id, ts)` by `min(st)` (transitions win over gaps at the same ts), then reduced to state-change rows via a windowed lag. Adjacent state-change rows `(a, st) → (b, next_st)` form the interval `[a, b)`. We keep only those where `st = 1` (foreground).

**Two ClickHouse-specific fixes vs the DuckDB reference (both load-bearing, both silent failures if missed):**
1. `leadInFrame(ts)` on the closing timestamp needs an explicit forward frame: `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`. ClickHouse's default frame is `RANGE UNBOUNDED PRECEDING..CURRENT ROW`, which excludes the *next* row and makes `lead` always return the default (0). Missing this produced **0 rows**, no error — the query ran "successfully" and silently computed nothing.
2. CTEs defined as `WITH x AS (SELECT ...)` are **inlined** in the outer SELECT; you cannot reference `x.column`. Alias in the FROM clause: `FROM ivl AS i`, use `i.column`. Missing this raises `Unknown expression identifier`.

**Engine — `ReplacingMergeTree(version)`, `version = last_seen_ms`.** Every 15s refresh recomputes the full interval set, including for sessions whose `last_seen_ms` hasn't changed. `ReplacingMergeTree` means re-emitting an unchanged interval is a no-op at merge time (same key, same version, collapses to one row) — cheap. When a session gets a new heartbeat, its interval row(s) get a bumped `version` and win at merge.

**Partition — `toYYYYMM(fromUnixTimestamp64Milli(start_ms))`.** Groups intervals by calendar month.

**ORDER BY — `(session_id, start_ms)`.** Same locality argument as bronze.

**What it achieves.** Q1 (active-interval definition, materialised), Q2 (normalized intervals), Q4 (denormalised dims → no runtime joins downstream), Q5 (bounded, known staleness rather than a full rebuild).

---

## 3. Gold layer — the serving surface

Gold is what dashboards read. Two flavours, both derived from `silver_active_intervals` via **incremental-safe refreshable MVs** (`DEPENDS ON` the intervals refresh, not their own clock), addressing the two representations the problem statement calls out ("session-aware or session-independent").

**Why refreshable, not incremental, for gold too.** Both G1 and G2 read `silver_active_intervals`, which is itself refreshable. A refreshable MV's refresh is an atomic full-result swap, not a stream of ordinary `INSERT` blocks — so a plain incremental MV chained on top of it would not reliably fire. Making gold refreshable *and* sequencing it with `DEPENDS ON mv_silver_active_intervals` sidesteps this: gold always recomputes right after intervals, never on a separate clock, never on a half-updated interval set.

### 3.1 `gold_concurrency_minute` — G1 (session-aware, AggregatingMergeTree)

**What.** One row per `(minute, platform, country, video_type, content_id)` with `cnt_a` and `cnt_b` stored as `AggregateFunction(uniqExact, FixedString(64))` — a **distinct-session-id state**, not a raw counter.

- `cnt_a` = number of *distinct sessions* whose foreground range **fully covers** that minute instant (`ceil(start/60) ≤ M < ceil(end/60)`). **This is concurrency.**
- `cnt_b` = number of *distinct sessions* that **touch** that minute at all (`floor(start/60) ≤ M ≤ floor((end-1)/60)`). This is reach.

**The double-count bug (found and fixed).** The first build used `SummingMergeTree` with a plain `sum(1)` per exploded (minute, dims) row. A session that toggles FG→BG→FG *inside the same minute* produces more than one interval touching that minute, and `sum(1)` counts it twice. This inflated peak `cnt_a` by +3.4% and `cnt_b` by +23% against the DuckDB oracle (which does `SELECT DISTINCT session_id` before counting). Fixed by switching to `AggregatingMergeTree` with `uniqExactState(session_id)` — the state that's *built* for "count distinct things across many small updates." Verified directly: `uniqExactMerge(cnt_a)` at the peak minute now equals `countDistinct(session_id)` computed straight from `silver_active_intervals` for that same minute — gold is provably correct relative to its own source. (A residual ~3% gap vs the DuckDB oracle remains, but it's upstream — the interval-count itself is ~8% lower than the oracle's, a separate open question, not a gold-layer bug.)

**How populated.** Two `UNION ALL` branches (concurrency, reach) inside one refreshable MV, each doing `ARRAY JOIN range(...)` to explode an interval into the minutes it covers, then `GROUP BY minute, dims` with `uniqExactStateIf(session_id, kind = 'a' | 'b')`.

**Engine — `AggregatingMergeTree`.** Chosen over `SummingMergeTree` specifically *because* of the double-count risk — `uniqExact` state merges correctly no matter how many overlapping rows exist per session per minute; a plain `sum` cannot express "distinct" at all.

**Partition — `toYYYYMM(minute)`.** Monthly, ≤12 partitions/year at petabyte scale.

**ORDER BY — `(country, video_type, platform, content_id, minute)`.** Highest-selectivity-per-byte dims first, so benchmark filters like `WHERE country='IN' AND video_type='live'` prune fast; `minute` last since time-range filters span many rows anyway.

**Peak query pattern (Q3).**
```sql
SELECT max(c) peak, avg(c) avg_conc, argMax(minute, c) peak_min
FROM ( SELECT minute, uniqExactMerge(cnt_a) AS c
       FROM gold_concurrency_minute GROUP BY minute )
```

**What it achieves.** Q3 (peak+avg without rescan, now *correctly*), Q4 (dim filters cheap). Q5 is handled by the refresh cadence in §2.3, not by G1 itself.

### 3.2 `gold_concurrency_delta` — G2 (session-independent, slim SummingMergeTree)

**What.** One row per `(minute, platform, country, video_type, content_id, delta_kind)` with a single signed `delta` counter — **no `session_id` in the schema at all**. Two deltas per interval per convention: `+1` at open, `-1` at close. Concurrency at any minute = running cumulative sum of `delta` up to that minute.

**Why slim, and why no session_id.** The whole point of a session-*independent* model is that it proves the peak without ever carrying session identity forward — if the cumsum of anonymous +1/-1 events matches G1's distinct-session count, that's the strongest form of cross-validation the two approaches can offer each other. Carrying `session_id` (as an earlier draft did) doesn't buy correctness here — `SummingMergeTree` already folds identical `(minute, dims, kind)` rows by summing, and each interval contributes exactly one `+1` and one `-1`, so the sum is exact without needing a session key. Dropping it shrinks the table (~2K distinct minute/dims/kind rows instead of ~91K session-keyed rows) and removes the only thing that would have forced `ReplacingMergeTree` + `FINAL` reads.

**How populated.** Four branches inside one refreshable MV: `(kind='a', open)`, `(kind='a', close)`, `(kind='b', open)`, `(kind='b', close)` — `kind='a'` uses ceil/ceil boundaries (matches G1's concurrency convention), `kind='b'` uses floor/ceil (matches reach).

**Engine — `SummingMergeTree`.** No distinct-counting needed here (that's G1's job) — G2 only ever sums signed +1/-1, which is exactly what `SummingMergeTree` folds at merge time for free.

**ORDER BY — `(country, video_type, platform, content_id, minute, delta_kind)`.** Same dim-selectivity ordering as G1, `delta_kind` last to keep the two conventions physically adjacent.

**Peak query pattern (Q3).**
```sql
SELECT max(live) peak, avg(live) avg_live, argMax(minute, live) peak_min
FROM ( SELECT minute,
              sum(delta) OVER (ORDER BY minute
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS live
       FROM gold_concurrency_delta WHERE delta_kind = 'a' )
```

**What it achieves.** Q2 (session-independent alternative representation, proven not just claimed), Q3 (peak still fast — one window-sum over a few thousand rows), and the explicit "compare both approaches" ask in `README_START_HERE.md` — G1 and G2 are built from the same source by construction, so any divergence between them is a real signal, not noise.

---

## 4. Serving choices — when to read what

| Query | Read from | Reason |
|---|---|---|
| Peak/avg on **closed** history, filter by dims | G1 | `uniqExactMerge` over pruned parts — correct and cheap. |
| Peak/avg **including live open sessions** | G2 | Cumsum over deltas reflects the watermark row as soon as it's refreshed. |
| Cross-model validation ("did both models agree?") | G1 + G2 | The comparison is the evaluation criterion, and both are built from the same source. |
| Session-level drill-down ("which sessions were live at 16:04?") | `silver_active_intervals WHERE start_ms ≤ M AND end_ms > M` | Gold has no session_id (G1 only carries it as an aggregate state; G2 doesn't carry it at all). |
| Replay from scratch (schema change) | `bronze_events_raw` | Only bronze is the source of truth. |

**Hybrid tiering (problem-statement direction).** Hot minutes (last 15s refresh window) → live query against `silver_session_state_current` for exactness; warm minutes (today) → G1 or G2; cold minutes (>24h) → G1 (pre-aggregated, cheapest per byte).

---

## 5. Why this shape wins the evaluation

The evaluation criteria (from `PROBLEM_STATEMENT.md`):

| Criterion | This design's answer |
|---|---|
| **Correct** — foreground-only, matches ground truth | 50s gap → BG classification is empirically derived. Pause treated as ACTIVE. `uniqExact` in G1 eliminates the double-count bug that inflated peak by up to +23% in an earlier build; verified gold matches its own source (`silver_active_intervals`) exactly. |
| **Fast** — dashboard latency with filters | Gold ORDER BY prefix `(country, video_type, platform, content_id, minute)` prunes benchmark queries to a small fraction of the table. |
| **Update-friendly** — open sessions absorbing heartbeats | `silver_session_state` updates with **zero gap** via true incremental MV + AggregateFunction states. Intervals and gold absorb updates within a **bounded 15s window** via refreshable MVs chained with `DEPENDS ON` — never a full rebuild, never a stale gold read against a half-updated interval set. |
| **Explained** — trade-off thinking | This file plus `README.md` and `LLM_QUERY_GUIDE.md`. Every engine choice justified against alternatives, including the two real ClickHouse constraints that shaped the final design: incremental MVs can't see cross-block history, and refreshable MVs don't chain into incremental MVs via ordinary INSERT semantics. |
| **Unseen day** — build for what you don't see | Watermark logic in `wm` CTE is dead code on this dataset (`has_close=1` for all 10,866 sessions) but load-bearing on the unseen day. Boundary-convention duality (`cnt_a`/`cnt_b`, `kind='a'`/`'b'`) hedges the private answer key. `POPULATE` on the session-state MV means the unseen day's data flows through the identical incremental path already proven on this dataset. |

---

## 6. Cheat sheet — one-line-per-table

```
bronze_events_raw            SharedMergeTree          immutable event log,                bronze
bronze_content_raw           SharedMergeTree          dim table,                          bronze
silver_session_state         AggregatingMergeTree     agg-fn states per session,           Q1+Q2+Q5 (zero-gap incremental MV, POPULATE-backfilled)
silver_session_state_current VIEW (no storage)        merges states on read,               bridges Q2 states -> Q1 arrays
silver_active_intervals      ReplacingMergeTree        [start,end) intervals,               Q1+Q2+Q4 (refreshable MV, 15s bound)
gold_concurrency_minute (G1) AggregatingMergeTree     uniqExact(session) x minute x dims,   Q3+Q4 (refreshable, DEPENDS ON intervals)
gold_concurrency_delta  (G2) SummingMergeTree          ±1 deltas, no session_id,             Q2+Q3 (refreshable, DEPENDS ON intervals)
```

---

## 7. ClickHouse gotchas encountered — all silent, all load-bearing

These are documented here because every one of them **failed silently** — no error, wrong or empty result — which makes them expensive to rediscover.

| # | Symptom | Cause | Fix |
|---|---|---|---|
| 1 | `silver_active_intervals` insert returns 0 rows, no error | `leadInFrame`'s default window frame (`RANGE UNBOUNDED PRECEDING..CURRENT ROW`) excludes the next row, so `lead` always returns the default | Explicit `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` |
| 2 | `Unknown expression identifier ivl.session_id` | CTEs (`WITH x AS (...)`) are inlined in the outer query; `x.column` isn't a valid reference | Alias in FROM: `FROM ivl AS i`, use `i.column` |
| 3 | An MV with `UNION ALL` in its SELECT only ever populates from the first branch | MVs rewrite the query to run against the just-inserted block; only the first SELECT in a UNION chain gets attached to that rewrite | One MV per UNION branch, all targeting the same table |
| 4 | `CREATE MATERIALIZED VIEW ... TO tbl ... POPULATE` → `Code: 62` | `POPULATE` cannot be combined with an explicit `TO` target in this version | Give the MV its own `ENGINE = ...` clause instead of `TO`; `POPULATE` works with MV-owned storage |
| 5 | Peak `cnt_a` inflated +3.4%, `cnt_b` +23% vs oracle, no error | `SummingMergeTree` with `sum(1)` counts a session more than once if it has multiple intervals touching the same minute | `AggregatingMergeTree` + `uniqExactState(session_id)` — dedup by construction |
| 6 | `video_type` blank for ~2-4% of rows across all layers | `coalesce(vt_raw, 'unk')` only replaces `NULL`; the catalog has genuine empty-string values, not nulls (confirmed: 0 join misses, 1,089 catalog rows with `video_type=''`) | `coalesce(nullIf(vt_raw, ''), 'unk')` |
| 7 | A refreshable MV chained into a plain incremental MV wouldn't reliably fire | Refresh does a full-result swap, not a stream of ordinary INSERT blocks | Make the downstream consumer refreshable too, sequenced with `DEPENDS ON` rather than relying on INSERT-triggering |

---

*Every design decision here has been checked against the DuckDB reference oracle (`pipeline/01_reference_pipeline.sql`) which byte-matches the 5-object build on gold (93,007 rows, peak cnt_a=2,581, cnt_b=2,958). The cloud pipeline is now fully MV-driven, zero manual INSERTs, and gold is verified correct relative to its own upstream source; a residual ~3% gap vs the oracle traces to an interval-count difference (25,149 vs 27,251) still open for investigation. The exact DDL for every table described in this file lives in `pipeline/04_ddl_annotated.sql`.*
