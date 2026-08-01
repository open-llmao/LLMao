# Table & Column Catalog — Bronze → Silver → Gold

> Companion to `DESIGN.md`. Every cardinality, null count and normalisation rule below is
> **measured** from the 905,558-row source, not estimated.
>
> Rule of thumb used throughout: **`LowCardinality` pays off below ~10,000 distinct values**
> and hurts above it. Every column is judged against that line.

---

## 0. Layer contract

| Layer | Rule | Tables |
|---|---|---|
| **BRONZE** | Exact mirror of source. Zero logic. Never rewritten. | 2 |
| **SILVER** | Typed, normalised, deduped, enriched. Decides active/inactive. | 5 |
| **GOLD** | Pre-aggregated serving. No session-level data. | 3 |

Everything — including deltas — is computed **at table level** via MVs. No business logic
lives in the dashboard query.

---

## 1. BRONZE — exactly two tables, exactly the source

Bronze mirrors the CSVs 1:1. Same column names, same order, no type narrowing, no
normalisation. If bronze is wrong, replay is impossible.

### 1.1 `bronze_events` — 905,558 rows

| # | Column | Source type | Distinct | Nulls | Bronze type | Why |
|---|---|---|---:|---:|---|---|
| 1 | `content_id` | numeric | 3,357 | 0 | `String` | keep raw; cast in silver |
| 2 | `video_session_id` | 64-char hex | 10,866 | 0 | `String` | raw fidelity |
| 3 | `user_id` | 64-char hex | 9,618 | 0 | `String` | raw fidelity |
| 4 | `event_type` | text | **7** | 0 | `String` | |
| 5 | `event` | text | **47** | 0 | `String` | |
| 6 | `event_timestamp` | epoch ms | 641,201 | 0 | `Int64` | **do not cast to DateTime in bronze** |
| 7 | `platform` | text | **10** | 0 | `String` | |
| 8 | `app_version` | text | 65 | 0 | `String` | |
| 9 | `country` | text | **1** | 0 | `String` | |
| 10 | `audio_language` | text | 40 | **1,991** | `String` | dirty — see §2.3 |
| 11 | `subtitle_language` | text | 10 | **2,006** | `String` | dirty — see §2.3 |
| 12 | `player_version` | text | 13 | **1,534** | `String` | |
| 13 | `session_start_epoch` | epoch ms | 10,848 | 0 | `Int64` | **constant per session** |

```sql
CREATE TABLE bronze_events
(
    content_id          String,
    video_session_id    String,
    user_id             String,
    event_type          String,
    event               String,
    event_timestamp     Int64,
    platform            String,
    app_version         String,
    country             String,
    audio_language      Nullable(String),
    subtitle_language   Nullable(String),
    player_version      Nullable(String),
    session_start_epoch Int64,
    _ingested_at        DateTime64(3) DEFAULT now64(3),
    _partition          Int32  DEFAULT 0,
    _offset             Int64  DEFAULT 0,
    _raw                String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(toDateTime(intDiv(session_start_epoch, 1000)))
ORDER BY (video_session_id, event_timestamp, event);
```

> **`PARTITION BY session_start_epoch`, not event date.** Verified: `session_start_epoch` is
> **constant across every row of every session**. Partitioning on it co-locates a session's
> entire history in one partition — including the 16 sessions that span multiple calendar
> days (max 43 hrs). Partition by event date and those sessions shatter.

> **Plain `MergeTree`, not `ReplacingMergeTree`.** Bronze keeps duplicates. Dedup is silver's
> job; if bronze dedups, you can never prove what actually arrived.

### 1.2 `bronze_content` — 33,464 rows

| Column | Distinct | Nulls | Type | Note |
|---|---:|---:|---|---|
| `content_id` | 33,464 | 0 | `String` | unique — true primary key |
| `title` | 30,508 | 0 | `String` | 2,956 duplicate titles |
| `video_type` | **2** | 0 | `String` | **only `live` / `vod`** |
| `category` | 84 | 0 | `String` | |

```sql
CREATE TABLE bronze_content
(
    content_id String,
    title      String,
    video_type String,
    category   String,
    _loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY content_id;
```

**Join coverage: 100%.** All 3,357 event `content_id`s exist in the content table — a
`LEFT JOIN` is safe, but assert the count anyway.

---

## 2. Column decisions — cardinality drives everything

### 2.1 The cardinality ladder

```
  1  country              ▏                    ← degenerate, keep for schema stability
  2  video_type           ▏                    ← live / vod. THE dimension for this problem
  7  event_type           ▏
 10  platform             ▏
 10  subtitle_language    ▏
 13  player_version       ▏
 40  audio_language       ▎                    ← 17 after normalisation
 47  event                ▎
 65  app_version          ▎
 84  category             ▍
────────────────────────── LowCardinality clearly wins ──────────────────────────
 3,357  content_id (in events)      ██
 9,618  user_id                     ████
10,866  video_session_id            █████
────────────────────────── LowCardinality breaks even ~10K ──────────────────────
33,464  content_id (catalog)        ███████████
```

### 2.2 Type recommendation per column

| Column | Distinct | **Silver type** | Bytes/row | vs `String` | Reasoning |
|---|---:|---|---:|---:|---|
| `country` | 1 | `LowCardinality(String)` | ~1 | 5x | degenerate today, may grow |
| `video_type` | 2 | `Enum8('vod'=1,'live'=2)` | **1** | 4x | closed set, never grows |
| `event_type` | 7 | `Enum8` | **1** | 17x | closed set |
| `platform` | 10 | `LowCardinality(String)` | ~1 | 17x | may add devices — not `Enum` |
| `subtitle_language` | 7* | `LowCardinality(String)` | ~1 | 11x | *after normalisation |
| `player_version` | 13 | `LowCardinality(String)` | ~1 | 32x | |
| `audio_language` | 17* | `LowCardinality(String)` | ~1 | 13x | *after normalisation |
| `event` | 47 | `LowCardinality(String)` | ~1 | 24x | keep raw for audit |
| `app_version` | 65 | `LowCardinality(String)` | ~1 | 9x | |
| `category` | 84 | `LowCardinality(String)` | ~1 | 5x | |
| `signal` | 5 | `Enum8` | **1** | — | derived (§2.4) |
| `content_id` | 3,357 / 33,464 | **`UInt32`** | 4 | 2.5x | max = 2,078,177,474 — fits |
| `video_session_id` | 10,866 | **`UInt128`** (hash) | 16 | **4x** | 64-char hex → see §2.5 |
| `user_id` | 9,618 | **`UInt128`** (hash) | 16 | **4x** | 64-char hex |
| `event_timestamp` | 641,201 | `DateTime64(3)` | 8 | — | ms precision required |
| `session_start_epoch` | 10,848 | `DateTime64(3)` | 8 | — | partition key |
| `title` | 30,508 | `String` | var | — | **do not** LowCardinality |

> **`Enum8` vs `LowCardinality`:** use `Enum8` only for genuinely closed sets
> (`video_type`, `event_type`, `signal`). An unseen-day value not in the `Enum` **throws**.
> `platform` and `event` use `LowCardinality` because a new device or telemetry event on the
> unseen day must not break ingestion.

### 2.3 Normalisation rules — the dirty columns

**`audio_language` is the worst column in the dataset.** 40 raw values collapse to 17:

| Step | Distinct | Removes |
|---|---:|---|
| raw | **40** | — |
| `lower()` | 25 | `HIN`/`hin`, `MAL`/`mal`, `ENG`/`eng` … |
| `splitByChar('-')[1]` | 18 | `hin-hindi`→`hin`, `eng-English`→`eng`, `-soundhandler`→`` |
| `coalesce(nullif(…,''),'unk')` | **17** | 1,991 NULLs |

```sql
coalesce(nullif(splitByChar('-', lower(assumeNotNull(audio_language)))[1], ''), 'unk')
```

Same rule collapses `subtitle_language` **10 → 7** (`UND`/`und`, `OFF`/`off`, `UNK`/`unk`).

> **Known residue:** `jap` (1,374) and `jpn` (386) are both Japanese and the rule does *not*
> merge them. Needs an explicit alias map. Left unmerged deliberately — flagged rather than
> silently guessed.

**NULL policy** — never leave a dimension nullable in silver; `Nullable` costs a byte per row
and breaks `LowCardinality` compression:

| Column | Nulls | Replace with |
|---|---:|---|
| `audio_language` | 1,991 | `'unk'` |
| `subtitle_language` | 2,006 | `'unk'` |
| `player_version` | 1,534 | `'unk'` |

### 2.4 Derived columns — computed once in silver, never at query time

| Column | Type | Rule | Cardinality |
|---|---|---|---:|
| `signal` | `Enum8` | §3.4 of `DESIGN.md` | **5** |
| `is_state` | `UInt8` | `signal != 'LIVENESS'` | 2 |
| `event_minute` | `DateTime` | `toStartOfMinute(event_ts)` | 3,686 |
| `video_type` | `Enum8` | `dictGet(dict_content,'video_type',content_id)` | 2 |
| `category` | `LowCardinality` | `dictGet(...)` | 84 |

```sql
signal Enum8('LIVENESS'=0,'STATE_ACTIVE'=1,'STATE_INACTIVE'=2,'SESSION_OPEN'=3,'SESSION_CLOSE'=4)
```

Distribution: `LIVENESS` 783,941 (86.6%) · `STATE_ACTIVE` 57,391 (6.3%) ·
`STATE_INACTIVE` 42,465 (4.7%) · `SESSION_CLOSE` 10,881 · `SESSION_OPEN` 10,880.

### 2.5 The ID columns — 4x win, with a caveat

`video_session_id` and `user_id` are **verified 100% uppercase 64-char hex**. As `String`
that's 64 bytes + length; as `UInt128` it's 16.

```sql
reinterpretAsUInt128(unhex(video_session_id))   -- lossless for 32 hex chars
```

> ⚠ **64 hex chars = 256 bits, which does not fit in `UInt128`.** Options:
> **(a)** `FixedString(32)` via `unhex()` — fully lossless, 32 bytes, 2x saving;
> **(b)** `cityHash64()` → `UInt64`, 8 bytes, 8x saving, ~0 collision risk at 10K–10M sessions.
>
> **Recommendation: `FixedString(32)`.** Lossless, still 2x, and keeps the join to bronze
> exact. Use the hash only if profiling shows the ID column dominates.

---

## 3. SILVER — five tables

```
 bronze_events ──┬─► S1 silver_events        (typed, normalised, deduped, enriched)
                 │        │
 bronze_content ─┘        ├─► S2 silver_session_dim       (1 row / session)
   (DICTIONARY)           ├─► S3 silver_session_timeline  (arrays / session)
                          │        │
                          │        └─► S4 silver_active_intervals  ◀ THE STATE MACHINE
                          │                   │
                          └───────────────────┴─► S5 silver_session_minutes
```

### S1 · `silver_events` — 905,558 rows

Row-level transform only ⇒ a plain incremental MV works.

| Column | Type | Cardinality | Role |
|---|---|---:|---|
| `session_id` | `FixedString(32)` | 10,866 | key |
| `user_id` | `FixedString(32)` | 9,618 | dim |
| `content_id` | `UInt32` | 3,357 | dim |
| `event_ts` | `DateTime64(3)` | 641,201 | time |
| `session_start` | `DateTime64(3)` | 10,848 | partition |
| `signal` | `Enum8` | 5 | **state machine input** |
| `event` | `LowCardinality(String)` | 47 | audit |
| `event_type` | `Enum8` | 7 | audit |
| `platform` | `LowCardinality(String)` | 10 | dim |
| `country` | `LowCardinality(String)` | 1 | dim |
| `video_type` | `Enum8` | 2 | dim (from dict) |
| `category` | `LowCardinality(String)` | 84 | dim (from dict) |
| `audio_language` | `LowCardinality(String)` | 17 | dim (normalised) |
| `subtitle_language` | `LowCardinality(String)` | 7 | dim (normalised) |
| `app_version` | `LowCardinality(String)` | 65 | dim |
| `player_version` | `LowCardinality(String)` | 13 | dim |

```sql
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(session_start)
ORDER BY (session_id, event_ts, event, signal);
```

**Dedup key = `(session_id, event_ts, event)`** — removes the 13 duplicate `VideoSessionStart`
and 14 duplicate `VideoSessionEnd` rows.

### S2 · `silver_session_dim` — 10,866 rows

One row per session with dimensions **pinned deterministically**. Required because 120
sessions carry >1 `user_id` and 95 carry >1 `platform`.

| Column | Aggregation | Why |
|---|---|---|
| `session_id` | key | |
| `platform` | `argMinState(platform, event_ts)` | **not `any()`** — must be reproducible |
| `country` | `argMinState(country, event_ts)` | |
| `content_id` | `argMinState(content_id, event_ts)` | |
| `video_type` | `argMinState(video_type, event_ts)` | |
| `user_id` | `argMinState(user_id, event_ts)` | |
| `session_start` | `minSimpleState(event_ts)` | |
| `last_seen` | `maxSimpleState(event_ts)` | **watermark driver** |
| `n_events` | `countState()` | QA |
| `has_close` | `maxSimpleState(signal='SESSION_CLOSE')` | open-session flag |

```sql
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(session_start)
ORDER BY session_id;
```

> **`argMin` not `any`.** `any()` is non-deterministic across merges — two runs give two
> answers, and the benchmark becomes unreproducible.

### S3 · `silver_session_timeline` — 10,866 rows

**This is the table that solves the materialized-view problem.** A ClickHouse MV only sees the
block being inserted, so it cannot run a state machine that needs the whole ordered session.
`AggregatingMergeTree` + `groupArray` sidesteps it: the array accumulates across blocks and
merges.

| Column | Aggregation | Size | Filter |
|---|---|---:|---|
| `transitions` | `groupArrayStateIf((event_ts, signal), signal != 'LIVENESS')` | **11.2/session** | **86.6% dropped** |
| `live_minutes` | `groupUniqArrayState(toUnixTimestamp(event_ts) DIV 60)` | 12.3/session | 783,941 → 133,296 (**5.9x**) |
| `last_seen` | `maxSimpleState(event_ts)` | 1 | |
| `has_close` | `maxSimpleState(...)` | 1 | |

```sql
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(session_start)
ORDER BY session_id;
```

**The two filters that define silver:**

| Filter | Keeps | Drops | Purpose |
|---|---:|---:|---|
| `signal != 'LIVENESS'` | 121,617 | 783,941 | state machine input |
| `groupUniqArray(minute)` | 133,296 | 650,645 | gap detection only |

`live_minutes` is bounded by session **duration**, not chattiness — the 1,803-event session
contributes at most `duration_minutes` entries.

### S4 · `silver_active_intervals` — 35,954 rows ◀ the state machine

All conditions applied here. Populated by a **refreshable MV** (needs whole-session context),
processing only sessions whose `last_seen` moved since the last refresh.

| Column | Type | Note |
|---|---|---|
| `session_id` | `FixedString(32)` | |
| `defn` | `Enum8('D1'=1,'D2'=2,'D3'=3)` | **all three definitions coexist** |
| `start_ms` / `end_ms` | `DateTime64(3)` | |
| `platform` / `country` / `video_type` / `content_id` / `category` | dims | denormalised |
| `is_open` | `UInt8` | 1 = provisional close at watermark |
| `close_reason` | `Enum8('END','WATERMARK','BACKGROUND','PAUSE','GAP')` | audit |
| `version` | `UInt64` | supersede on real close |

```sql
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(start_ms)
ORDER BY (session_id, defn, start_ms);
```

**The complete condition set:**

| # | Condition | Trigger | Cost if applied |
|---|---|---|---:|
| 1 | Session opens | `SESSION_OPEN` | — |
| 2 | Session closes | `SESSION_CLOSE` | — |
| 3 | Backgrounded | `AppBackgrounded` | **−915 hrs** |
| 4 | Foregrounded | `AppForegrounded` | — |
| 5 | Paused | `pause` (D2/D3 only) | **−159 hrs** |
| 6 | Resumed | `resume` / `Play` | — |
| 7 | Buffering | `BufferStart` (D3 only) | −10 hrs |
| 8 | **Heartbeat gap > 90s** | gap in `live_minutes` | inferred background |
| 9 | **Idempotency** | drop transition == previous | prevents cumsum drift |
| 10 | Watermark close | `last_seen + 90s` and no `SESSION_CLOSE` | open sessions |

| `defn` | Excludes | Active hrs | **Peak** |
|---|---|---:|---:|
| `D1` | background | 2,061.7 | **2,657** |
| `D2` | background + pause | 1,902.9 | **2,427** |
| `D3` | + buffering | 1,876.8 | **2,411** |

> Condition 9 is one `arrayFilter` line and it is **not optional** — 109 `BG→BG` and 45
> `FG→FG` pairs produce unbalanced deltas, and because concurrency is a cumulative sum, one
> unbalanced delta corrupts **every subsequent minute permanently**.

### S5 · `silver_session_minutes` — 136,924 rows

Explodes each interval to the minutes it covers, **deduplicated per session-minute**.

| Column | Type |
|---|---|
| `minute` | `DateTime` |
| `session_id` | `FixedString(32)` |
| `defn` | `Enum8` |
| `covers_boundary` | `UInt8` — Definition A |
| `covers_any` | `UInt8` — Definition B |
| dims | denormalised |

```sql
ENGINE = ReplacingMergeTree
ORDER BY (minute, defn, session_id);
```

> **This table exists solely to kill the 9.54% double-count bug.** A session can hold two
> active intervals inside one minute (pause + resume within 60s). Without dedup here:
> 149,992 session-minutes instead of **136,924** — a **+9.54%** inflation.

---

## 4. GOLD — three tables

### G1 · `gold_concurrency_minute` — 90,511 rows ◀ primary serving table

**The measurement that changes the design.** Aggregating the exploded minutes to
`(minute × dimensions)` collapses them almost completely:

```
 session-minutes exploded    136,924
 → aggregated to full grain   90,511   ◀ only 1.26x the delta table (71,908)
```

The problem statement warns that per-minute explosion is "prohibitively large." **That is true
for per-session-per-minute rows, and false once you aggregate to dimension grain.** Measured:
**24.6 dimension-combos active per minute** on average, 1.51 sessions per cell.

| Column | Type | Cardinality |
|---|---|---:|
| `minute` | `DateTime` | 3,686 |
| `platform` | `LowCardinality(String)` | 10 |
| `country` | `LowCardinality(String)` | 1 |
| `video_type` | `Enum8` | 2 |
| `content_id` | `UInt32` | 3,357 |
| `defn` | `Enum8` | 3 |
| `cnt_a` | `SimpleAggregateFunction(sum, UInt32)` | Definition A |
| `cnt_b` | `SimpleAggregateFunction(sum, UInt32)` | Definition B |

```sql
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (defn, country, video_type, platform, content_id, minute);
```

> **✅ Verified property: counts are perfectly additive across dimensions.** Rolling the
> full-grain table up to `(minute, platform)` and comparing against a direct
> `(minute, platform)` count over 5,110 rows gives **0 mismatches, max diff 0**.
>
> This holds because each session is pinned (S2) to exactly **one** `(platform, country,
> video_type, content_id)` tuple — the dimension tuple *partitions* the session set.
>
> **Therefore: `SUM` across dimensions ✅ · `SUM` across time ❌ (use `MAX`/`AVG`).**

This is what makes the whole thing work: **no cumulative sum at query time.** Peak and average
become plain aggregates, which removes three failure modes at once — the filter-before-cumsum
trap, the `WITH FILL` gap problem, and negative-drift from unbalanced deltas.

```sql
-- peak + average, any filter, any grain. No window function.
SELECT max(c) AS peak, avg(c) AS avg_conc, argMax(minute, c) AS peak_minute
FROM (
    SELECT minute, sum(cnt_b) AS c
    FROM gold_concurrency_minute
    WHERE defn = 'D2' AND minute BETWEEN {from} AND {to} AND platform = {p}
    GROUP BY minute
);
```

### G2 · `gold_concurrency_delta` — 71,908 rows

Kept **alongside** G1, not instead of it. Deltas are update-friendly; counts are query-friendly.

| Column | Type |
|---|---|
| `minute`, dims, `defn` | as G1 |
| `delta_a` / `delta_b` | `SimpleAggregateFunction(sum, Int32)` |

```sql
ENGINE = SummingMergeTree
ORDER BY (defn, country, video_type, platform, content_id, minute);
```

| | G1 counts | G2 deltas |
|---|---|---|
| Rows | 90,511 | 71,908 |
| Write cost per interval | O(duration in minutes) | **O(1)** — 2 rows |
| Query | `max`/`avg` — trivial | needs cumsum + `WITH FILL` |
| Extending an open session | rewrite minute range | **append one row** |
| Correcting an end time | delete + reinsert | **one `−1` moves** |

**Tiering:** G2 serves the hot window (open sessions, last N minutes); G1 serves finalised
history once the watermark passes. That is the "hybrid tiering / incremental compaction" the
problem statement asks for.

### G3 · `gold_concurrency_hour` — ~62 rows

| Column | Type |
|---|---|
| `hour` | `DateTime` |
| dims, `defn` | as G1 |
| `peak` | `AggregateFunction(max, UInt32)` |
| `avg_conc` | `AggregateFunction(avg, UInt32)` |
| `minutes_covered` | `AggregateFunction(count, UInt32)` |

`maxMerge` over minute values is valid — peak of an hour **is** the max of its minutes.
`sum` would not be.

---

## 5. Size ledger

| Layer | Table | Rows | vs bronze |
|---|---|---:|---:|
| 🥉 | `bronze_events` | 905,558 | 100% |
| 🥉 | `bronze_content` | 33,464 | 3.7% |
| 🥈 | `silver_events` | 905,558 | 100% |
| 🥈 | `silver_session_dim` | 10,866 | 1.2% |
| 🥈 | `silver_session_timeline` | 10,866 | 1.2% |
| 🥈 | **`silver_active_intervals`** | **35,954** | **4.0%** |
| 🥈 | `silver_session_minutes` | 136,924 | 15.1% |
| 🥇 | **`gold_concurrency_minute`** | **90,511** | **10.0%** |
| 🥇 | `gold_concurrency_delta` | 71,908 | 7.9% |
| 🥇 | `gold_concurrency_hour` | ~62 | 0.007% |

**Serving layer is 10% of raw**, and it grows with *minutes × observed dimension combos* —
not with session count. Ten million sessions in one minute still produce ~25 rows.

---

## 6. Open items

1. **`jap` / `jpn` alias** — both Japanese, 1,760 rows, not merged by the normalisation rule.
2. **`FixedString(32)` vs `cityHash64`** for IDs — 2x vs 8x saving; recommend lossless.
3. **90-second gap threshold** — anchored on measured cadence (p50 30s / p90 40s) but
   **unvalidated**. Sweep 60/90/120.
4. **`defn` triples every silver/gold row.** If the benchmark pins one definition, drop the
   others and reclaim 3x.
5. **`country` has 1 value.** Keep in the ordering key for schema stability; it costs ~nothing
   under `LowCardinality` and avoids a migration if the unseen day adds markets.
