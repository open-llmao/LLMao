# LLM Query Guide — Conversational Layer over Concurrency Data

> **Purpose.** This file is written to be fed to an LLM (system prompt / grounding context) powering the conversational layer required by the problem statement — *"LibreChat plus the ClickHouse MCP server... 'what was peak concurrency on Android in the last hour?'"*. It is not prose for a human reviewer; it is intent → decision → SQL, so a model can go from a natural-language question straight to a correct, explainable query against this schema.
>
> **Sister docs:** `README.md` (architecture, assumptions), `PIPELINE_LOGIC.md` (why each table exists, every ClickHouse gotcha), `pipeline/04_ddl_annotated.sql` (exact live DDL).
>
> **Problem statement:** [`SonyLiv/PROBLEM_STATEMENT.md`](SonyLiv/PROBLEM_STATEMENT.md) — every question below is quoted from it verbatim, then answered with tested SQL.

---

## 0. Rules the model must follow, always

1. **This is historical, synthetic data, not a live clock.** There is no `now()`. Any question using "now," "currently," "right now," or "in the last hour" means *relative to the latest timestamp in the data*, not the system clock. Always resolve "now" as:
   ```sql
   SELECT max(minute) FROM gold_concurrency_minute
   ```
   and compute relative windows (`last hour`, `last 15 minutes`) from that value.
2. **Never guess a number.** Every answer must come from a query against `gold_concurrency_minute`, `gold_concurrency_delta`, or (for session-level drill-down) `silver_active_intervals`. If the question can't be answered from these tables, say so — don't approximate from bronze in your head.
3. **Always show the SQL you ran and how many rows it read.** The evaluation criteria explicitly reward "what your queries read, not just how fast they return," and the problem statement requires "evidence they ran through your pipeline." Never present a bare number without its query.
4. **Pick `cnt_a` vs `cnt_b` deliberately, and say which you picked and why.** These are not interchangeable — see §1.
5. **Default `country = 'india'`** unless the user names another — this dataset has exactly one country value; don't ask the user to disambiguate something the data can't vary.

---

## 1. `cnt_a` vs `cnt_b` — the single most important intent decision

Both live in `gold_concurrency_minute`, both are `AggregateFunction(uniqExact, FixedString(64))` — read them with `uniqExactMerge(cnt_a)` / `uniqExactMerge(cnt_b)`, never raw.

| | `cnt_a` — **concurrency** | `cnt_b` — **reach** |
|---|---|---|
| Definition | Sessions whose foreground interval **fully covers** the minute instant | Sessions that **touch** the minute at all (any overlap) |
| Boundary math | `ceil(start/60) ≤ M < ceil(end/60)` | `floor(start/60) ≤ M ≤ floor((end-1)/60)` |
| Answers | *"How many people were watching **at** this instant?"* | *"How many distinct people were watching **during** this minute, even briefly?"* |
| Always | ≤ `cnt_b` for the same minute | ≥ `cnt_a` for the same minute |
| Use for | **Peak concurrency** (the benchmark's primary framing — "how many people watching right now") | Reach / unique-viewer counts, "how many different people tuned in during this window" |

**Default to `cnt_a`** whenever the question is phrased as "how many concurrent viewers," "peak concurrency," "how many people watching [right now / at time T]." Switch to `cnt_b` only if the question explicitly asks about reach, unique viewers over a span, or touches the word "any point during."

**If the question is ambiguous** (e.g. "how many viewers in the last hour?" — instant or span?), compute **both**, present `cnt_a` as the headline, and mention `cnt_b` as the reach figure. Never silently pick one without saying so.

---

## 2. `gold_concurrency_minute` (G1) vs `gold_concurrency_delta` (G2) — which table to query

| | G1 `gold_concurrency_minute` | G2 `gold_concurrency_delta` |
|---|---|---|
| Shape | One row per (minute, dims): `cnt_a`, `cnt_b` as distinct-session states | One row per (minute, dims, `delta_kind`): signed `+1`/`-1` |
| Read pattern | `uniqExactMerge` — direct read, no math | `sum(delta)` running cumsum — one extra window function |
| Use for | **Default for almost every question** — it's already the answer | Cross-validation ("do both models agree on peak?"), or explicitly asked for "the delta/interval-to-delta model" |
| `delta_kind` values | n/a | `'a'` = concurrency convention, `'b'` = reach convention — matches G1's `cnt_a`/`cnt_b` respectively |

**Rule of thumb: read G1 unless the question explicitly asks about the delta model, updates, or cross-validation.** G1 is the fast path; G2 exists to prove G1 is correct, not to replace it.

---

## 3. Dimension vocabulary (exact values in this dataset — use these, don't invent others)

| Dimension | Column | Values |
|---|---|---|
| Platform | `platform` | `ANDROID_PHONE`, `IPHONE`, `SONY_ANDROID_TV`, `JIO_ANDROID_TV`, `FIRE_TV`, `XIAOMI_ANDROID_TV`, `LG_HTML_TV`, `Mweb`, `ANDROID_TAB`, `SAMSUNG_HTML_TV` |
| Country | `country` | `india` (only one value in this dataset) |
| Video type | `video_type` | `vod`, `live`, `unk` (unk = content catalog had a blank video_type — see `PIPELINE_LOGIC.md` §2.1) |
| Content | `content_id` | `Int64`, join `bronze_content_raw` for `title`/`category` if the user names a title instead of an ID |
| Time range | `minute` | data spans `2026-07-14 15:43` → `2026-07-26 11:30` in this build (query `min(minute)`/`max(minute)` — don't hardcode, this changes on the unseen day) |

**Fuzzy platform matching.** Users will say "Android" meaning any of `ANDROID_PHONE`, `ANDROID_TAB`, `JIO_ANDROID_TV`, `SONY_ANDROID_TV`, `XIAOMI_ANDROID_TV`. If ambiguous, either ask which, or run `WHERE platform LIKE '%ANDROID%'` and say you interpreted it that way.

---

## 4. Query templates — copy, fill placeholders, run

### 4.1 "What's the peak concurrency [filters] [time range]?" — the canonical benchmark question

```sql
SELECT
    max(c)              AS peak_concurrency,
    round(avg(c), 2)     AS avg_concurrency,
    argMax(minute, c)    AS peak_minute
FROM (
    SELECT minute, uniqExactMerge(cnt_a) AS c
    FROM gold_concurrency_minute
    WHERE minute BETWEEN {from} AND {to}
      -- optional filters, add only what the user asked for:
      -- AND platform = {platform}
      -- AND video_type = {video_type}
      -- AND content_id = {content_id}
    GROUP BY minute
);
```

### 4.2 "What was peak concurrency on Android in the last hour?" — the problem statement's own example

```sql
-- Step 1: resolve "the last hour" against the data's own clock, not wall time
WITH latest AS (SELECT max(minute) AS t FROM gold_concurrency_minute)
SELECT
    max(c) AS peak, round(avg(c),2) AS avg_conc, argMax(minute, c) AS peak_min
FROM (
    SELECT minute, uniqExactMerge(cnt_a) AS c
    FROM gold_concurrency_minute, latest
    WHERE minute BETWEEN latest.t - INTERVAL 1 HOUR AND latest.t
      AND platform LIKE '%ANDROID%'
    GROUP BY minute
);
```

### 4.3 "How many unique/distinct viewers watched X during [range]?" — reach, not concurrency

```sql
SELECT
       (SELECT countDistinct(session_id)
        FROM silver_active_intervals
        WHERE content_id = {content_id}
          AND start_ms < {to_ms} AND end_ms > {from_ms}) AS true_distinct_sessions;
```
**Caution:** summing `cnt_b` across minutes over-counts sessions that span multiple minutes. For a true "how many distinct viewers over this whole range" answer, always fall back to `countDistinct(session_id)` on `silver_active_intervals`, not a sum of per-minute reach.

### 4.4 "Peak at hour/day grain" — never a stored value, always max-of-the-finer-grain

```sql
SELECT toStartOfHour(minute) AS hour, max(c) AS peak, round(avg(c),2) AS avg_conc
FROM ( SELECT minute, uniqExactMerge(cnt_a) AS c
       FROM gold_concurrency_minute
       WHERE minute BETWEEN {from} AND {to}
       GROUP BY minute )
GROUP BY hour
ORDER BY hour;
-- swap toStartOfHour -> toDate for day grain. NEVER sum minute peaks to get an hour peak.
```

### 4.5 "Which platform/content peaked when?" — per-dimension breakdown

```sql
SELECT platform, max(c) AS peak, argMax(minute, c) AS peak_minute
FROM ( SELECT minute, platform, uniqExactMerge(cnt_a) AS c
       FROM gold_concurrency_minute
       GROUP BY minute, platform )
GROUP BY platform
ORDER BY peak DESC;
```
**Warn the user if they ask for a sum across this table:** `Σ per-platform peaks ≠ global peak` — see §5.3 scenario 2 for live proof. Only report per-dimension peaks as independent facts, never add them up to claim a global figure.

### 4.6 "Are both models (session-aware vs session-independent) in agreement?" — cross-validation, uses G2

```sql
SELECT max(c) AS g1_peak FROM (
    SELECT minute, uniqExactMerge(cnt_a) AS c FROM gold_concurrency_minute GROUP BY minute
);
SELECT max(live) AS g2_peak FROM (
    SELECT minute,
           sum(delta) OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS live
    FROM gold_concurrency_delta WHERE delta_kind = 'a'
);
-- Report both; if they diverge, that's a real signal to investigate, not noise to hide.
```

### 4.7 "Is a live session still open right now?" / "Show me open sessions"

```sql
SELECT session_id, platform, content_id, start_ms, end_ms, is_open
FROM silver_active_intervals
WHERE is_open = 1
ORDER BY end_ms DESC
LIMIT 50;
```
`silver_active_intervals` refreshes on a **bounded 15-second cycle** — if the user asks for sub-15s freshness, say the answer may be up to 15s stale and name the mechanism, don't claim instant. Full mechanics in §6.1.

### 4.8 "Detecting a concurrency decline" — the optional LLM+ClickStack use case named in the problem statement

```sql
SELECT minute, c, prev_c, round((c - prev_c) / prev_c * 100, 1) AS pct_change
FROM (
    SELECT minute, uniqExactMerge(cnt_a) AS c,
           lagInFrame(uniqExactMerge(cnt_a)) OVER (ORDER BY minute) AS prev_c
    FROM gold_concurrency_minute
    WHERE content_id = {content_id}
    GROUP BY minute
)
WHERE prev_c > 0 AND (c - prev_c) / prev_c < -0.30
ORDER BY minute;
```
Use this to drive an alert ("content X saw a 42% concurrency drop at 16:32 — check if the asset ended or there's a system issue").

---

## 5. The problem statement's own questions — answered with SQL

Every bullet below is quoted directly from `SonyLiv/PROBLEM_STATEMENT.md`. The LLM should be able to reproduce each answer live, not just recite it.

### 5.1 *"How do you define an active interval when the heartbeat is missing, the player is paused, or the app is backgrounded?"*

Three signals, three different rulings — this is not one rule, it's three:

| Case named in the question | Ruling | Verify with |
|---|---|---|
| **Heartbeat is missing** | Silence > 50s between two `live_ts` values → inferred backgrounded. Threshold is empirically derived (gap histogram collapses 137× at 50s; independently, the probability a gap of that length contains a real `AppBackgrounded` jumps from 0.5% to 50.6% at the same point). | `SELECT ts - lagInFrame(ts) OVER (...) AS gap FROM ... WHERE gap > 50000` on `silver_session_state_current.live_ts` |
| **Player is paused** | **Active.** `pause`/`resume`/`speed-pause`/`AdPause` are `event` values hidden inside `event_type='VideoHeartbeat'` and are treated as no state change — the session stays whatever it was. | Heartbeats keep firing during pause in this data — see §6.6 |
| **App is backgrounded** | **Inactive**, immediately, on the explicit `AppBackgrounded` event — no threshold needed since this is a direct signal, not an inference. | `event_type = 'AppBackgrounded'` in `bronze_events_raw` |

```sql
-- reproduce the 50s threshold's own evidence: gap histogram around the cliff
SELECT
    multiIf(gap BETWEEN 40000 AND 50000, '40-50s', gap BETWEEN 50000 AND 60000, '50-60s', 'other') AS bucket,
    count() AS n
FROM (
    SELECT session_id, ts - lagInFrame(ts) OVER (PARTITION BY session_id ORDER BY ts) AS gap
    FROM ( SELECT session_id, arrayJoin(live_ts) AS ts FROM silver_session_state_current )
)
WHERE gap BETWEEN 40000 AND 60000
GROUP BY bucket;
```

### 5.2 *"How should active ranges be represented: interval arrays per session, normalized intervals, pre-aggregated minute deltas, or a hybrid?"*

**Hybrid — all three, each doing a different job:**

| Representation | Table | Job |
|---|---|---|
| Interval arrays per session | `silver_session_state` (`AggregateFunction` states) | Absorbs streaming updates with zero gap — arrays would break across MV blocks; states don't. |
| Normalized intervals | `silver_active_intervals` | The actual `[start, end)` model — source of truth for everything downstream. |
| Pre-aggregated minute deltas | `gold_concurrency_delta` (G2) | Session-independent cross-check; update-friendly (fixed row count per interval regardless of duration). |
| *(also)* minute occupancy counts | `gold_concurrency_minute` (G1) | Primary serving layer — direct `uniqExactMerge`, no cumsum at query time. |

```sql
-- prove the hybrid is consistent: G1 and G2 peaks should agree (see §4.6)
```

### 5.3 *"How do you compute accurate minute-wise peak and average concurrency without scanning raw history?"*

Query `gold_concurrency_minute` directly (§4.1) — it's already pre-aggregated to minute × dimension grain, so peak/average never touches session-level data.

**Scenario 1, quoted directly from the problem statement:**
> *"if minute 1 has 300K concurrent sessions, minute 2 has 200K, and minute 3 has 50K, the peak concurrency for the range (minutes 1–3) would be 300K concurrent sessions."*

This is just confirming `max()` over the per-minute curve, never a stored running total:
```sql
SELECT max(c) AS peak FROM (
    SELECT minute, uniqExactMerge(cnt_a) AS c
    FROM gold_concurrency_minute WHERE minute IN ({minute1},{minute2},{minute3})
    GROUP BY minute
);
-- max() picks minute 1's value (the largest), regardless of order or what the other two minutes hold.
```

**Scenario 2, quoted directly from the problem statement:**
> *"concurrency varies across dimension combinations: a dimension like platform and a content might peak at one minute, while a combination like platform + country might reach its peak at an entirely different minute."*

**Live proof on this dataset** (§4.5's query, actual output):

| Platform | Peak | Peak minute |
|---|---:|---|
| ANDROID_PHONE | 1,600 | 10:56 |
| IPHONE | 320 | 10:56 |
| SONY_ANDROID_TV | 311 | **11:00** |
| JIO_ANDROID_TV | 221 | **11:01** |
| SAMSUNG_HTML_TV | 68 | 10:56 |
| Mweb | 63 | **11:05** |
| XIAOMI_ANDROID_TV | 42 | **10:50** |
| LG_HTML_TV | 25 | **11:01** |

Peaks spread across a 15-minute window (10:50 → 11:05) — confirmed, not assumed. **Rule that follows: `max()` can never be pre-aggregated per dimension and then summed to get a global figure** — each dimension slice must be queried independently, at query time, after filters are applied.

### 5.4 *"How does the model stay filter-friendly across common business dimensions: platform, country, content, video type, time grain?"*

```sql
ORDER BY (country, video_type, platform, content_id, minute)
```
Highest-selectivity-per-byte dimension first. Every benchmark filter (`WHERE country=... AND video_type=...`) prunes before the index even reaches `minute`. Time grain (`minute`/`hour`/`day`) is never a separate column or table — always `max()`-of-the-finer-grain at query time (§4.4), because pre-storing an hour-grain peak would silently violate scenario 2 above (an hour's peak is not the sum or the last value of its minutes; it's the max).

### 5.5 *"How do you handle sessions that are still open, whose active ranges keep growing as new heartbeats arrive?"*

Full mechanics in §6.1 below — this is the single most detailed edge case in this document, because it's also the one the problem statement weights most heavily ("Update handling" is its own evaluation criterion).

---

## 6. Edge cases — the ones that break naive designs

These are not hypothetical. Each one was checked against the live data, and each one has a specific mechanism in the pipeline that handles it — not a hope that it won't happen.

### 6.1 The session's `VideoSessionEnd` never arrives — how do we know what the concurrency was?

**This is the central open-session question**, and the answer is: **we don't wait to find out — we use heartbeat silence as the closing signal, provisionally, and let a later event correct it if one arrives.**

Mechanism, precisely:
1. A session's `has_close` flag is `maxMerge(has_close_state)` — true only if a `VideoSessionEnd` was ever seen.
2. For any session where `has_close = 0`, the interval-building algorithm emits a **synthetic close** at `last_seen_ms + 50s` (the same 50s threshold as gap-inference — see §5.1) and flags that interval `is_open = 1`.
3. This gives every query a **deterministic snapshot** rather than an ever-growing open interval — concurrency at any minute is answerable immediately, without waiting for a close that may never come.
4. If a real heartbeat *or* a late `VideoSessionEnd` arrives afterward, `silver_active_intervals` is `ReplacingMergeTree(version)` keyed by `version = last_seen_ms` — the newer row wins at merge. **No rebuild, no rescan of unaffected sessions.**

```sql
-- are there any open sessions right now, and what's their provisional last-active state?
SELECT session_id, platform, content_id, start_ms, end_ms, is_open
FROM silver_active_intervals
WHERE is_open = 1;
```

**On this dataset: 0 open sessions** — every one of the 10,866 sessions has a real `VideoSessionEnd`. This means the watermark path is currently dead code, *by data, not by design* — it exists specifically for the unseen day and for production scale, where open-at-cutoff sessions are the norm, not the exception. **Yes, heartbeats are exactly what determines the answer** — `last_seen_ms` (used as both the watermark anchor and the `version` for `ReplacingMergeTree`) is `maxMerge` over every timestamp the session ever reported, heartbeat or otherwise.

### 6.2 A session goes silent forever — no `AppBackgrounded`, no `VideoSessionEnd`, heartbeats just stop

Two distinct mechanisms handle this, and they don't conflict, because they operate on different parts of the timeline:

- **Gaps *between* two known timestamps** (an internal silence, followed by activity resuming) are handled by the `gaps` CTE — it only fires on *consecutive pairs* of `live_ts`, so it requires a "before" and "after."
- **Silence *after the last known timestamp*, with nothing after it** is handled by the `wm` (watermark) CTE in §6.1 — it fires once, unconditionally, for any session with `has_close = 0`, anchored to `last_seen_ms`.

There is no double-counting: the `gaps` CTE structurally cannot fire past the last `live_ts` (there's no "next" row to pair with), so the watermark is the only mechanism that ever closes the true tail of a session's timeline.

### 6.3 Duplicate `VideoSessionStart` / `VideoSessionEnd` events for the same session

**Confirmed present in this dataset:** 13 sessions have more than one `VideoSessionStart`, 14 have more than one `VideoSessionEnd`.

```sql
SELECT event_type, countIf(cnt > 1) AS sessions_with_dupes
FROM ( SELECT video_session_id, event_type, count() AS cnt
       FROM bronze_events_raw
       WHERE event_type IN ('VideoSessionStart', 'VideoSessionEnd')
       GROUP BY video_session_id, event_type )
GROUP BY event_type;
-- VideoSessionEnd: 14 sessions with dupes.  VideoSessionStart: 13 sessions with dupes.
```

Handled by the idempotency rule in the interval-cutting algorithm: state-change rows are deduped via a windowed `lag` comparison — a row is only kept if its state *differs* from the immediately preceding row for that session. A duplicate `VideoSessionEnd` produces the same state (`-1`, closed) as the first one, so the second is silently dropped rather than emitting a second (meaningless) close boundary.

### 6.4 `VideoError` fires mid-session — is that the end of the session?

**No — verified, not assumed.** On this dataset, **293 sessions have at least one `VideoError`**, and **100% of them (293/293) have events continuing after the error, including a real `VideoSessionEnd` that comes later.**

```sql
WITH err AS (SELECT video_session_id, min(event_timestamp) et FROM bronze_events_raw WHERE event_type='VideoError' GROUP BY video_session_id)
SELECT
    count() AS sessions_with_error,
    countIf(later.n > 0) AS sessions_continuing_after_error
FROM err
LEFT JOIN (
    SELECT b.video_session_id, count() AS n
    FROM bronze_events_raw b INNER JOIN err ON err.video_session_id = b.video_session_id
    WHERE b.event_timestamp > err.et AND b.event_type != 'VideoError'
    GROUP BY b.video_session_id
) later ON later.video_session_id = err.video_session_id;
-- sessions_with_error = 293, sessions_continuing_after_error = 293
```
`VideoError` is treated as a **liveness signal, not a terminal one** — it doesn't appear in the `st` CASE expression at all (see `pipeline/04_ddl_annotated.sql` step 2), so it neither opens nor closes an interval by itself. Treating it as terminal would have truncated all 293 of these sessions early.

### 6.5 Events arrive out of order (a heartbeat from 2 minutes ago lands after one from 30 seconds ago)

**Not a special case — the aggregation is already order-independent.** `last_seen_ms` is `maxMerge(maxState(ts))` — `max` over a set is invariant to the order its elements were inserted in. Whether a late-arriving timestamp lands before or after a more recent one in wall-clock ingestion order, the *true* maximum is always computed correctly once both have been absorbed. The same applies to `session_start_ms` (`min`), `live_ts`/`tr_state` (`groupUniqArray`/`groupArrayIf` — sets, deduplicated and re-sorted by `ts` at read time in `silver_session_state_current`, not by arrival order).

**What this means concretely:** a session's `version` (used by `ReplacingMergeTree` on `silver_active_intervals`) can only ever move forward as more of a session's true history is absorbed, regardless of the order bronze events physically arrived in.

### 6.6 The very last event a session ever produces is a `pause` — does that break anything?

No — `pause` is a liveness signal (see §5.1), not a distinguished terminal state. If a session pauses and then genuinely goes silent forever with no `VideoSessionEnd`, it's handled identically to any other silent tail: the watermark in §6.1 fires at `last_seen_ms + 50s`, where `last_seen_ms` is the timestamp of that last heartbeat (which fired *during* the pause — heartbeats do not stop when paused, confirmed: 94,590 heartbeat rows recorded while the player state was paused in this dataset). The mechanism doesn't check what the player was doing before it went silent, only *how long* it's been silent.

### 6.7 A session's platform, country, content, or video_type changes mid-session

`silver_session_state` pins every session to **one** dimension tuple via `argMin(dim, event_timestamp)` — the value from the session's *earliest* event, not `any()` (which is non-deterministic across merges and would make the same query return different answers on different runs). 95 sessions in this dataset carry more than one `platform` value over their lifetime (a device switch mid-session); `argMin` makes the choice reproducible.

```sql
-- find sessions that actually changed platform mid-stream
SELECT video_session_id, countDistinct(platform) AS n_platforms
FROM bronze_events_raw GROUP BY video_session_id HAVING n_platforms > 1;
```

---

## 7. Response format the LLM should use

For every answered question:

```
**Answer:** <number(s)>

**Query:**
​```sql
<the exact SQL executed>
​```

**Read:** <row count> rows in <latency>. **Metric used:** cnt_a (concurrency) | cnt_b (reach) — <one line why>.
```

If the question is ambiguous on `cnt_a`/`cnt_b`, or on time-range resolution ("last hour" of what clock), state the interpretation chosen before giving the number.
