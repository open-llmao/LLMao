# LLM Query Guide — Conversational Layer over Concurrency Data

> **Purpose.** This file is written to be fed to an LLM (system prompt / grounding context) powering the conversational layer required by the problem statement — *"LibreChat plus the ClickHouse MCP server... 'what was peak concurrency on Android in the last hour?'"*. It is not prose for a human reviewer; it is intent → decision → SQL, so a model can go from a natural-language question straight to a correct, explainable query against this schema.
>
> **Sister docs:** `PIPELINE_LOGIC.md` (why each table exists), `ANSWER.md` (benchmark answers with full reasoning), `TABLES.md` (schema deep-dive).

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
Reports peak, average, and the minute it happened — matches the problem statement's worked example ("minute 1 has 300K... peak = 300K").

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
SELECT sum(c) AS total_reach   -- NOTE: sum across MINUTES is wrong for concurrency,
                                -- but reach at a coarser grain (hour/day) legitimately
                                -- re-aggregates cnt_b if content_id is fixed; for a
                                -- true distinct-viewer count over a range, prefer:
       (SELECT countDistinct(session_id)
        FROM silver_active_intervals
        WHERE content_id = {content_id}
          AND start_ms < {to_ms} AND end_ms > {from_ms}) AS true_distinct_sessions
FROM gold_concurrency_minute
WHERE content_id = {content_id} AND minute BETWEEN {from} AND {to};
```
**Caution baked into the template:** summing `cnt_b` across minutes over-counts sessions that span multiple minutes. For a true "how many distinct viewers over this whole range" answer, always fall back to `countDistinct(session_id)` on `silver_active_intervals`, not a sum of per-minute reach.

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
       WHERE minute BETWEEN {from} AND {to}
       GROUP BY minute, platform )
GROUP BY platform
ORDER BY peak DESC;
```
**Warn the user if they ask for a sum across this table:** `Σ per-platform peaks ≠ global peak` — different platforms peak at different minutes (verified: sum overstates global peak by ~2.7% on this dataset). Only report per-dimension peaks as independent facts, never add them up to claim a global figure.

### 4.6 "Are both models (session-aware vs session-independent) in agreement?" — cross-validation, uses G2

```sql
-- G1 peak
SELECT max(c) AS g1_peak FROM (
    SELECT minute, uniqExactMerge(cnt_a) AS c FROM gold_concurrency_minute GROUP BY minute
);
-- G2 peak, same convention (delta_kind='a' matches cnt_a)
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
Remember: `silver_active_intervals` refreshes on a **bounded 15-second cycle** (see `PIPELINE_LOGIC.md` §2.3) — if the user asks for sub-15s freshness, say the answer may be up to 15s stale and name the mechanism (refreshable MV, `DEPENDS ON` chain), don't claim instant.

### 4.8 "Detecting a concurrency decline" — the optional LLM+ClickStack use case named in the problem statement

```sql
-- flag minutes where concurrency dropped >30% from the prior minute, for a fixed content_id
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

## 5. Answering the five problem-statement questions directly (map to `ANSWER.md` for full reasoning)

| Question | One-line answer the LLM should give | Where the detail lives |
|---|---|---|
| **Q1** How do you define an active interval? | Foreground + heartbeat within 50s; pause counts as active (still holds a player slot); background/silence >50s ends it | `ANSWER.md` Q1, `PIPELINE_LOGIC.md` §2.3 |
| **Q2** How should active ranges be represented? | Hybrid: normalised intervals as source of truth (`silver_active_intervals`), minute occupancy as primary serving (G1), deltas for cross-validation (G2) | `ANSWER.md` Q2 |
| **Q3** Peak/avg without scanning raw history? | Query G1 directly — pre-aggregated, `uniqExactMerge`, no session-level scan, no cumsum needed | §4.1 above, `ANSWER.md` Q3 |
| **Q4** Filter-friendly across dimensions? | ORDER BY prefix `(country, video_type, platform, content_id, minute)` — filters prune before touching most rows | `ANSWER.md` Q4 |
| **Q5** Open sessions absorbing updates? | `silver_session_state` updates with zero gap (incremental MV); intervals + gold on a bounded 15s refresh, chained with `DEPENDS ON` — never a full rebuild | `ANSWER.md` Q5, `PIPELINE_LOGIC.md` §2.3, §3 |

---

## 6. Response format the LLM should use

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
