-- ============================================================================
-- 5-OBJECT VARIANT.  Same semantics, intermediates inlined as CTEs.
-- Objects: bronze_events, bronze_content(dict), silver_session_state,
--          silver_active_intervals, gold_concurrency_minute
-- ============================================================================
SET VARIABLE gap_ms = 50000;

CREATE OR REPLACE TABLE bronze_events AS
  SELECT * FROM read_csv_auto('../click-a-thon-2026/SonyLiv/data/ch-hackathon-raw-data.csv');
CREATE OR REPLACE TABLE bronze_content AS
  SELECT * FROM read_csv_auto('../click-a-thon-2026/SonyLiv/data/ch-hackathon-content-data.csv');

-- (3) session state: dims + ordered transitions + liveness, ONE table
CREATE OR REPLACE TABLE silver_session_state AS
WITH e AS (
  SELECT b.video_session_id sid, b.event_timestamp ts, b.platform, b.country,
         b.content_id::BIGINT cid, coalesce(c.video_type,'unk') vt,
         CASE WHEN b.event_type='VideoSessionStart' THEN 1
              WHEN b.event_type='VideoSessionEnd'   THEN -1
              WHEN b.event_type='AppBackgrounded'   THEN 0
              WHEN b.event_type='AppForegrounded'   THEN 1
              WHEN b.event='Play' THEN 1 END st
  FROM bronze_events b LEFT JOIN bronze_content c ON c.content_id=b.content_id)
SELECT sid,
       arg_min(platform,ts) platform, arg_min(country,ts) country,
       arg_min(cid,ts) content_id,   arg_min(vt,ts) video_type,
       min(ts) session_start_ms, max(ts) last_seen_ms,
       max(CASE WHEN st=-1 THEN 1 ELSE 0 END) has_close,
       list(DISTINCT ts) live_ts,
       list({'ts':ts,'st':st}) FILTER (WHERE st IS NOT NULL) transitions
FROM e GROUP BY sid;

-- (4) active intervals: state machine, all CTEs inline
CREATE OR REPLACE TABLE silver_active_intervals AS
WITH exploded AS (
  SELECT sid, platform, country, content_id, video_type, has_close, last_seen_ms,
         unnest(transitions) AS t FROM silver_session_state),
expl AS (SELECT sid,platform,country,content_id,video_type,has_close,last_seen_ms,
                t.ts AS ts, t.st AS st FROM exploded),
pulses AS (SELECT sid, unnest(live_ts) AS ts FROM silver_session_state),
gaps AS (
  SELECT sid, prev + getvariable('gap_ms') AS ts, 0 AS st FROM
    (SELECT sid, ts, lag(ts) OVER (PARTITION BY sid ORDER BY ts) prev FROM pulses)
  WHERE prev IS NOT NULL AND ts-prev > getvariable('gap_ms')
  UNION ALL
  SELECT sid, ts, 1 FROM
    (SELECT sid, ts, lag(ts) OVER (PARTITION BY sid ORDER BY ts) prev FROM pulses)
  WHERE prev IS NOT NULL AND ts-prev > getvariable('gap_ms')),
wm AS (SELECT sid, last_seen_ms + getvariable('gap_ms') AS ts, -1 AS st
       FROM silver_session_state WHERE has_close=0),
allt AS (SELECT sid,ts,min(st) st FROM (
    SELECT sid,ts,st FROM expl
    UNION ALL SELECT sid,ts,st FROM gaps
    UNION ALL SELECT sid,ts,st FROM wm) GROUP BY 1,2),
tr AS (SELECT sid,ts,st FROM (
    SELECT sid,ts,st,lag(st) OVER (PARTITION BY sid ORDER BY ts) pv FROM allt)
  WHERE pv IS NULL OR st IS DISTINCT FROM pv),
iv AS (SELECT sid, ts a, lead(ts) OVER (PARTITION BY sid ORDER BY ts) b, st FROM tr)
SELECT iv.sid AS session_id, iv.a AS start_ms, iv.b AS end_ms,
       s.platform, s.country, s.content_id, s.video_type,
       CASE WHEN s.has_close=0 THEN 1 ELSE 0 END AS is_open
FROM iv JOIN silver_session_state s ON s.sid=iv.sid
WHERE iv.st=1 AND iv.b IS NOT NULL AND iv.b>iv.a;

-- (5) gold: minute occupancy. BOTH bucketing definitions, side by side.
--
--   cnt_a = CONCURRENCY.  Session active AT the minute-boundary instant m*60s.
--           interval [s,e) covers instant t=m*60s  iff  s <= t < e
--           => m from ceil(s/60s) to ceil(e/60s)-1
--           Stable under bucket size (2,599@1s -> 2,581@60s, 1.7% spread).
--
--   cnt_b = REACH.  Session active at ANY point inside minute m.
--           interval [s,e) touches [m*60s,(m+1)*60s)  iff  s < (m+1)*60s AND e > m*60s
--           => m from floor(s/60s) to floor((e-1)/60s)
--           Inflates with bucket size (2,607@1s -> 2,958@60s -> 3,999@300s).
--
-- Both are DISTINCT per (session, minute) to kill the +5.14% double-count.
CREATE OR REPLACE TABLE gold_concurrency_minute AS
WITH a AS (
  SELECT DISTINCT session_id, platform, country, video_type, content_id, m AS minute_id
  FROM silver_active_intervals,
       range(CAST(ceil(start_ms/60000.0) AS BIGINT),
             CAST(ceil(end_ms  /60000.0) AS BIGINT)) t(m)),
b AS (
  SELECT DISTINCT session_id, platform, country, video_type, content_id, m AS minute_id
  FROM silver_active_intervals,
       range(start_ms//60000, (end_ms-1)//60000 + 1) t(m))
SELECT coalesce(a.minute_id, b.minute_id)   AS minute_id,
       coalesce(a.platform,  b.platform)    AS platform,
       coalesce(a.country,   b.country)     AS country,
       coalesce(a.video_type,b.video_type)  AS video_type,
       coalesce(a.content_id,b.content_id)  AS content_id,
       count(a.session_id)                  AS cnt_a,
       count(b.session_id)                  AS cnt_b
FROM a FULL OUTER JOIN b
  ON  a.session_id=b.session_id AND a.minute_id=b.minute_id
  AND a.platform=b.platform AND a.country=b.country
  AND a.video_type=b.video_type AND a.content_id=b.content_id
GROUP BY 1,2,3,4,5;
