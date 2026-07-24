-- freshness.sql
-- Run these in a Snowflake worksheet while the generator is still writing to
-- Postgres.

-- Row count and the latest event timestamp. Run it, wait for the next sync,
-- run it again: the count increases and the max timestamp advances.
select count(*), max(event_ts)
from CDC_DEMO.PUBLIC.ORDERS;

-- End-to-end freshness. Because the generator writes continuously,
-- lag_seconds approximates true latency: the time from a row being written in
-- Postgres to that row being queryable in Snowflake.
select
  max(event_ts)                                          as latest_event,
  current_timestamp()                                    as checked_at,
  datediff('second', max(event_ts), current_timestamp()) as lag_seconds
from CDC_DEMO.PUBLIC.ORDERS;
