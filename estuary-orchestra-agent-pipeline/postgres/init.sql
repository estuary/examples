-- init.sql
-- Runs automatically the first time the Postgres container starts.
-- It prepares the database for Estuary CDC and creates the orders table
-- that the traffic generator writes to. This is the operational source that
-- the Estuary agent skills will capture and stream into Snowflake.

-- ---------------------------------------------------------------------------
-- The table we capture from.
-- event_ts records when each change happened, which lets the dbt models and
-- Orchestra quality checks reason about freshness later.
-- ---------------------------------------------------------------------------
create table if not exists public.orders (
  order_id      uuid primary key default gen_random_uuid(),
  customer_name text not null,
  amount        numeric(10,2) not null,
  status        text not null default 'pending',
  event_ts      timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

-- ---------------------------------------------------------------------------
-- CDC prerequisites for Estuary.
-- ---------------------------------------------------------------------------

-- A dedicated user with replication rights. The replication role lets the
-- connector create and read from a replication slot, which is Postgres's way
-- of holding your place in the WAL so no changes are discarded before you've
-- read them.
create user flow_capture with password 'secret' replication;
grant pg_read_all_data to flow_capture;

-- A watermarks table the connector uses to coordinate a precise
-- backfill-to-streaming handoff. The connector writes marker values into it
-- during backfill so it can stitch the snapshot and the live stream together
-- without gaps or overlaps.
create table if not exists public.flow_watermarks (
  slot text primary key,
  watermark text
);
grant all privileges on table public.flow_watermarks to flow_capture;

-- A publication: the allowlist of tables whose changes are exposed. Only
-- tables added here emit changes, so you capture exactly what you intend to.
create publication flow_publication
  for table public.orders, public.flow_watermarks;
alter publication flow_publication
  set (publish_via_partition_root = true);
