# Real-Time CDC from PostgreSQL to Snowflake

Companion repo for the Estuary walkthrough
[*Real-Time CDC From PostgreSQL to Snowflake, Step by Step*](https://estuary.dev/blog/).
Follow the blog post for the full guide — this repo is what you clone to follow along.

It ships a Dockerized Postgres pre-configured for logical replication and a
traffic generator that continuously writes orders (inserts, updates, deletes),
so you always have live changes to stream.

## Quick start

```bash
docker compose up -d          # Postgres + traffic generator
ngrok tcp 5432                # expose Postgres so Estuary can reach it
```

Then create the capture and Snowflake materialization in the
[Estuary dashboard](https://dashboard.estuary.dev), as described in the post.

Tear down with `docker compose down -v`.

## What's here

| Path | What it is |
|------|------------|
| `docker-compose.yml` | Postgres (`wal_level=logical`) + generator |
| `postgres/init.sql` | `orders` table and CDC prerequisites (replication user, publication, watermarks) |
| `generator/` | Python order generator |
| `snowflake/setup.sql` | Snowflake database, role, user, and warehouse |
| `snowflake/freshness.sql` | end-to-end latency query |
| `flow/` | declarative `flowctl` specs for the capture and materialization |
