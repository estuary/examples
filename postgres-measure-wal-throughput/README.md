# Measure PostgreSQL WAL Throughput to Size CDC Pipelines with Estuary

Measure PostgreSQL Write-Ahead-Log (WAL) throughput to approximate the change-event volume a Change Data Capture (CDC) pipeline will produce **before** you build it. This example spins up a self-contained PostgreSQL instance (configured for logical replication, exactly like an Estuary CDC source), continuously generates inserts/updates/deletes, samples the current WAL LSN every minute with `pg_cron`, and exposes SQL views that report bytes-per-second and rolling-window WAL rates. The same Postgres is pre-wired with a `flow_capture` replication user, publication, and watermarks table so you can point an Estuary capture at it over an ngrok tunnel and compare measured WAL throughput against real CDC throughput.

Reference blog post: https://estuary.dev/measuring-postgresql-wal-throughput/

## Why measure WAL throughput

WAL is PostgreSQL's source of truth for every committed change, and logical-replication CDC connectors (including Estuary's `source-postgres`) read the change stream from it. The byte volume PostgreSQL writes to the WAL over a time window is a close upper bound on the data a CDC connector has to decode and ship. Sampling `pg_current_wal_lsn()` at fixed intervals and diffing consecutive LSNs with `pg_wal_lsn_diff()` gives you a concrete bytes/second figure you can use to size connectors, estimate egress, and forecast cost without first standing up the full pipeline.

## How it works

The technique is pure SQL plus a scheduler:

```
pg_cron (every 1 min) ──> record_current_wal_lsn()
                              │  INSERT pg_current_wal_lsn() into wal_lsn_history
                              ▼
                        wal_lsn_history (timestamp, lsn_position)
                              │  diff consecutive LSNs with pg_wal_lsn_diff()
                              ├──> wal_volume_analytics  (per-sample bytes & rate)
                              └──> wal_volume_summary    (5 min / 15 min / 1 hr / 1 day rollups)
```

- `record_current_wal_lsn()` captures the current WAL LSN and appends it to `wal_lsn_history`.
- `pg_cron` runs that function once per minute via the scheduled job `Record WAL LSN every minute` (`*/1 * * * *`).
- `wal_volume_analytics` diffs each sample against the previous one to report `wal_bytes_since_previous`, `bytes_per_second`, and a pretty rate.
- `wal_volume_summary` aggregates samples into `Last 5 minutes`, `Last 15 minutes`, `Last hour`, and `Last day` windows with total size and average rate.

When data is actively changing (the `datagen` service drives that), the rates in those views approximate the change-event throughput a CDC pipeline would carry.

### Optional: validate against a real Estuary CDC pipeline

Because the database is already configured for logical replication, you can also stand up a live Estuary capture and compare:

```
PostgreSQL (wal_level=logical, flow_publication, flow_watermarks)
        │  exposed via ngrok TCP tunnel
        ▼
Estuary capture (source-postgres)  ──>  collection (public/sales)
```

The Estuary `source-postgres` connector reads the `public.sales` table through the `flow_publication` publication and streams every insert/update/delete into an Estuary collection, where you can observe the actual document/byte throughput in the dashboard.

## What's included

- `docker-compose.yml` — defines three services:
  - `postgres` (container `postgres-wal-measure`, hostname `postgres`) built from `postgres/Dockerfile`. Started with `wal_level=logical`, `log_statement=all`, and `shared_preload_libraries=pg_cron`. Exposes port `5432`.
  - `datagen` (container `datagen-wal-measure`) built from `datagen/Dockerfile`. Continuously writes to the `sales` table to produce WAL activity.
  - `ngrok` (container `ngrok-wal-measure`, image `ngrok/ngrok:latest`) runs `tcp postgres:5432` to expose the local database to Estuary's managed connectors. The ngrok inspector is published on port `4040`.
- `postgres/Dockerfile` — `postgres:16` plus the `postgresql-16-cron` package (`pg_cron`).
- `postgres/init.sql` — the heart of the example. Creates the `flow_capture` replication user, grants, the `flow_watermarks` table, the `flow_publication` publication, the `sales` table, the `wal_lsn_history` table, the `record_current_wal_lsn()` function, the `wal_volume_analytics` and `wal_volume_summary` views, enables the `pg_cron` extension, and schedules the per-minute LSN sampling job.
- `datagen/Dockerfile` — `python:3.12` image that installs `requirements.txt` and runs `datagen.py`.
- `datagen/datagen.py` — connects to Postgres and loops forever, performing a weighted mix of `insert` (70%), `delete` (20%), and `update` (10%) on the `sales` table, one operation per second.
- `datagen/requirements.txt` — `Faker==25.1.0` and `psycopg2==2.9.9`.

## Prerequisites

- Docker and Docker Compose.
- An ngrok account and authtoken (free tier works) — required only if you want to connect a hosted Estuary capture to the local database. Get a token at https://dashboard.ngrok.com/get-started/your-authtoken.
- A free Estuary account at https://dashboard.estuary.dev — required only for the optional live-CDC comparison.

You do **not** need Estuary, ngrok, or flowctl just to measure WAL throughput. The measurement runs entirely inside the Postgres container.

## Running it

From this directory, set your ngrok token and start the stack:

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
docker compose up --build
```

If you only want to measure WAL throughput and skip the tunnel, start just the database and data generator:

```bash
docker compose up --build postgres datagen
```

On startup, `init.sql` runs automatically, `pg_cron` schedules the per-minute sampling job, and `datagen` begins writing to the `sales` table. Let it run for several minutes so the WAL history accumulates enough samples to diff.

## Inspect WAL throughput

Open a `psql` session inside the running container:

```bash
docker exec -it postgres-wal-measure psql -U postgres -d postgres
```

Per-sample rate (newest first):

```sql
SELECT * FROM wal_volume_analytics;
```

Example columns returned: `wal_bytes_since_previous`, `wal_size_since_previous`, `bytes_per_second`, `rate_pretty`, `seconds_since_previous`.

Rolling-window summary:

```sql
SELECT * FROM wal_volume_summary;
```

Returns one row per window (`Last 5 minutes`, `Last 15 minutes`, `Last hour`, `Last day`) with `samples`, `total_wal_size`, `avg_wal_per_minute`, and `avg_rate`.

Check the raw samples or the live LSN directly:

```sql
SELECT * FROM wal_lsn_history ORDER BY timestamp DESC LIMIT 10;
SELECT pg_current_wal_lsn();
```

The `avg_rate` from `wal_volume_summary` is your headline number: the approximate WAL throughput, and therefore the approximate CDC change-event volume, for the captured workload.

## Optional: connect an Estuary CDC capture

To compare measured WAL throughput against a real Estuary CDC pipeline, expose the database and point a capture at it.

### 1. Get the public ngrok endpoint

With the `ngrok` service running, read the public TCP address from the inspector at http://localhost:4040, or via:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
```

This prints something like `tcp://6.tcp.ngrok.io:14108`. Strip the `tcp://` prefix when pasting into Estuary — you want `6.tcp.ngrok.io` as the host and `14108` as the port.

### 2. Create the capture

In the Estuary dashboard, create a new PostgreSQL capture at https://dashboard.estuary.dev/captures and use the **PostgreSQL** source connector (`source-postgres`). Connector docs: https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/

Use the connection values that `postgres/init.sql` and `docker-compose.yml` provision:

| Field | Value |
| --- | --- |
| Server Address | `<ngrok-host>:<ngrok-port>` (from step 1) |
| Database | `postgres` |
| User | `flow_capture` |
| Password | `password` |

The connector discovers the `public.sales` table, reads it through the `flow_publication` publication, and uses `public.flow_watermarks` for consistent backfills — all already created by `init.sql`. Select the `public.sales` binding and publish the capture.

### 3. Verify and compare

Confirm change events are flowing into the collection:

```bash
flowctl collections read --collection <YOUR_PREFIX>/public/sales --uncommitted | head
```

Then compare:

- **Measured WAL throughput** from `wal_volume_summary.avg_rate`.
- **Actual CDC throughput** from the capture's bytes/docs metrics in the Estuary dashboard, or via `flowctl catalog status <capture-name>`.

The two should track closely, validating WAL sampling as a planning tool for sizing CDC pipelines.

## Next steps

- Adjust the per-minute schedule in `init.sql` (`*/1 * * * *`) to sample more or less frequently for finer/coarser resolution.
- Replace the synthetic `datagen.py` workload with a snapshot of your real write patterns to forecast production CDC volume.
- Use the measured throughput to plan an Estuary capture and materialization to your warehouse or lakehouse of choice.

## Resources

- Blog: [Measuring PostgreSQL WAL Throughput](https://estuary.dev/measuring-postgresql-wal-throughput/)
- Estuary PostgreSQL capture connector docs: https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
- Estuary documentation: https://docs.estuary.dev
- flowctl CLI: https://docs.estuary.dev/concepts/flowctl/
