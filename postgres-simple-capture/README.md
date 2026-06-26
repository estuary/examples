# Real-Time PostgreSQL CDC Capture with Estuary (Docker + ngrok Demo)

A minimal, self-contained PostgreSQL change data capture (CDC) demo for [Estuary](https://estuary.dev). It spins up a logical-replication-ready PostgreSQL database with Docker, generates a continuous stream of inserts, updates, and deletes against a `public.sales` table, and exposes the database over an ngrok TCP tunnel so Estuary's fully managed `source-postgres` connector can stream every row change into a real-time Estuary collection.

This is the simplest possible end-to-end example for learning how Estuary captures Postgres CDC: no cloud database, no external dependencies beyond Docker and free ngrok/Estuary accounts.

## Architecture

```
┌────────────┐     INSERT/UPDATE/DELETE      ┌──────────────┐
│  datagen   │  ───────────────────────────► │  PostgreSQL  │
│ (Python)   │      public.sales             │ wal_level=   │
└────────────┘                               │   logical    │
                                             └──────┬───────┘
                                                    │ logical replication
                                                    │ (flow_publication)
                                              ┌─────▼──────┐
                                              │   ngrok    │  tcp postgres:5432
                                              │ TCP tunnel │
                                              └─────┬──────┘
                                                    │ public host:port
                                          ┌─────────▼──────────┐
                                          │   Estuary     │
                                          │  source-postgres   │  capture
                                          │        ▼           │
                                          │   collection       │  real-time data lake
                                          └────────────────────┘
```

End-to-end data flow in Estuary terms:

1. **Source** — A local PostgreSQL instance running with `wal_level=logical`, a `flow_capture` replication user, a `flow_publication` publication, and a `flow_watermarks` table (Estuary's CDC bookkeeping table).
2. **Tunnel** — Because Estuary is fully managed, the local database is published to the internet through an ngrok TCP tunnel pointing at `postgres:5432`.
3. **Capture** — Estuary's `source-postgres` connector reads the Postgres write-ahead log (WAL) via the publication and streams every change into an Estuary **collection** (a schematized, real-time JSON dataset backed by cloud storage).
4. **Materialization (optional)** — From the collection you can add a materialization to push rows into any supported destination (BigQuery, Snowflake, Postgres, etc.). That step is left to you.

## What's included

- `docker-compose.yml` — Defines three services: `postgres` (PostgreSQL started with `wal_level=logical`, exposed on port `5432`), `datagen` (the load generator), and `ngrok` (TCP tunnel to `postgres:5432`, dashboard on port `4040`).
- `postgres/init.sql` — Runs on first boot. Creates the `flow_capture` replication user, grants read/write access, creates the `flow_watermarks` table, creates the `flow_publication` publication (with `publish_via_partition_root = true`), creates the `public.sales` table, and adds both tables to the publication.
- `datagen/datagen.py` — Connects to Postgres and, once per second, performs a randomized insert / update / delete (weighted 70% / 10% / 20%) against `public.sales` using Faker-generated data, producing a steady CDC workload.
- `datagen/Dockerfile` — Builds the Python 3.12 generator image.
- `datagen/requirements.txt` — Python dependencies: `Faker==25.1.0`, `psycopg2==2.9.9`.

### The `sales` table

The capture streams the `public.sales` table created by `init.sql`:

| Column        | Type            | Notes                |
|---------------|-----------------|----------------------|
| `sale_id`     | `SERIAL`        | Primary key          |
| `product_id`  | `INTEGER`       | not null             |
| `customer_id` | `INTEGER`       | not null             |
| `sale_date`   | `TIMESTAMP`     | not null             |
| `quantity`    | `INTEGER`       | not null             |
| `unit_price`  | `NUMERIC(10,2)` | not null             |
| `total_price` | `NUMERIC(10,2)` | not null             |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose.
- A free [ngrok account](https://dashboard.ngrok.com/signup) and an authtoken (required to expose the local database to Estuary's hosted connector).
- A free [Estuary account](https://dashboard.estuary.dev).

## Setup

1. Set your ngrok authtoken in the environment (the `ngrok` service reads `NGROK_AUTHTOKEN`):

   ```bash
   export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
   ```

2. Start the stack:

   ```bash
   docker compose up -d
   ```

   This builds the `datagen` image, starts PostgreSQL with `wal_level=logical`, runs `postgres/init.sql`, begins generating change events, and opens the ngrok TCP tunnel.

3. Get the public host and port that Estuary will connect to:

   ```bash
   curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'
   ```

   You can also open the ngrok web dashboard at http://localhost:4040. The URL looks like `tcp://0.tcp.ngrok.io:12345`. **Strip the `tcp://` prefix** — you will paste `0.tcp.ngrok.io` as the host and `12345` as the port into Estuary.

## Configure the Estuary capture

The capture uses Estuary's **PostgreSQL** source connector, `source-postgres` ([connector docs](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)).

### Option A — Estuary dashboard (recommended)

1. Go to [dashboard.estuary.dev/captures](https://dashboard.estuary.dev/captures) and click **New Capture**.
2. Search for and select **PostgreSQL**.
3. Fill in the endpoint configuration using the values from this demo:

   | Field      | Value                                              |
   |------------|----------------------------------------------------|
   | Server Address | the ngrok host:port from the step above (e.g. `0.tcp.ngrok.io:12345`) |
   | Database   | `postgres`                                          |
   | User       | `flow_capture`                                      |
   | Password   | `password`                                          |

4. Click **Next**. Estuary discovers the available tables; select `public.sales` (the `public.flow_watermarks` table is internal bookkeeping and can be left unbound).
5. **Save and Publish**. Estuary backfills the existing rows, then streams new inserts, updates, and deletes from the WAL in real time.

### Option B — flowctl

Prefer the CLI? Authenticate and use [flowctl](https://docs.estuary.dev/concepts/flowctl/):

```bash
flowctl auth login
```

Create a `flow.yaml` similar to the following (replace `your-prefix` with your Estuary tenant prefix and set `address` to the ngrok host:port):

```yaml
captures:
  your-prefix/postgres-simple/source-postgres:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:dev
        config:
          address: 0.tcp.ngrok.io:12345
          database: postgres
          user: flow_capture
          password: password
    bindings:
      - resource:
          namespace: public
          stream: sales
        target: your-prefix/postgres-simple/public/sales
```

Then publish and check status:

```bash
flowctl catalog publish --source flow.yaml --auto-approve
flowctl catalog status your-prefix/postgres-simple/source-postgres
```

## Verify

Confirm data is flowing into the collection:

```bash
flowctl collections read \
  --collection your-prefix/postgres-simple/public/sales \
  --uncommitted | head
```

Or watch live throughput and document counts on the capture's page in the [Estuary dashboard](https://dashboard.estuary.dev/captures). Because `datagen` runs continuously, you should see a steady stream of new documents, plus update and delete events reflecting the WAL changes.

## Next steps

- Add a **materialization** to push the `sales` collection into a destination such as [BigQuery](https://docs.estuary.dev/reference/Connectors/materialization-connectors/BigQuery/), [Snowflake](https://docs.estuary.dev/reference/Connectors/materialization-connectors/Snowflake/), or [PostgreSQL](https://docs.estuary.dev/reference/Connectors/materialization-connectors/PostgreSQL/) from [dashboard.estuary.dev/materializations](https://dashboard.estuary.dev/materializations).
- Transform the collection in SQL, TypeScript, or Python with a [derivation](https://docs.estuary.dev/concepts/derivations/).

## Cleanup

```bash
docker compose down -v
```

Disable or delete the capture in the Estuary dashboard so it stops attempting to reach the (now closed) ngrok tunnel.

## References

- [Estuary documentation](https://docs.estuary.dev)
- [PostgreSQL capture connector reference](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
- [flowctl CLI](https://docs.estuary.dev/concepts/flowctl/)
- [Estuary dashboard](https://dashboard.estuary.dev)
