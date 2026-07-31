# Real-Time PostgreSQL CDC to MotherDuck with Estuary

Stream a live pet-store order feed from PostgreSQL into [MotherDuck](https://motherduck.com/) (serverless DuckDB) in real time using [Estuary](https://estuary.dev). This demo spins up a local Postgres configured for logical replication, a data generator that continuously inserts and updates orders, and an ngrok TCP tunnel so the fully managed Estuary `source-postgres` connector can reach the database. Change Data Capture (CDC) events flow into an Estuary collection and are materialized into a MotherDuck table you can query with DuckDB SQL.

## Architecture

```
                                  ngrok TCP tunnel
  ┌─────────────┐   inserts/    ┌────────────────┐   :5432    ┌──────────────────────┐
  │  datagen    │──updates────▶ │   PostgreSQL   │ ─────────▶ │  Estuary source-     │
  │ (orders)    │               │ wal_level=     │            │  postgres (CDC)      │
  └─────────────┘               │ logical        │            └──────────┬───────────┘
                                └────────────────┘                       │ capture
                                                                          ▼
                                                            ┌──────────────────────────┐
                                                            │  Estuary collection       │
                                                            │  (schematized JSON)       │
                                                            └──────────┬────────────────┘
                                                                       │ materialization
                                                                       ▼
                                                            ┌──────────────────────────┐
                                                            │  MotherDuck (DuckDB)      │
                                                            └──────────────────────────┘
```

End-to-end flow in Estuary terms:

1. **Capture** — the `source-postgres` connector reads the Postgres write-ahead log (WAL) via logical replication and emits insert/update events as CDC documents.
2. **Collection** — captured documents land in an Estuary collection, a real-time, schematized JSON data lake backed by cloud storage.
3. **Materialization** — the `materialize-motherduck` connector continuously pushes the collection into a MotherDuck table.

Because Estuary is fully managed and hosted, the locally-running Postgres is exposed through an ngrok TCP tunnel so the cloud connector can connect to it.

## What's included

- **`docker-compose.yml`** — defines three services:
  - `postgres` (container `postgres-cdc-motherduck-postgres`, image `postgres:latest`) started with `wal_level=logical`, listening on port `5432`, seeded from `postgres/init.sql`.
  - `datagen` (container `postgres-cdc-motherduck-datagen`) builds the `datagen/` image and continuously writes order data.
  - `ngrok` (container `postgres-cdc-motherduck-ngrok`) runs `tcp postgres:5432` to publish a public TCP endpoint; its inspector UI is exposed on port `4040`.
- **`postgres/init.sql`** — bootstraps Postgres for CDC: grants `REPLICATION` and `pg_read_all_data` to the `postgres` user, creates the `public.flow_watermarks` table required by the connector, creates the `flow_publication` publication (with `publish_via_partition_root = true`), and seeds demo tables `products`, `transactions`, and `reviews`.
- **`datagen/datagen.py`** — creates the `orders` table at runtime (`pgcrypto` extension + `gen_random_uuid()` primary key) and loops forever: ~70% of the time it inserts a new order in `placed` status, ~30% of the time it advances a random open order through the lifecycle `placed → packed → shipped → delivered` (or `cancelled`). Supports an optional Google Cloud SQL connection path via `USE_CLOUD_SQL`.
- **`datagen/Dockerfile`** — Python 3.12 image that installs `datagen/requirements.txt` and runs `datagen.py`.
- **`datagen/requirements.txt`** — Python dependencies (`psycopg2-binary`, `Faker`, `SQLAlchemy`, `pg8000`, `cloud-sql-python-connector`, `python-dotenv`).

### Demo data: the `orders` table

The generator drives change events against this table:

| Column          | Type          | Notes                                                        |
| --------------- | ------------- | ----------------------------------------------------------- |
| `order_id`      | `UUID`        | Primary key, `DEFAULT gen_random_uuid()`                    |
| `customer_name` | `TEXT`        | Faker-generated name                                         |
| `product_name`  | `TEXT`        | One of 10 pet-store products                                 |
| `status`        | `TEXT`        | `placed` / `packed` / `shipped` / `delivered` / `cancelled` |
| `created_at`    | `TIMESTAMPTZ` | `DEFAULT now()`                                              |

The `status` updates are what make this a good CDC demo: each row changes over time, and those updates are streamed downstream in real time.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose.
- A free [ngrok](https://ngrok.com/) account and authtoken (the local Postgres is tunneled so Estuary can reach it).
- A free [Estuary account](https://dashboard.estuary.dev).
- A [MotherDuck](https://motherduck.com/) account and a service/access token for the materialization.

## Setup

1. Clone the repo and change into this directory:

   ```bash
   cd estuary-motherduck-orders
   ```

2. Export your ngrok authtoken (read by the `ngrok` service):

   ```bash
   export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
   ```

3. Start everything:

   ```bash
   docker compose up --build
   ```

   This launches Postgres with logical replication, the order generator, and the ngrok tunnel. You should see `Inserted new order.` and `Updated order ... → ...` log lines from the `datagen` container.

> Note: the `datagen` service references an `OPENAI_API_KEY` environment variable. It is not used by `datagen.py`, so you can leave it unset.

### Get the public Postgres endpoint

The ngrok tunnel maps a public `host:port` to the local Postgres. Open the inspector UI at [http://localhost:4040](http://localhost:4040), or grab it from the API:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
```

You'll get something like `tcp://6.tcp.ngrok.io:18923`. **Strip the `tcp://` prefix** — the host is `6.tcp.ngrok.io` and the port is `18923` — when entering it into Estuary.

### Make sure the `orders` table is captured

`postgres/init.sql` adds `transactions`, `products`, and `reviews` to `flow_publication`, but the `orders` table is created later by the generator. Add it to the publication so its CDC events are replicated:

```bash
docker exec -it postgres-cdc-motherduck-postgres \
  psql -U postgres -d postgres \
  -c "ALTER PUBLICATION flow_publication ADD TABLE public.orders;"
```

## Configure the Estuary capture

You can wire this up entirely from the [Estuary dashboard](https://dashboard.estuary.dev).

1. Go to [dashboard.estuary.dev/captures](https://dashboard.estuary.dev/captures) and create a new capture using the **PostgreSQL** connector (`source-postgres`).
2. Enter the connection details from `docker-compose.yml`, using the public endpoint from ngrok:

   | Field      | Value                                |
   | ---------- | ------------------------------------ |
   | Server Address | `<ngrok-host>:<ngrok-port>` (e.g. `6.tcp.ngrok.io:18923`) |
   | User       | `postgres`                           |
   | Password   | `postgres`                           |
   | Database   | `postgres`                           |

3. Run discovery and select the `orders` table (and any of `products`, `transactions`, `reviews` you want to stream). Save and publish.

The connector uses the pre-created `flow_publication` and `public.flow_watermarks` from `init.sql`.

PostgreSQL capture connector reference: https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/

## Configure the Estuary materialization

1. Go to [dashboard.estuary.dev/materializations](https://dashboard.estuary.dev/materializations) and create a new materialization using the **MotherDuck** connector (`materialize-motherduck`).
2. Provide your MotherDuck connection settings:
   - **Token** — your MotherDuck service/access token.
   - **Database** — the target MotherDuck database name.
   - **Schema** — the destination schema (e.g. `main`).
3. Bind the capture's `orders` collection to a destination MotherDuck table and publish.

MotherDuck materialization connector reference: https://docs.estuary.dev/reference/Connectors/materialization-connectors/motherduck/

## Verify

- In the Estuary dashboard, open the capture and materialization and watch the **docs / bytes** metrics climb as the generator keeps writing.
- If you use [flowctl](https://docs.estuary.dev/concepts/flowctl/), tail the collection directly:

  ```bash
  flowctl auth login
  flowctl collections read --collection <your-prefix>/orders --uncommitted | head
  ```

- Query the destination in MotherDuck:

  ```sql
  SELECT status, COUNT(*) AS orders
  FROM your_database.main.orders
  GROUP BY status
  ORDER BY orders DESC;
  ```

  Re-run it after a minute — counts shift as orders advance through their lifecycle, confirming updates are streaming end to end.

## Next steps

- Add the other seeded tables (`products`, `transactions`, `reviews`) to the same capture for a richer model.
- Transform the stream with a [derivation](https://docs.estuary.dev/concepts/derivations/) in SQL, TypeScript, or Python (e.g. compute current-status counts or per-product revenue).
- Fan the same collection out to additional destinations (Snowflake, BigQuery, ClickHouse) via more materializations.

## Resources

- Estuary docs: https://docs.estuary.dev
- PostgreSQL capture connector: https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
- MotherDuck materialization connector: https://docs.estuary.dev/reference/Connectors/materialization-connectors/motherduck/
- flowctl CLI: https://docs.estuary.dev/concepts/flowctl/
- Estuary dashboard: https://dashboard.estuary.dev
