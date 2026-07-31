# Real-Time Fraud Detection with PostgreSQL CDC to Databricks using Estuary

This example demonstrates a real-time fraud detection pipeline that streams change data capture (CDC) events from PostgreSQL into Databricks using [Estuary](https://estuary.dev). A local Postgres instance with logical replication enabled emits a continuous stream of transaction inserts, updates, and deletes — including deliberately injected anomalies (unusually high and low amounts) — which Estuary captures and materializes into Databricks tables, ready for SQL-based fraud analysis on the lakehouse.

Companion blog post: [Real-Time Fraud Detection with Estuary and Databricks](https://estuary.dev/real-time-fraud-detection-databricks/).

## Architecture

The pipeline follows the standard Estuary data movement pattern: a capture reads the source into collections, and a materialization pushes those collections to the destination.

```
PostgreSQL (wal_level=logical)
        │  CDC events (insert / update / delete) on users + transactions
        ▼
Estuary capture  ──►  Estuary collections  ──►  Estuary materialization
 (source-postgres)    (schematized JSON)      (materialize-databricks)
                                                     │
                                                     ▼
                                          Databricks tables (Unity Catalog)
                                                     │  SQL fraud analysis
                                                     ▼
                                       anomalous transactions surfaced
```

- **Capture**: the `source-postgres` connector reads the Postgres write-ahead log via the `flow_publication` publication and streams every row change into Estuary **collections** (a real-time, schematized JSON data lake backed by cloud storage). Each document carries Estuary metadata such as the CDC operation type, alongside the source columns.
- **Materialization**: the `materialize-databricks` connector continuously pushes the `users` and `transactions` collections into Databricks tables via Unity Catalog.
- **Analysis**: because the anomalous transactions land in Databricks in real time, you can run SQL (or notebooks / SQL alerts) to flag outliers — e.g. amounts far above or below the normal `10.0`–`1000.0` range — as they arrive.

## What's included

- **`docker-compose.yml`** — spins up three services: `postgres` (Postgres with `wal_level=logical`, exposed on port `5432`), `datagen` (a continuous transaction generator), and `ngrok` (a TCP tunnel so Estuary's managed connector can reach your local database). Container names are `postgres-cdc-databricks-postgres`, `postgres-cdc-databricks-datagen`, and `postgres-cdc-databricks-ngrok`.
- **`postgres/init.sql`** — initializes Postgres for CDC: grants `REPLICATION` and `pg_read_all_data` to the `postgres` user, creates the `public.flow_watermarks` table, creates the `flow_publication` publication (with `publish_via_partition_root = true`), adds the `public.users` and `public.transactions` tables to the publication, and seeds 20 fake users.
- **`datagen/`** — a Python container (`datagen.py`, `Dockerfile`, `requirements.txt`) that connects to Postgres and every second performs a random insert (70%), delete (20%), or update (10%) against the `transactions` table using [Faker](https://faker.readthedocs.io/). Roughly 10% of generated transactions are anomalies: a 5% chance of an unusually high amount (`1000.0`–`10000.0`) and a 5% chance of an unusually low amount (`0.01`–`1.0`); the rest are normal (`10.0`–`1000.0`). This produces a realistic CDC stream with fraud-like outliers.

### Source tables

`postgres/init.sql` creates the two tables that the capture reads:

**`public.users`**

| Column              | Type           | Notes                          |
| ------------------- | -------------- | ------------------------------ |
| `user_id`           | `SERIAL`       | Primary key                    |
| `name`              | `VARCHAR(100)` |                                |
| `email`             | `VARCHAR(100)` |                                |
| `registration_date` | `TIMESTAMP`    | Defaults to `CURRENT_TIMESTAMP`|

**`public.transactions`**

| Column             | Type            | Notes                          |
| ------------------ | --------------- | ------------------------------ |
| `transaction_id`   | `SERIAL`        | Primary key                    |
| `user_id`          | `INT`           | References a user (1–20)       |
| `transaction_date` | `TIMESTAMP`     | Defaults to `CURRENT_TIMESTAMP`|
| `amount`           | `DECIMAL(10,2)` | Normal or anomalous value      |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- A free [ngrok](https://ngrok.com/) account and authtoken (required because Estuary is fully managed and must reach your local Postgres over a public TCP tunnel)
- A free [Estuary account](https://dashboard.estuary.dev)
- A [Databricks](https://www.databricks.com/) workspace with a **SQL Warehouse** and a **Unity Catalog** catalog/schema you can write to, plus a [personal access token](https://docs.estuary.dev/reference/Connectors/materialization-connectors/databricks/) for the materialization

## Setup

### 1. Start Postgres, the data generator, and the ngrok tunnel

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
docker compose up --build -d
```

This launches Postgres (initialized via `postgres/init.sql`), starts the `datagen` container generating CDC traffic against the `transactions` table, and opens an ngrok TCP tunnel to `postgres:5432`.

### 2. Get the public database endpoint

Open the ngrok web UI at [http://localhost:4040](http://localhost:4040), or grab the public address from the command line:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
```

You'll get something like `tcp://6.tcp.ngrok.io:18922`. **Strip the `tcp://` prefix** — the host and port are what you'll paste into Estuary.

## Configure the Estuary capture

Create the PostgreSQL capture in the [Estuary dashboard](https://dashboard.estuary.dev/captures) (search for **PostgreSQL** / `source-postgres`) or with `flowctl`. Use the values from `docker-compose.yml` and `postgres/init.sql`:

| Field            | Value                                              |
| ---------------- | -------------------------------------------------- |
| Server Address   | the ngrok host:port (e.g. `6.tcp.ngrok.io:18922`)  |
| User             | `postgres`                                          |
| Password         | `postgres`                                          |
| Database         | `postgres`                                          |

The connector will discover the `public.users` and `public.transactions` tables (added to `flow_publication` in `init.sql`) and bind each to an Estuary collection. The pre-created `public.flow_watermarks` table and the `wal_level=logical` setting are what make CDC work.

Connector reference: [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/).

## Configure the Estuary materialization

Create a Databricks materialization from the [dashboard](https://dashboard.estuary.dev/materializations) (search for **Databricks** / `materialize-databricks`) and bind the `users` and `transactions` collections from the capture above. Provide:

- Your Databricks **Server Hostname** and **HTTP Path** for the target SQL Warehouse
- The Unity Catalog **Catalog** and **Schema** to write into
- A Databricks **personal access token** with write access

Connector reference: [Databricks materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/databricks/).

## Verify

Confirm the pipeline end to end:

- **Capture/materialization metrics**: check the docs read/written counters on each task in the [Estuary dashboard](https://dashboard.estuary.dev).
- **Read a collection** directly with flowctl:

  ```bash
  flowctl collections read --collection <your-tenant>/transactions --uncommitted | head
  ```

- **Query Databricks** to see live data and surface anomalies arriving in real time:

  ```sql
  SELECT transaction_id, user_id, amount, transaction_date
  FROM <catalog>.<schema>.transactions
  WHERE amount > 1000.0 OR amount < 1.0
  ORDER BY transaction_date DESC;
  ```

## Next steps

- Build a Databricks SQL alert or notebook that flags anomalous transactions (e.g. amount above `1000.0` or below `1.0`) as they land.
- Join `transactions` to `users` in Databricks to attribute suspicious activity to specific accounts.
- Add more source tables to `flow_publication` and re-discover the capture to stream additional collections.
- Swap Databricks for another destination such as [Snowflake](https://docs.estuary.dev/reference/Connectors/materialization-connectors/Snowflake/) or [BigQuery](https://docs.estuary.dev/reference/Connectors/materialization-connectors/BigQuery/).

## Resources

- Blog: [Real-Time Fraud Detection with Estuary and Databricks](https://estuary.dev/real-time-fraud-detection-databricks/)
- [Estuary documentation](https://docs.estuary.dev)
- [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
- [Databricks materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/databricks/)
- [flowctl CLI](https://docs.estuary.dev/concepts/flowctl/)
