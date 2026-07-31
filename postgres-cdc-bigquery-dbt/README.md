# Real-Time PostgreSQL CDC to BigQuery with Estuary and dbt

This example demonstrates an end-to-end ELT pipeline that streams change data capture (CDC) events from PostgreSQL into Google BigQuery in real time using [Estuary](https://estuary.dev), then models the raw, append-only data into analytics-ready tables with [dbt](https://www.getdbt.com/). A local Postgres instance with logical replication enabled emits inserts, updates, and deletes that Estuary captures and materializes to BigQuery, where a dbt project deduplicates and incrementalizes the stream into clean `sales` tables.

## Architecture

The pipeline follows the standard Estuary data movement pattern, finished off with an in-warehouse dbt transformation:

```
PostgreSQL (logical replication)
        │  CDC events (insert / update / delete)
        ▼
Estuary capture  ──►  Estuary collection  ──►  Estuary materialization
 (source-postgres)    (schematized JSON)     (materialize-bigquery)
                                                     │
                                                     ▼
                                              BigQuery raw table
                                                     │  dbt run
                                                     ▼
                                       stg_sales (view) ──► sales (incremental)
```

- **Capture**: the `source-postgres` connector reads the Postgres write-ahead log via the `flow_publication` publication and streams every row change into an Estuary **collection** (a real-time, schematized JSON data lake backed by cloud storage). Each document carries Estuary metadata such as `_meta_op` (the CDC operation: `c`/`u`/`d`), `flow_published_at`, and `flow_document`.
- **Materialization**: the `materialize-bigquery` connector continuously pushes the collection into a BigQuery table.
- **Transform**: the dbt project in [`sales_dbt_project/`](./sales_dbt_project) reads that BigQuery table as a source, stages it in `stg_sales`, then builds an **incremental** `sales` model that filters out deletes (`_meta_op != 'd'`) and only processes rows newer than the last run using `flow_published_at`.

## What's included

- **`docker-compose.yml`** — spins up three services: `postgres` (Postgres with `wal_level=logical`, exposed on port `5432`), `datagen` (continuous data generator), and `ngrok` (a TCP tunnel so Estuary's managed connector can reach your local database).
- **`postgres/init.sql`** — initializes Postgres for CDC: grants `REPLICATION` and `pg_read_all_data` to the `postgres` user, creates the `public.flow_watermarks` table, creates the `flow_publication` publication, and creates the `public.sales` table used as the source.
- **`datagen/`** — a Python container (`datagen.py`, `Dockerfile`, `requirements.txt`) that connects to Postgres and continuously inserts (70%), deletes (20%), and updates (10%) rows in the `sales` table every second using [Faker](https://faker.readthedocs.io/), producing a realistic CDC stream.
- **`sales_dbt_project/`** — the dbt project that transforms the materialized BigQuery data. See its own [README](./sales_dbt_project/README.md) and the models in [`sales_dbt_project/models/`](./sales_dbt_project/models).
- **`requirements.txt`** — Python dependencies for running dbt locally (`dbt-core==1.8.0`, `dbt-bigquery==1.8.1`).

### The `sales` source table

`postgres/init.sql` creates the table that the capture reads:

| Column        | Type            | Notes                          |
| ------------- | --------------- | ------------------------------ |
| `sale_id`     | `SERIAL`        | Primary key                    |
| `product_id`  | `INTEGER`       | `NOT NULL`                     |
| `customer_id` | `INTEGER`       | `NOT NULL`                     |
| `sale_date`   | `TIMESTAMP`     | `NOT NULL`                     |
| `quantity`    | `INTEGER`       | `NOT NULL`                     |
| `unit_price`  | `NUMERIC(10,2)` | `NOT NULL`                     |
| `total_price` | `NUMERIC(10,2)` | `NOT NULL`                     |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- A free [ngrok](https://ngrok.com/) account and authtoken (required because Estuary is fully managed and must reach your local Postgres over a public TCP tunnel)
- A free [Estuary account](https://dashboard.estuary.dev)
- A Google Cloud project with **BigQuery** and a [service account JSON key](https://docs.estuary.dev/reference/Connectors/materialization-connectors/BigQuery/) that can write to your target dataset (used by both the Estuary materialization and dbt)
- Python 3.12+ to run dbt locally (`pip install -r requirements.txt`)

## Setup

### 1. Start Postgres, the data generator, and the ngrok tunnel

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
docker compose up --build -d
```

This launches Postgres (initialized via `postgres/init.sql`), starts the `datagen` container generating CDC traffic, and opens an ngrok TCP tunnel to `postgres:5432`.

### 2. Get the public database endpoint

Open the ngrok web UI at [http://localhost:4040](http://localhost:4040), or grab the public address from the command line:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
```

You'll get something like `tcp://6.tcp.ngrok.io:18922`. **Strip the `tcp://` prefix** — the host and port are what you'll paste into Estuary.

## Configure the Estuary capture

Create the PostgreSQL capture in the [Estuary dashboard](https://dashboard.estuary.dev/captures) (search for **PostgreSQL** / `source-postgres`) or with `flowctl`. Use the values from `docker-compose.yml` and `postgres/init.sql`:

| Field            | Value                                            |
| ---------------- | ------------------------------------------------ |
| Server Address   | the ngrok host:port (e.g. `6.tcp.ngrok.io:18922`) |
| User             | `postgres`                                        |
| Password         | `postgres`                                         |
| Database         | `postgres`                                         |

The connector will discover the `public.sales` table (added to `flow_publication` in `init.sql`) and bind it to an Estuary collection. The pre-created `public.flow_watermarks` table and the `wal_level=logical` setting are what make CDC work.

Connector reference: [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/).

## Configure the Estuary materialization

Create a BigQuery materialization from the [dashboard](https://dashboard.estuary.dev/materializations) (search for **BigQuery** / `materialize-bigquery`) and bind the `sales` collection from the capture above. Provide:

- Your Google Cloud **Project ID**
- The target **Dataset** (in this example the dbt project reads from dataset `dani_dev` — set yours and update the dbt source accordingly)
- The **Service Account JSON** key with BigQuery write access
- A **Cloud Storage bucket** Estuary uses to stage data before loading into BigQuery

Connector reference: [BigQuery materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/BigQuery/).

> Note: the dbt project's source ([`sales_dbt_project/models/staging_models.yml`](./sales_dbt_project/models/staging_models.yml)) points at `database: estuary-theatre`, `schema: dani_dev`, `table: sales2`. Update these to match your own BigQuery project, dataset, and the table name your materialization writes to.

## Transform with dbt

Once data is landing in BigQuery, run the dbt project to build the staging view and incremental table.

```bash
pip install -r requirements.txt
```

Configure a `sales_dbt_project` profile in `~/.dbt/profiles.yml` pointing at your BigQuery project (using the same service account), then:

```bash
cd sales_dbt_project
dbt deps
dbt run
dbt test
```

This builds two models:

- **`stg_sales`** — a 1:1 staging view over the Estuary source table that selects the business columns plus Estuary metadata (`_meta_op`, `flow_published_at`, `flow_document`).
- **`sales`** — an `incremental` model that excludes deleted rows (`_meta_op != 'd'`) and, on incremental runs, only ingests rows with `flow_published_at` newer than the latest already loaded, keeping transforms fast and append-friendly.

See [`sales_dbt_project/README.md`](./sales_dbt_project/README.md) and [`sales_dbt_project/models/`](./sales_dbt_project/models) for the model definitions and column documentation.

## Verify

Confirm the pipeline end to end:

- **Capture/materialization metrics**: check the docs read/written counters on each task in the [Estuary dashboard](https://dashboard.estuary.dev).
- **Read the collection** directly with flowctl:

  ```bash
  flowctl collections read --collection <your-tenant>/sales --uncommitted | head
  ```

- **Query BigQuery** to see live data and CDC operations arriving:

  ```sql
  SELECT _meta_op, COUNT(*) FROM `your_project.your_dataset.sales` GROUP BY _meta_op;
  ```

- **Check the dbt output**: after `dbt run`, the `sales` table should contain no rows where `_meta_op = 'd'`.

## Next steps

- Add more source tables to `flow_publication` and re-discover the capture to stream additional collections.
- Extend the dbt project with marts (aggregations, dimensions) on top of the `sales` model.
- Swap the BigQuery materialization for another destination such as [Snowflake](https://docs.estuary.dev/reference/Connectors/materialization-connectors/Snowflake/) or [Databricks](https://docs.estuary.dev/reference/Connectors/materialization-connectors/databricks/).

## Resources

- Blog: [Efficient ELT with Estuary and dbt](https://estuary.dev/efficient-elt-with-estuary-flow-and-dbt/)
- [Estuary documentation](https://docs.estuary.dev)
- [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
- [BigQuery materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/BigQuery/)
- [flowctl CLI](https://docs.estuary.dev/concepts/flowctl/)
