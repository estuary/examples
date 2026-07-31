# Real-Time PostgreSQL CDC to Apache Iceberg Lakehouse with Estuary, AWS Glue & DuckDB/PyIceberg

Build a streaming lakehouse: stream change data capture (CDC) from PostgreSQL into an [Apache Iceberg](https://iceberg.apache.org/) lakehouse on Amazon S3 (cataloged by AWS Glue) using [Estuary](https://estuary.dev), then query the Iceberg tables with PyIceberg / DuckDB. A local Postgres database is seeded and continuously mutated by a data generator, exposed to Estuary's managed connectors via an ngrok TCP tunnel, captured in real time, and materialized into Iceberg where `main.py` reads the `transactions` table and reconstructs the latest row state from CDC operation metadata.

This example accompanies the walkthrough: [Building a Streaming Lakehouse with Estuary and Iceberg](https://estuary.dev/building-streaming-lakehouse-flow-iceberg/).

## Architecture

```
PostgreSQL (logical replication)        Estuary (managed)              Apache Iceberg lakehouse
+-----------------------------+         +----------------------+           +-------------------------+
| users                       |         | source-postgres      |           | AWS Glue catalog        |
| transactions      --CDC-->  |  ngrok  |  capture             | --coll--> | S3 data files           |
| transaction_metadata        | =======>| -> collections       |  materialize| iceberg-rest/glue tables|
+-----------------------------+   TCP   +----------------------+           +-------------------------+
        ^                                                                            |
        | inserts/updates/deletes (datagen)                          PyIceberg/DuckDB (main.py)
                                                                     scan + rebuild latest state
```

Data flow in Estuary terms:

1. **Capture** — the `source-postgres` connector reads the Postgres write-ahead log (logical replication) and streams inserts, updates, and deletes from `public.users`, `public.transactions`, and `public.transaction_metadata`.
2. **Collections** — each table lands in an Estuary collection: a schematized, real-time data lake of JSON documents in cloud storage. Each document carries CDC metadata under `_meta` (including the operation type `_meta.op`: `c`/`u`/`d`).
3. **Materialization** — the Iceberg materialization connector writes the collections to Apache Iceberg tables backed by S3 and registered in the AWS Glue Data Catalog.
4. **Query** — `main.py` uses PyIceberg's `GlueCatalog` to load the `transactions` Iceberg table into pandas and rebuilds the current state by applying the captured operations.

## What's included

| Path | Role |
| --- | --- |
| `docker-compose.yml` | Spins up three services: `postgres-streaming-lakehouse` (Postgres with `wal_level=logical`), `datagen-streaming-lakehouse` (the load generator), and `ngrok-streaming-lakehouse` (a TCP tunnel exposing Postgres to Estuary). |
| `postgres/init.sql` | Runs on first boot: creates the `flow_capture` replication user, grants read/write, creates the `public.flow_watermarks` table, creates the `flow_publication` publication, defines the `users`, `transactions`, and `transaction_metadata` tables, and seeds 20 users. |
| `datagen/datagen.py` | Continuously inserts (70%), deletes (20%), and updates (10%) random transactions once per second, including occasional anomalous amounts, to produce a steady CDC stream. Inserts also write the associated transaction metadata and deletes remove it; updates touch the `transactions` table only. |
| `datagen/Dockerfile` | Builds the Python 3.12 image for the data generator. |
| `datagen/requirements.txt` | Data generator dependencies: `Faker==25.1.0`, `psycopg2==2.9.9`. |
| `main.py` | Reads the materialized `transactions` Iceberg table via PyIceberg `GlueCatalog`, prints stats, and reconstructs latest row state from `_meta.op` (filtering deletes, applying updates, aggregating per user). |
| `requirements.txt` | Query-side dependencies: `pyiceberg==0.6.1`, `boto3==1.34.134`, `pandas`, and `python-dotenv` (imported by `main.py`). |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose.
- A free [ngrok](https://ngrok.com/) account and authtoken — Estuary is fully managed, so the local Postgres must be exposed via an ngrok TCP tunnel for the capture connector to reach it.
- A free [Estuary account](https://dashboard.estuary.dev).
- An AWS account with:
  - An S3 bucket for Iceberg data files.
  - AWS Glue Data Catalog access (the materialization registers Iceberg tables in Glue).
  - IAM credentials (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) with permission to read/write the bucket and manage Glue tables.
- Python 3.12+ to run `main.py` (the query/consumer step).

## Setup

### 1. Start the local stack

Set your ngrok authtoken and bring up the containers:

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
docker compose up
```

This starts Postgres (with logical replication enabled), applies `postgres/init.sql`, launches the data generator, and opens the ngrok tunnel. You should see the generator logging `Inserted new transaction ...` once it connects.

### 2. Get the public Postgres endpoint

The ngrok tunnel forwards `postgres:5432`. Read the public `host:port` from the ngrok dashboard at <http://localhost:4040>, or via the API:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'
```

This prints something like `tcp://6.tcp.ngrok.io:18632`. Strip the `tcp://` prefix when pasting into Estuary (use `6.tcp.ngrok.io:18632`).

## Configure the Estuary capture (PostgreSQL CDC)

The `postgres/init.sql` script has already provisioned everything the `source-postgres` connector needs: the `flow_capture` user, the `flow_watermarks` table, and the `flow_publication` publication. Create the capture in the [Estuary dashboard](https://dashboard.estuary.dev/captures):

1. Go to **Captures → New Capture** and select the **PostgreSQL** connector.
2. Enter the connection details:

   | Field | Value |
   | --- | --- |
   | Server Address | the ngrok `host:port` from step 2 (no `tcp://`) |
   | Database | `postgres` |
   | User | `flow_capture` |
   | Password | `password` |

3. Publish. The connector discovers `public.users`, `public.transactions`, and `public.transaction_metadata` and writes each to an Estuary collection.

Connector reference: [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/).

> The seeded `flow_capture` credentials (`flow_capture` / `password`) and publication name (`flow_publication`) come directly from `postgres/init.sql`. Postgres is started with `-c wal_level=logical` in `docker-compose.yml`.

## Configure the Estuary materialization (Apache Iceberg)

Create a materialization from the captured collections into Apache Iceberg backed by S3 + AWS Glue in the [Estuary dashboard](https://dashboard.estuary.dev/materializations):

1. Go to **Materializations → New Materialization** and choose the **Apache Iceberg** connector.
2. Provide your AWS credentials, S3 bucket, AWS region, and the Glue catalog namespace you want the tables created under.
3. Link the `users`, `transactions`, and `transaction_metadata` collections from the capture above and publish.

Note: the Apache Iceberg materialization connector provisions and runs on AWS EMR Serverless to write Iceberg data files, so you'll also need to supply the EMR Serverless configuration (application/role) required by the connector in addition to the AWS credentials, S3 bucket, region, and Glue namespace — see the connector reference below for the full set of required fields.

Connector reference: [Apache Iceberg materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/apache-iceberg/).

Use the **same** namespace, AWS region, and credentials here that you will pass to `main.py` so the query step can find the materialized tables.

## Verify

Confirm CDC is flowing before querying Iceberg:

- In the Estuary dashboard, watch the capture and materialization metrics climb as the generator runs (it commits an insert/update/delete every second).
- Or tail a collection with flowctl:

  ```bash
  flowctl auth login
  flowctl collections read --collection <your-prefix>/transactions --uncommitted | head
  ```

  Each document includes a `_meta` object; `_meta.op` is `c` (create), `u` (update), or `d` (delete) — this is exactly what `main.py` reads.

## Query the Iceberg lakehouse (PyIceberg / DuckDB)

`main.py` loads the materialized `transactions` table from the AWS Glue catalog and reconstructs the current state from the CDC operations.

Install the query dependencies and set the AWS / namespace environment variables (`main.py` reads them via `python-dotenv`, so an `.env` file also works):

```bash
pip install -r requirements.txt

export AWS_REGION=<your-aws-region>
export AWS_ACCESS_KEY_ID=<your-access-key-id>
export AWS_SECRET_ACCESS_KEY=<your-secret-access-key>
export NAMESPACE=<the-glue-namespace-from-the-materialization>

python main.py
```

What `main.py` does:

- Connects to the AWS Glue catalog and lists namespaces and tables.
- Loads `{NAMESPACE}.transactions` and scans it into a pandas DataFrame.
- Parses `flow_document` JSON to extract `_meta.op` for each row.
- Filters out deletes (`d`), applies updates (`u`) by keeping the last document per `transaction_id`, and aggregates total transaction amount per `user_id`.

> Because the materialization is append-aware CDC, the raw Iceberg table contains the full change history. `main.py` shows the standard pattern for collapsing that history into the latest state — the same logic you would express in DuckDB SQL against the Iceberg tables.

## Next steps

- Add an Estuary [derivation](https://docs.estuary.dev/concepts/derivations/) (SQL, TypeScript, or Python) to compute the latest-state or per-user aggregates inside Estuary instead of in `main.py`.
- Point a different SQL engine at the same Iceberg tables (DuckDB's `iceberg` extension, Trino, Spark, Athena) — the lakehouse is engine-agnostic.
- Swap the local Postgres for a production database; the capture, collections, and materialization stay identical.

## References

- Blog: [Building a Streaming Lakehouse with Estuary and Iceberg](https://estuary.dev/building-streaming-lakehouse-flow-iceberg/)
- [Estuary documentation](https://docs.estuary.dev)
- [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
- [Apache Iceberg materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/apache-iceberg/)
- [flowctl CLI](https://docs.estuary.dev/concepts/flowctl/)
- [PyIceberg](https://py.iceberg.apache.org/) · [DuckDB Iceberg extension](https://duckdb.org/docs/extensions/iceberg.html)
