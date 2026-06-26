# Real-Time PostgreSQL CDC to SingleStore with Estuary

Stream PostgreSQL change data capture (CDC) into [SingleStore](https://www.singlestore.com/) in real time using [Estuary](https://estuary.dev). This is the demo environment from the Estuary x SingleStore webinar: a local Postgres instance configured for logical replication, a Python data generator that continuously writes a realistic pet-store order stream, and an ngrok tunnel so the fully managed Estuary connector can reach your local database. Estuary captures every insert and update from Postgres into a collection and materializes it into SingleStore for low-latency analytics.

## Architecture

The pipeline follows the standard Estuary capture -> collection -> materialization pattern:

```
PostgreSQL (wal_level=logical)        Estuary                 SingleStore
+-------------------------+     +------------------------+     +---------------+
| products / transactions |     | source-postgres        |     | analytics     |
| reviews / orders        | --> | capture --> collection | --> | tables /      |
| flow_publication        |     | (real-time data lake)  |     | pipelines     |
+-------------------------+     +------------------------+     +---------------+
        ^                                  ^
        | datagen.py                       | ngrok tcp tunnel
        | (inserts + status updates)       | (exposes local Postgres to Estuary)
```

1. **Capture** — the `source-postgres` connector reads the Postgres write-ahead log (WAL) via the `flow_publication` publication and streams row-level inserts/updates into Estuary collections.
2. **Collections** — each captured table becomes a schematized collection: a real-time data lake of JSON stored in cloud storage.
3. **Materialization** — a SingleStore materialization pushes the collections into SingleStore tables, kept continuously up to date.

Because Estuary is fully managed, your locally running Postgres has to be reachable from the internet. The included ngrok service publishes a TCP tunnel to `postgres:5432` that you paste into the Estuary capture configuration.

## What's included

- **`docker-compose.yml`** — spins up three services:
  - `postgres` (container `postgres-cdc-motherduck-postgres`, image `postgres:latest`) started with `wal_level=logical` for CDC, listening on host port `5432`.
  - `datagen` (container `postgres-cdc-motherduck-datagen`) built from `datagen/`, which continuously writes order activity into Postgres.
  - `ngrok` (container `postgres-cdc-motherduck-ngrok`, image `ngrok/ngrok:latest`) running `tcp postgres:5432`, with its inspection UI on port `4040`.
- **`postgres/init.sql`** — runs on first boot to prepare Postgres for Estuary CDC. It grants the `postgres` user `REPLICATION` and `pg_read_all_data`, creates the `public.flow_watermarks` watermarks table, creates the `flow_publication` publication (with `publish_via_partition_root = true`), creates the `products`, `transactions`, and `reviews` tables, adds all of them to the publication, and seeds the `products` table with a static catalog of pet-store items.
- **`datagen/datagen.py`** — connects to Postgres, creates an `orders` table (UUID primary key via `pgcrypto`), then loops forever generating fake orders with [Faker](https://faker.readthedocs.io/) and randomly advancing their `status` through `placed -> packed -> shipped -> delivered` (or `cancelled`). It can optionally target Google Cloud SQL instead of local Postgres (see env vars below).
- **`datagen/Dockerfile`** — Python 3.12 image that installs `requirements.txt` and runs `datagen.py`.
- **`datagen/requirements.txt`** — Python dependencies: `psycopg2-binary`, `Faker`, `SQLAlchemy`, `pg8000`, `cloud-sql-python-connector[pg8000]`, `python-dotenv`, `openai`.

### Tables captured

| Table | Key | Notes |
| --- | --- | --- |
| `products` | `product_id` | Seeded with a static pet-store catalog in `init.sql`. |
| `transactions` | `transaction_id` | Empty by default; available for the publication. |
| `reviews` | `review_id` | Empty by default; available for the publication. |
| `orders` | `order_id` (UUID) | Created and continuously updated by `datagen.py`. |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- A free [ngrok](https://ngrok.com/) account and authtoken (used to expose local Postgres to Estuary)
- A free [Estuary account](https://dashboard.estuary.dev)
- A [SingleStore](https://www.singlestore.com/) account (Helios/Cloud) with a database and database user for the destination

## Setup

### 1. Set environment variables

The compose file reads `NGROK_AUTHTOKEN` (required) and `OPENAI_API_KEY` (optional; passed to the datagen container but not required for the core pipeline).

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
# Optional:
export OPENAI_API_KEY=<your-openai-key>
```

### 2. Start the stack

```bash
docker compose up -d
```

This starts Postgres with logical replication enabled, applies `postgres/init.sql`, begins generating order data, and opens the ngrok TCP tunnel.

### 3. Get the public Postgres endpoint

Estuary connects to Postgres through ngrok. Read the public `host:port` from the ngrok inspection UI at [http://localhost:4040](http://localhost:4040), or grab it from the command line:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
# e.g. tcp://6.tcp.ngrok.io:18642
```

Strip the `tcp://` prefix — you will paste `6.tcp.ngrok.io` as the host and `18642` as the port into Estuary.

### Running the data generator locally (alternative to Docker)

If you only want to produce data against an existing Postgres without the full compose stack:

```bash
pip install -r datagen/requirements.txt
python datagen/datagen.py
```

Configure the target via environment variables (defaults shown): `POSTGRES_HOST=localhost`, `POSTGRES_PORT=5432`, `POSTGRES_DB=postgres`, `POSTGRES_USER=postgres`, `POSTGRES_PASSWORD=postgres`. To target Google Cloud SQL instead, set `USE_CLOUD_SQL=true` along with `CLOUD_SQL_CONNECTION_NAME`, `CLOUD_SQL_USER`, `CLOUD_SQL_PASSWORD`, and `CLOUD_SQL_DB`.

## Configure the Estuary capture (PostgreSQL CDC)

Create the source capture from the Estuary dashboard.

1. Go to [dashboard.estuary.dev/captures](https://dashboard.estuary.dev/captures) and click **New Capture**.
2. Choose the **PostgreSQL** connector (`source-postgres`).
3. Enter the connection details from `docker-compose.yml` and the ngrok endpoint from step 3 above:

   | Field | Value |
   | --- | --- |
   | Server Address | `<ngrok-host>:<ngrok-port>` (e.g. `6.tcp.ngrok.io:18642`) |
   | Database | `postgres` |
   | User | `postgres` |
   | Password | `postgres` |

4. The connector uses the publication and watermarks table created by `init.sql` (`flow_publication` and `public.flow_watermarks`). Select the tables to capture (`products`, `transactions`, `reviews`, `orders`).
5. Save and publish. Each captured table becomes a collection.

Connector reference: [PostgreSQL capture connector docs](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/).

> The default `postgres`/`postgres` credentials and open ngrok tunnel are fine for a local webinar demo. Do not use them for anything you care about — rotate credentials and lock down access for real workloads.

## Configure the Estuary materialization (SingleStore)

Estuary offers two ways to land Estuary collections in SingleStore. Pick one.

### Option A — direct SingleStore materialization (recommended)

Writes directly into SingleStore tables over the MySQL wire protocol.

1. Go to [dashboard.estuary.dev/materializations](https://dashboard.estuary.dev/materializations) and click **New Materialization**.
2. Choose the **SingleStore** connector (`materialize-singlestore`).
3. Provide your SingleStore connection details:

   | Field | Value |
   | --- | --- |
   | Address | SingleStore host and port, e.g. `svc-abc123.aws-region.svc.singlestore.com:3333` |
   | Database | your SingleStore database name |
   | User | SingleStore database user |
   | Password | SingleStore password |

   For SingleStore Helios/Cloud, expand **Advanced Options**, set SSL Mode to `verify_ca`, and supply SingleStore's TLS/SSL certificate as the SSL Server CA.
4. Bind the Postgres collections (`products`, `orders`, etc.) to destination tables and publish.

Connector reference: [SingleStore materialization connector docs](https://docs.estuary.dev/reference/Connectors/materialization-connectors/MySQL/singlestore-mysql/).

### Option B — Dekaf (SingleStore Kafka ingestion pipeline)

Exposes your collections as Kafka-compatible topics that SingleStore pulls with a native `CREATE PIPELINE ... LOAD DATA KAFKA` statement.

1. Create a **SingleStore (Dekaf)** materialization in Estuary and set an auth token of your choosing.
2. Note the full materialization name (`YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`) — this is the SASL/schema-registry username.
3. In the SingleStore SQL Editor, create a table and a pipeline pointed at the Dekaf broker:

   ```sql
   CREATE PIPELINE orders_pipeline AS
       LOAD DATA KAFKA "dekaf.estuary-data.com:9092/orders"
       CONFIG '{
           "security.protocol":"SASL_SSL",
           "sasl.mechanism":"PLAIN",
           "sasl.username":"YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION",
           "broker.address.family": "v4",
           "schema.registry.username": "YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION",
           "fetch.wait.max.ms": "2000"
       }'
       CREDENTIALS '{
           "sasl.password": "YOUR_AUTH_TOKEN",
           "schema.registry.password": "YOUR_AUTH_TOKEN"
       }'
       INTO TABLE orders
       FORMAT AVRO SCHEMA REGISTRY 'https://dekaf.estuary-data.com'
       ( ... );
   ```

Connector reference: [SingleStore (Dekaf) materialization connector docs](https://docs.estuary.dev/reference/Connectors/materialization-connectors/Dekaf/singlestore/).

## Verify

- In the Estuary dashboard, open your capture and materialization and watch the documents/bytes counters increase as `datagen.py` writes orders.
- Read live from a collection with flowctl:

  ```bash
  flowctl auth login
  flowctl collections read --collection <your-prefix>/orders --uncommitted | head
  ```

- Query the destination in SingleStore to confirm rows are landing and order statuses advance over time:

  ```sql
  SELECT status, COUNT(*) FROM orders GROUP BY status;
  ```

## Teardown

```bash
docker compose down -v
```

## Next steps

- Add a [derivation](https://docs.estuary.dev/concepts/derivations/) to transform the order stream in SQL, TypeScript, or Python (e.g. join `orders` against the seeded `products` catalog, or compute rolling order-status funnels).
- Point the same Postgres collections at additional destinations (Snowflake, BigQuery, ClickHouse) by adding more materializations.
- Swap the local Postgres + ngrok setup for a managed Postgres (RDS, Cloud SQL, Supabase, Neon) and remove the tunnel.

## Resources

- [Estuary documentation](https://docs.estuary.dev)
- [Estuary dashboard](https://dashboard.estuary.dev)
- [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
- [SingleStore materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/MySQL/singlestore-mysql/)
- [SingleStore (Dekaf) materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/Dekaf/singlestore/)
- [flowctl CLI](https://docs.estuary.dev/concepts/flowctl/)
