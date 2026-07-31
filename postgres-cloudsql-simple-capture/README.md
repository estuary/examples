# PostgreSQL CDC Capture for Google Cloud SQL with Estuary

A self-contained example for setting up a real-time **PostgreSQL CDC (Change Data Capture)** pipeline with [Estuary](https://dashboard.estuary.dev), targeting **Google Cloud SQL for PostgreSQL**. It ships a Docker Compose stack (local Postgres + ngrok TCP tunnel) so you can test the Estuary `source-postgres` connector end-to-end without a cloud database, plus a data generator (`datagen/`) written against the **Cloud SQL Python Connector** for streaming continuous inserts, updates, and deletes into a `sales` table on a real Cloud SQL instance.

The Postgres bootstrap (`postgres/init.sql`) provisions everything Estuary needs for logical-replication CDC: a `flow_capture` replication user, a `flow_watermarks` table, and a `flow_publication` publication.

## Architecture

Estuary ingests row-level changes from Postgres via logical replication and lands them in a real-time collection, which you can then materialize to any destination.

```
PostgreSQL (Cloud SQL or local Docker)
   wal_level=logical
   publication: flow_publication
   tables: public.sales, public.flow_watermarks
        │  (logical replication / CDC)
        ▼
Estuary source-postgres capture  ──►  Collection (schematized JSON in your tenant)
        │
        ▼
Materialization (BigQuery, Snowflake, Postgres, …)  ← optional, your choice
```

- **Source / Capture:** the `source-postgres` connector reads inserts, updates, and deletes from `public.sales` over a logical replication slot.
- **Collection:** captured documents are written to an Estuary collection (a real-time data lake of schematized JSON in cloud storage).
- **Materialization:** add a destination connector later to push the collection into your warehouse, database, or lake. Not included here — this example focuses on the capture.

Because Estuary is fully managed, the Postgres database must be reachable from the public internet. For Cloud SQL you expose a public IP and authorized network (or use the connector's SSH tunnel); for the local Docker Postgres in this repo, the included **ngrok** service opens a TCP tunnel.

## What's included

- **`docker-compose.yml`** — spins up three services:
  - `postgres` — `postgres:latest` started with `wal_level=logical`, database `postgres`, user/password `postgres`/`postgres`, exposed on host port `5432`. Mounts `postgres/init.sql` as a Docker entrypoint init script.
  - `datagen` — builds and runs the data generator (see `datagen/`).
  - `ngrok` — `ngrok/ngrok:latest` running `tcp postgres:5432` to expose the local Postgres publicly; its inspection UI is on host port `4040`.
- **`postgres/init.sql`** — runs once on first container start. Creates the `flow_capture` user with `REPLICATION` (password `password`), grants `pg_read_all_data` / `pg_write_all_data`, creates the `public.flow_watermarks` table, creates the `flow_publication` publication (with `publish_via_partition_root = true`), adds `public.flow_watermarks` and `public.sales` to it, and creates the `public.sales` table.
- **`datagen/datagen.py`** — connects to a **Google Cloud SQL** instance using the [Cloud SQL Python Connector](https://github.com/GoogleCloudPlatform/cloud-sql-python-connector) (`pg8000` driver), creates the `sales` table if needed, and loops once per second performing weighted random operations: 70% inserts, 20% deletes, 10% updates. This generates the CDC event stream the Estuary capture consumes.
- **`datagen/Dockerfile`** — `python:3.12` image that installs requirements and runs `python -u datagen.py`.
- **`datagen/requirements.txt`** — `Faker==25.1.0`, `cloud-sql-python-connector[pg8000]`, `SQLAlchemy`, and `python-dotenv`.

### `sales` table schema

| Column        | Type            | Notes                  |
|---------------|-----------------|------------------------|
| `sale_id`     | `SERIAL`        | Primary key            |
| `product_id`  | `INTEGER`       | `NOT NULL`             |
| `customer_id` | `INTEGER`       | `NOT NULL`             |
| `sale_date`   | `TIMESTAMP`     | `NOT NULL`             |
| `quantity`    | `INTEGER`       | `NOT NULL`             |
| `unit_price`  | `NUMERIC(10,2)` | `NOT NULL`             |
| `total_price` | `NUMERIC(10,2)` | `NOT NULL`             |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose (for the local test path).
- A free [Estuary account](https://dashboard.estuary.dev).
- An [ngrok account](https://ngrok.com) and authtoken (required to expose the local Docker Postgres so the hosted connector can reach it).
- **For the Cloud SQL data generator only:** a Google Cloud project with a Cloud SQL for PostgreSQL instance, the Cloud SQL Admin API enabled, and [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials) configured for the Cloud SQL Python Connector.

## Two ways to run

This example supports two paths. The data generator (`datagen.py`) is written for **Cloud SQL**; the Docker Compose stack provides a **local Postgres** so you can test the capture mechanics without a cloud database.

> Note: the `datagen` service env vars in `docker-compose.yml` (`POSTGRES_HOST`, `POSTGRES_PORT`, …) point at the local `postgres` service, but `datagen.py` reads `DB_NAME`, `DB_USER`, `DB_PASSWORD`, and `GCP_PROJECT_ID` / `GCP_REGION` / `GCP_CLOUDSQL_INSTANCE_NAME` to build a Cloud SQL connection. To exercise the generator against real Cloud SQL, run it as a standalone script with those variables set (see below). For a purely local test, the `postgres` service alone is enough to wire up and verify the capture.

### Option A — Local Postgres + ngrok (fast capture test)

Start the stack with your ngrok authtoken set:

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
docker compose up -d
```

Get the public host:port for the Postgres tunnel:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'
# e.g. tcp://2.tcp.ngrok.io:14823
```

Or open the ngrok inspector at <http://localhost:4040> and read the forwarding address. Strip the `tcp://` prefix when pasting into Estuary — you want the host and port separately.

### Option B — Real Google Cloud SQL

Provision your Cloud SQL instance with logical replication enabled, then run `postgres/init.sql` against it to create the `flow_capture` user, `flow_watermarks` table, and `flow_publication`. Run the generator locally against the instance:

```bash
cd datagen
pip install -r requirements.txt

export GCP_PROJECT_ID=<your-project>
export GCP_REGION=<your-region>
export GCP_CLOUDSQL_INSTANCE_NAME=<your-instance>
export DB_NAME=postgres
export DB_USER=postgres
export DB_PASSWORD=<your-password>

python -u datagen.py
```

The script prints the resolved instance connection name (`<project>:<region>:<instance>`) and then logs each insert/update/delete.

## Configure the Estuary capture

Use the [`source-postgres`](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/) connector. Connector image: `ghcr.io/estuary/source-postgres:dev`.

### Via the Estuary dashboard

1. Go to <https://dashboard.estuary.dev/captures> and click **New Capture**.
2. Search for and select **PostgreSQL**.
3. Fill in the endpoint config using the values from this example:

   | Field      | Local (ngrok)                          | Cloud SQL                              |
   |------------|----------------------------------------|----------------------------------------|
   | Server Address | `<host>:<port>` from the ngrok tunnel | Cloud SQL public IP : `5432`           |
   | User       | `flow_capture`                          | `flow_capture`                          |
   | Password   | `password`                              | `password`                              |
   | Database   | `postgres`                              | `postgres`                              |

4. Save and publish. The connector discovers `public.sales` (and `public.flow_watermarks`) and proposes bindings. Keep the `sales` binding to stream the generated data into a collection.

> The `flow_capture` user and `password` are defined in `postgres/init.sql`. Change them for any non-demo deployment.

### Via flowctl

Authenticate and discover from your `source-postgres` config, then publish:

```bash
flowctl auth login
flowctl discover --source flow.yaml   # generates bindings from the endpoint config
flowctl catalog publish --source flow.yaml --auto-approve
```

A minimal capture spec for this source looks like:

```yaml
captures:
  YOUR_PREFIX/postgres-cloudsql/source-postgres:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:dev
        config:
          address: "<host>:<port>"   # ngrok host:port, or Cloud SQL IP:5432
          user: flow_capture
          password: password
          database: postgres
    bindings:
      - resource:
          name: sales
          namespace: public
        target: YOUR_PREFIX/postgres-cloudsql/sales
```

See the [flowctl docs](https://docs.estuary.dev/concepts/flowctl/) for installation and auth details.

## Verify

Confirm data is flowing:

- In the dashboard, open the capture and watch the documents/bytes counters increase as `datagen` writes to `sales`.
- With flowctl, tail the collection:

  ```bash
  flowctl collections read --collection YOUR_PREFIX/postgres-cloudsql/sales --uncommitted | head
  ```

  You should see `sales` rows arriving with `_meta` CDC fields reflecting inserts, updates, and deletes.

## Cleanup

```bash
docker compose down -v
```

The `-v` flag removes the Postgres volume so the next `up` re-runs `init.sql` from scratch.

## Next steps

- Add a [materialization](https://dashboard.estuary.dev/materializations) to push the `sales` collection into BigQuery, Snowflake, Postgres, or another destination.
- Transform the stream with a [derivation](https://docs.estuary.dev/concepts/derivations/) in SQL, TypeScript, or Python.

## References

- Estuary docs: <https://docs.estuary.dev>
- PostgreSQL capture connector reference: <https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/>
- flowctl: <https://docs.estuary.dev/concepts/flowctl/>
- Estuary dashboard: <https://dashboard.estuary.dev>
