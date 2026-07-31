# SQL Server CDC to Materialize for Real-Time Analytics with Estuary

Stream change data capture (CDC) events from Microsoft SQL Server into [Materialize](https://materialize.com/) — the streaming operational database — using [Estuary](https://estuary.dev) and the Dekaf Kafka-compatible API. This example spins up a SQL Server instance with CDC enabled, generates a continuous stream of insert/update/delete operations against a `sales` table, captures those changes with Estuary, and consumes them in Materialize as a Kafka source to power an incrementally maintained `sales_anomalies` view.

Companion blog post: https://estuary.dev/cdc-sqlserver-materialize/

## Architecture

```
┌────────────┐   INSERT/UPDATE/DELETE   ┌──────────────┐   CDC capture    ┌──────────────────┐
│  datagen   │ ───────────────────────► │  SQL Server  │ ───────────────► │  Estuary    │
│ (faker)    │      dbo.sales           │  (CDC + MSSQL│   source-        │  collection      │
└────────────┘                          │   Agent)     │   sqlserver      │  .../sales       │
                                        └──────┬───────┘                  └────────┬─────────┘
                                               │ ngrok TCP tunnel                  │ Dekaf
                                               │ (port 1433 -> public)             │ (Kafka API + CSR)
                                               ▼                                   ▼
                                        Estuary connector                  ┌──────────────────┐
                                        reaches local DB                   │   Materialize    │
                                                                           │  KAFKA SOURCE +  │
                                                                           │  sales_anomalies │
                                                                           └──────────────────┘
```

End-to-end flow in Estuary terms:

1. **Capture** — the `source-sqlserver` connector reads the SQL Server CDC change tables for `dbo.sales` and streams every insert, update, and delete into an Estuary **collection**.
2. **Collection** — the change stream lands as schematized JSON in your Estuary collection (a real-time, durable data lake backing the pipeline).
3. **Consume via Dekaf** — instead of a managed materialization connector, Materialize reads the collection directly through Estuary's **Dekaf** Kafka-compatible API. Materialize is configured with a `KAFKA` connection and a `CONFLUENT SCHEMA REGISTRY` connection pointing at Dekaf, then declares a `CREATE SOURCE` over the collection topic with `ENVELOPE UPSERT`.
4. **Transform in Materialize** — the `sales_anomalies` view computes a rolling 7-day per-customer average spend and surfaces sales whose `total_price` exceeds 1.5x that average, kept fresh incrementally by Materialize.

Because Estuary is fully managed, the SQL Server instance running on your machine is exposed to the connector through an `ngrok` TCP tunnel.

## What's included

- **`docker-compose.yml`** — defines three services: `sql-server` (the source database, port `1433`), `datagen` (the load generator), and `ngrok` (TCP tunnel exposing SQL Server to Estuary, web UI on port `4040`).
- **`sqlserver/Dockerfile`** — builds on `mcr.microsoft.com/mssql/server:2022-latest`, copies in `init.sql`, and runs it after the server starts.
- **`sqlserver/init.sql`** — creates the `SampleDB` database, enables CDC at the database level (`sys.sp_cdc_enable_db`), creates the `flow_capture` login/user with `SELECT` on the `dbo` and `cdc` schemas, creates the `dbo.flow_watermarks` table required by the Estuary connector, creates the `dbo.sales` table, and enables CDC on both tables via `sys.sp_cdc_enable_table`.
- **`datagen/datagen.py`** — connects via `pyodbc` and continuously performs weighted random operations against `dbo.sales` (70% inserts, 20% deletes, 10% updates), one per second, using `Faker` to generate realistic sale rows.
- **`datagen/Dockerfile`** + **`datagen/requirements.txt`** — Python 3.12 image with the Microsoft ODBC Driver 18 and `mssql-tools18`; pins `Faker==25.1.0` and `pyodbc==5.1.0`.
- **`materialize-setup.sql`** — the SQL you run inside Materialize: creates the Dekaf `KAFKA` and `CONFLUENT SCHEMA REGISTRY` connections, the `CREATE SOURCE sqlserver_sales` Kafka source, the `sales_anomalies` view, and an index on it.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose.
- A free [ngrok](https://ngrok.com/) account and authtoken (the SQL Server instance runs locally and must be reachable by Estuary's hosted connector).
- A free [Estuary account](https://dashboard.estuary.dev).
- A [Materialize](https://materialize.com/) account (Materialize Cloud or a self-managed instance with the `psql` client).

## Setup

### 1. Start the stack

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
docker compose up --build
```

This builds and starts:

- `sql-server` — SQL Server 2022 (Developer edition) with the SQL Server Agent enabled (required for CDC). The `init.sql` script creates `SampleDB`, the `dbo.sales` table, the `flow_capture` capture user, the `dbo.flow_watermarks` table, and enables CDC.
- `datagen` — begins inserting/updating/deleting rows in `dbo.sales` once the database healthcheck passes.
- `ngrok` — opens a TCP tunnel to `sql-server:1433`.

### 2. Get the public SQL Server endpoint

Open the ngrok inspector at http://localhost:4040, or grab it from the API:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
# e.g. tcp://6.tcp.ngrok.io:14820
```

Strip the `tcp://` prefix — the host (e.g. `6.tcp.ngrok.io`) and port (e.g. `14820`) go into the Estuary capture config.

## Configure the Estuary capture

Create a new SQL Server capture in the [Estuary dashboard](https://dashboard.estuary.dev/captures) (**+ NEW CAPTURE → SQL Server**) using the [`source-sqlserver` connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/SQLServer/). The local stack provisions everything the connector needs, so use these exact values:

| Field | Value |
| --- | --- |
| Server Address | the ngrok host:port from step 2 (e.g. `6.tcp.ngrok.io:14820`) |
| Database | `SampleDB` |
| User | `flow_capture` |
| Password | `Secretsecret1` |

The capture discovers the `dbo.sales` table and writes its CDC stream into a collection (e.g. `<your-prefix>/<capture-name>/sales`). Note the full collection name — you'll reference it as the Dekaf topic in Materialize.

> The `flow_capture` user, its `SELECT` grants on `dbo`/`cdc`, the `dbo.flow_watermarks` table, and database/table-level CDC are all created automatically by `sqlserver/init.sql`. No manual SQL Server setup is required.

## Configure Materialize (consume via Dekaf)

Materialize reads the Estuary collection through [Dekaf](https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/), Estuary's Kafka-compatible API. The `materialize-setup.sql` file contains the full script. Before running it:

1. Generate an Estuary access/refresh token at https://dashboard.estuary.dev/admin/api and paste it into the `estuary_refresh_token` secret.
2. Replace the `TOPIC` value with **your** full collection name from the capture step. The file ships with `Dani/sqlservertest1/sales` as an example — change it to your collection (e.g. `<your-prefix>/<capture-name>/sales`).

Then apply it against Materialize:

```bash
psql "<your-materialize-connection-string>" -f materialize-setup.sql
```

Key statements in `materialize-setup.sql`:

```sql
CREATE SECRET estuary_refresh_token AS '<your-estuary-refresh-token>';

CREATE CONNECTION estuary_connection TO KAFKA (
    BROKER 'dekaf.estuary.dev',
    SECURITY PROTOCOL = 'SASL_SSL',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = '{}',
    SASL PASSWORD = SECRET estuary_refresh_token
);

CREATE CONNECTION csr_estuary_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://dekaf.estuary.dev',
    USERNAME = '{}',
    PASSWORD = SECRET estuary_refresh_token
);

CREATE SOURCE sqlserver_sales
  FROM KAFKA CONNECTION estuary_connection (TOPIC 'Dani/sqlservertest1/sales')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_estuary_connection
    ENVELOPE UPSERT;
```

The script then defines the `sales_anomalies` view (rolling 7-day per-customer average spend, flagging sales above 1.5x that average) and indexes it on `customer_id`.

## Verify

Confirm change events are landing in your Estuary collection from the dashboard, or with `flowctl`:

```bash
flowctl auth login
flowctl collections read --collection <your-prefix>/<capture-name>/sales --uncommitted | head
```

In Materialize, confirm rows are flowing and the anomaly view is populated:

```sql
SELECT count(*) FROM sqlserver_sales;
SELECT * FROM sales_anomalies LIMIT 20;
```

Because `datagen` runs continuously, you'll see counts and anomalies change as new sales stream in.

## Next steps

- Add a [managed materialization](https://dashboard.estuary.dev/materializations) (Snowflake, BigQuery, ClickHouse, Postgres, etc.) on the same `sales` collection to fan out to more destinations.
- Apply a [derivation](https://docs.estuary.dev/concepts/derivations/) to transform the collection in SQL, TypeScript, or Python before it reaches downstream systems.
- Extend `sqlserver/init.sql` with more tables and let the capture discover them.

## Resources

- Blog post: [Real-Time CDC from SQL Server to Materialize](https://estuary.dev/cdc-sqlserver-materialize/)
- Estuary docs: https://docs.estuary.dev
- SQL Server capture connector: https://docs.estuary.dev/reference/Connectors/capture-connectors/SQLServer/
- Reading collections from Kafka (Dekaf): https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/
- flowctl: https://docs.estuary.dev/concepts/flowctl/
