# Seed a Movies Table as a SQL Source for an Estuary Capture

A minimal demo that creates and seeds a `movies` table in a relational database so it can be used as a source for an [Estuary](https://estuary.dev) capture. Run the single SQL script against PostgreSQL, MySQL, SQL Server, or any ANSI-SQL database, point an Estuary capture at the table, and stream the rows into an Estuary collection in real time.

This is intentionally a "hello world" source: a tiny static dataset (10 Marvel movie rows) to validate a capture connection, walk through the capture setup flow, or seed downstream demos.

## Architecture

The script only provides the **source table**. Estuary does the data movement:

```
movies (SQL table)  ──capture──▶  Estuary collection  ──materialization──▶  destination
   (this script)                  (real-time data lake)                  (warehouse / DB / Kafka)
```

- **Capture (source):** an Estuary capture connector reads the `movies` table and streams rows into an Estuary **collection** — a schematized, real-time copy of the data backed by cloud storage.
- **Materialization (destination):** optionally push the collection to a warehouse, database, or other destination. Not included here — add one once the capture is running.

## What's included

- `create-schema` — a SQL DDL + DML script that:
  - Creates the `movies` table with columns `prod_id` (`INTEGER`, NOT NULL), `prod_price` (`NUMERIC(10,2)`, NOT NULL), and `prod_descrip` (`VARCHAR(100)`, NOT NULL).
  - Adds a unique index `movies_x0` on `prod_id` (the natural primary key, used by the capture as the collection key).
  - Inserts 10 movie rows (`X-Men: Apocalypse`, `Doctor Strange`, `Captain America: Civil War`, ...).

## Prerequisites

- A reachable SQL database (PostgreSQL, MySQL, SQL Server, etc.) where you can create a table.
- A free Estuary account: https://dashboard.estuary.dev
- The database client for your engine (`psql`, `mysql`, `sqlcmd`, ...).
- If the database runs **locally** (not on a public cloud host), expose it to Estuary's managed connectors with an [ngrok](https://ngrok.com) TCP tunnel or an SSH tunnel.

## Setup

Load the schema and seed data into your database. Pick the command for your engine:

```bash
# PostgreSQL
psql "postgres://USER:PASSWORD@HOST:5432/DBNAME" -f create-schema

# MySQL
mysql -h HOST -P 3306 -u USER -pPASSWORD DBNAME < create-schema

# SQL Server
sqlcmd -S HOST,1433 -U USER -P PASSWORD -d DBNAME -i create-schema
```

Verify the rows landed:

```sql
SELECT prod_id, prod_price, prod_descrip FROM movies ORDER BY prod_id;
-- expect 10 rows
```

### Exposing a local database (optional)

Estuary is fully managed, so a database on `localhost` must be reachable from the internet. Expose the DB port with ngrok:

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
ngrok tcp 5432   # use your DB's port: 5432 Postgres, 3306 MySQL, 1433 SQL Server
```

Read the public `host:port` from the ngrok dashboard at http://localhost:4040, or:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
```

Strip the `tcp://` prefix before pasting the address into Estuary.

## Configure the Estuary capture

Create the capture in the Estuary dashboard at https://dashboard.estuary.dev/captures, or via `flowctl`.

1. Choose the capture connector that matches your database engine:
   - PostgreSQL — [`source-postgres`](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
   - MySQL — [`source-mysql`](https://docs.estuary.dev/reference/Connectors/capture-connectors/MySQL/)
   - SQL Server — [`source-sqlserver`](https://docs.estuary.dev/reference/Connectors/capture-connectors/SQLServer/)
2. Enter the connection details — the public host/port (the ngrok address if tunneling), database name, user, and password.
3. In the discovery step, select the `movies` table. Estuary infers the schema and uses the unique key on `prod_id` as the collection key.
4. Save and publish. The 10 rows backfill into the collection; new inserts/updates stream as they happen (for CDC-capable engines, with the prerequisites that connector requires — e.g. `wal_level=logical`, a replication user, and a publication for Postgres).

> CDC connectors require additional source-side setup (replication user, publication/binlog/CDC enablement). See the connector docs linked above for the exact grants and server settings for your engine.

## Verify

Confirm rows are flowing into the collection:

```bash
flowctl auth login
flowctl collections read --collection <your/collection/name> --uncommitted | head
```

Or watch the capture's document and byte counts on its page in the Estuary dashboard.

## Next steps

- Add a **materialization** to land the collection in a destination: https://dashboard.estuary.dev/materializations
- Transform the data with a **derivation** (SQL, TypeScript, or Python): https://docs.estuary.dev/concepts/derivations/

## Resources

- Estuary docs: https://docs.estuary.dev
- flowctl CLI: https://docs.estuary.dev/concepts/flowctl/
- Captures concept: https://docs.estuary.dev/concepts/captures/
- Estuary blog: https://estuary.dev/blog/
