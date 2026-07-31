# Real-Time PostgreSQL Shipments Data Generator for Estuary CDC

A self-contained Docker stack that continuously generates realistic, mutating shipments and logistics data in PostgreSQL, wired for change data capture (CDC) with [Estuary](https://dashboard.estuary.dev). The Postgres instance ships with `wal_level=logical`, a replication-ready user, a publication, and a watermarks table, so you can point Estuary's PostgreSQL capture connector at it in minutes and stream inserts, updates, and deletes into a collection, then materialize to any destination (BigQuery, Snowflake, StarTree, ClickHouse, and more) for real-time dashboards and analytics.

Use this as a synthetic backend to test, demo, and benchmark real-time CDC pipelines without needing a production database.

## Architecture

The Python generator drives a steady stream of `INSERT`, `UPDATE`, and `DELETE` operations against the `shipments` table. PostgreSQL's logical replication exposes those changes, an ngrok TCP tunnel makes the local database reachable from Estuary's managed cloud, and Estuary captures the change stream into a collection that can be materialized anywhere.

```
datagen (Python + Faker)
        │  INSERT / UPDATE / DELETE
        ▼
PostgreSQL 17.4  (wal_level=logical, flow_publication, flow_watermarks)
        │  logical replication
        ▼
ngrok TCP tunnel  (postgres:5432 → public host:port)
        │
        ▼
Estuary capture  (source-postgres)
        │
        ▼
Estuary collection  (real-time, schematized JSON)
        │
        ▼
Materialization  →  BigQuery / Snowflake / StarTree / ClickHouse / ...
```

- **Capture (source):** the [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/) reads the logical replication stream from the `public.shipments` table.
- **Collection:** captured change events land in an Estuary collection — a real-time data lake of schematized JSON in cloud storage.
- **Materialization (destination):** push the collection to any [supported destination connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/) to power live dashboards.

## What's included

- **`docker-compose.yml`** — spins up three services: `shipments-datagen` (the generator), `shipments-postgres` (PostgreSQL 17.4 started with `wal_level=logical`, exposed on port `5432`), and `shipments-ngrok` (an ngrok TCP tunnel forwarding `postgres:5432`, with its inspection UI on port `4040`).
- **`postgres/init.sql`** — runs on first boot. Grants the `postgres` user `REPLICATION` and read access, creates the `public.flow_watermarks` table, creates the `flow_publication` publication (with `publish_via_partition_root = true`), defines the `ship_status` enum and `coord` composite type, creates the `shipments` table, and adds both tables to the publication.
- **`datagen/datagen.py`** — the main loop. Randomly inserts new shipments, advances existing shipments through their statuses, updates current locations, and deletes shipments older than 30 days, producing a continuous CDC change feed.
- **`datagen/geo.py`** — geographic helpers: picks the nearest warehouse, estimates transit time (~800 mi/day), and moves shipments along a randomized route toward their destination within the United States.
- **`datagen/Dockerfile`** — builds the generator on `python:3.13.2` and runs `python -u datagen.py`.
- **`datagen/requirements.txt`** — Python dependencies: `Faker`, `geopy`, `psycopg2`.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (with Docker Compose)
- A free [ngrok](https://ngrok.com/) account and authtoken (required to expose the local PostgreSQL database to Estuary's managed service)
- A free [Estuary account](https://dashboard.estuary.dev)

## Setup

1. Add your ngrok authtoken to `docker-compose.yml`, replacing `<YOUR-TOKEN-HERE>` for the `ngrok` service:

   ```yaml
   ngrok:
     environment:
       NGROK_AUTHTOKEN: <YOUR-TOKEN-HERE>
   ```

   Optionally change the `POSTGRES_PASSWORD` (default `postgres`) in both the `datagen` and `postgres` service blocks — keep them in sync.

2. From the `shipments-datagen` directory, start the stack:

   ```bash
   docker compose up -d
   ```

   On first boot, `postgres/init.sql` provisions the replication user, publication, watermarks table, and `shipments` table. The generator begins producing change events immediately.

3. Find the public TCP endpoint created by ngrok. Either open the ngrok inspection UI at [http://localhost:4040](http://localhost:4040) (or the **Endpoints** tab of your ngrok dashboard), or run:

   ```bash
   curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
   ```

   You'll get something like `tcp://0.tcp.ngrok.io:12345`. Strip the `tcp://` prefix — the host and port are what you paste into Estuary.

## Configure the Estuary capture

Create a new capture in the [Estuary dashboard](https://dashboard.estuary.dev/captures) using the **PostgreSQL** connector ([docs](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/), image `ghcr.io/estuary/source-postgres:dev`). Use the values baked into this stack:

| Setting | Value |
| --- | --- |
| Server Address | the ngrok host:port from the step above (e.g. `0.tcp.ngrok.io:12345`) |
| User | `postgres` |
| Password | your `POSTGRES_PASSWORD` (default `postgres`) |
| Database | `postgres` |

The connector auto-discovers `public.shipments` and uses the pre-created `flow_publication` and `public.flow_watermarks`. Save and publish; the capture begins backfilling and then streaming live CDC change events into a new collection.

## Configure the materialization

Once data is flowing into your collection, create a [materialization](https://dashboard.estuary.dev/materializations) to a destination of your choice — pick any [materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/) (BigQuery, Snowflake, ClickHouse, StarTree, and more) and bind it to the `shipments` collection.

This dataset was originally built to drive a real-time StarTree dashboard, but the generator works as a drop-in CDC backend for testing any pipeline setup. Note that `delivery_coordinates` and `current_location` are PostgreSQL composite (`coord`) types — handy for geospatial dashboards.

## Verify

Confirm change events are arriving by reading from the collection with [flowctl](https://docs.estuary.dev/concepts/flowctl/):

```bash
flowctl auth login
flowctl collections read --collection <your/collection/name> --uncommitted | head
```

You can also watch throughput and document counts on the capture's page in the Estuary dashboard, or query your destination table after the materialization is live.

## The data

Generated data is associated with a single `shipments` table:

| Field name | Data type | Description |
| --- | --- | --- |
| `id` | integer | Serial primary key for the table |
| `customer_id` | integer | Randomly generated; indicates foreign key for a fictional `customer` table |
| `order_id` | UUID | Randomly generated universally unique identifier for the order |
| `created_at` | timestamp | Date-time when the order was generated |
| `updated_at` | timestamp | Date-time when the order was last modified |
| `delivery_name` | string | Randomly generated name of a person receiving the delivery |
| `street_address` | string | Randomly generated street address |
| `city` | string | Randomly generated city name |
| `delivery_coordinates` | tuple containing two float values | Randomly generated point within the US |
| `shipment_status` | string/enum | One of: 'Processing', 'In Transit', 'At Checkpoint', 'Out For Delivery', 'Delivered', 'Delayed', depending on current point in the shipment process |
| `current_location` | tuple containing two float values | Shipment's current coordinates; will be somewhere between a set warehouse location and the delivery coordinates |
| `expected_delivery_date` | date | Approximate expected delivery, based on distance and shipment priority |
| `is_priority` | boolean | Whether or not a shipment is considered priority; affects initial processing time |

New shipments are generated roughly every 15-75 seconds. Existing shipments are updated only after their `updated_at` is older than 15 minutes; they then progress through shipment statuses while updating their current locations. Shipments older than 30 days are periodically deleted, producing delete change events for CDC. While initial and ending coordinates are real points within the United States, the route between the two is randomly generated rather than corresponding to actual roads.

The data is meant for demonstration purposes, providing a facsimile of real-time shipping and logistics data.

## Next steps

- Add a [derivation](https://docs.estuary.dev/concepts/derivations/) to transform the `shipments` collection in SQL, TypeScript, or Python — for example, computing on-time delivery rates or per-status counts.
- Materialize to a warehouse or real-time analytics store and build a live logistics dashboard.

## Resources

- [Estuary documentation](https://docs.estuary.dev)
- [PostgreSQL capture connector reference](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
- [Materialization connectors reference](https://docs.estuary.dev/reference/Connectors/materialization-connectors/)
- [flowctl CLI](https://docs.estuary.dev/concepts/flowctl/)
- [Estuary dashboard](https://dashboard.estuary.dev)
