# Full Outer Join Across Collections with a SQL Derivation in Estuary

This example shows how to implement a **full outer join across two collections** using an **Estuary SQL derivation**. A local PostgreSQL database streams `artists` and `albums` tables into Estuary collections via change data capture (CDC), and a SQLite-backed derivation joins them on `artist_id` to produce a real-time, per-artist rollup of total plays — combining rows that exist in either source, even when a matching row is missing on one side.

The full write-up is here: [How to Join Collections in Estuary with SQL Derivations](https://estuary.dev/derivations-join-collections-sql/).

## Architecture

The pipeline is end-to-end real-time. Inserts into Postgres flow continuously through CDC into source collections, and the derivation reactively re-computes the joined output as new documents arrive.

```text
Postgres (artists, albums)
        │  logical replication (CDC)
        ▼
source-postgres capture  ──►  dani-demo/demo-music/artists
                          └─►  dani-demo/demo-music/albums
                                        │
                                        ▼
                    SQL derivation (SQLite) — full outer join on /artist_id
                                        │
                                        ▼
                       dani-demo/demo-derivations1/artist_total_plays
```

- A **capture** (`source-postgres`) reads the `artists` and `albums` tables from Postgres using logical replication and writes them to two collections.
- A **derivation** consumes both collections. Two transforms (`fromArtists` and `fromAlbums`) are each shuffled on `/artist_id` so related documents land in the same partition, and emit partial documents keyed by `artist_id`.
- The output collection's **reduction annotations** stitch the two sides together: documents are merged by key, `artist_name` is reduced with `maximize` (carries the name from the artists side), and `total_plays` is reduced with `sum` (accumulates plays from the albums side). Because reduction merges on key regardless of which transform produced the document, artists with no albums and albums whose artist row hasn't arrived yet both appear — the behavior of a full outer join.

## What's included

- `docker-compose.yml` — spins up three services: `postgres` (image `postgres:latest`, started with `wal_level=logical` for CDC), `datagen` (continuously inserts fake artists and albums), and `ngrok` (TCP tunnel exposing Postgres `5432` so Estuary's managed connector can reach your local database).
- `postgres/init.sql` — runs on first boot: creates the `flow_capture` replication user, grants read/write, creates the `flow_watermarks` table, creates the `flow_publication` publication, and creates the `artists` and `albums` tables, adding all of them to the publication.
- `datagen/datagen.py` — Python generator using `Faker` and `psycopg2` that inserts one artist plus 1-5 albums per loop, every second.
- `datagen/Dockerfile` / `datagen/requirements.txt` — build the datagen container (`Faker==25.1.0`, `psycopg2==2.9.9`).
- `derivation/flow.yaml` — defines the `dani-demo/demo-derivations1/artist_total_plays` collection and its SQL derivation (the join logic), deployed with `flowctl`.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- A free [ngrok](https://ngrok.com/) account and authtoken (the local DB is exposed through an ngrok TCP tunnel)
- A free [Estuary account](https://dashboard.estuary.dev)
- The [`flowctl` CLI](https://docs.estuary.dev/concepts/flowctl/) (used to publish the derivation)

## Setup

1. Export your ngrok authtoken and start the stack:

   ```bash
   export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
   docker compose up
   ```

   This starts Postgres, begins generating data, and opens the tunnel.

2. Get the public Postgres endpoint from the ngrok tunnel:

   ```bash
   curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'
   ```

   You can also open the ngrok dashboard at [http://localhost:4040](http://localhost:4040). The value looks like `tcp://0.tcp.ngrok.io:12345` — strip the `tcp://` prefix when pasting into Estuary, and split the host and port.

## Configure the Estuary capture

Create the PostgreSQL capture in the [Estuary dashboard](https://dashboard.estuary.dev/captures) (or via `flowctl`) using the **PostgreSQL** connector (`source-postgres`). Connection values come straight from `docker-compose.yml` and `postgres/init.sql`:

| Field      | Value                                   |
| ---------- | --------------------------------------- |
| Server Address | `<ngrok-host>:<ngrok-port>` (from step 2 above) |
| User       | `flow_capture`                          |
| Password   | `password`                              |
| Database   | `postgres`                              |

Select the `public.artists` and `public.albums` tables. To match the derivation's source names in `derivation/flow.yaml`, bind them to the collections `dani-demo/demo-music/artists` and `dani-demo/demo-music/albums` (substitute your own tenant prefix for `dani-demo` and update `flow.yaml` accordingly).

Connector reference: [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/).

## Deploy the SQL derivation

The derivation in `derivation/flow.yaml` joins the two captured collections. Authenticate and publish it with `flowctl`:

```bash
flowctl auth login
flowctl catalog publish --source derivation/flow.yaml --auto-approve
```

The key parts of `derivation/flow.yaml`:

- **Two transforms**, each shuffled on `/artist_id`:
  - `fromArtists` reads `dani-demo/demo-music/artists` and emits `select $artist_id, $name as artist_name;`
  - `fromAlbums` reads `dani-demo/demo-music/albums` and emits `select $artist_id, $total_plays;`
- **Reduction strategy** on the output schema does the join:
  - top-level `reduce: { strategy: merge }`
  - `artist_name` → `maximize`
  - `total_plays` → `sum`
- **Collection key** `/artist_id`, with `artist_id` as the only required field.

> Note: the names in `flow.yaml` use the `dani-demo` tenant prefix. Replace it with your own Estuary tenant in both the capture bindings and `flow.yaml` so the derivation's `source.name` values resolve to your collections.

## Verify

Confirm data is flowing into the derived collection:

```bash
flowctl collections read --collection dani-demo/demo-derivations1/artist_total_plays --uncommitted | head
```

You should see merged documents with `artist_id`, `artist_name`, and an accumulating `total_plays`. You can also watch live throughput on the collection and tasks in the [Estuary dashboard](https://dashboard.estuary.dev).

## Next steps

- Add a **materialization** to push `artist_total_plays` into a warehouse such as [BigQuery](https://docs.estuary.dev/reference/Connectors/materialization-connectors/BigQuery/) or [Snowflake](https://docs.estuary.dev/reference/Connectors/materialization-connectors/Snowflake/).
- Adjust the reduction strategies (for example, swap `maximize` for `lastWriteWins`) to change how the join resolves conflicting values.
- Tear everything down with `docker compose down -v`.

## Resources

- Blog: [How to Join Collections in Estuary with SQL Derivations](https://estuary.dev/derivations-join-collections-sql/)
- [Derivations concept docs](https://docs.estuary.dev/concepts/derivations/)
- [Reductions and reduction annotations](https://docs.estuary.dev/reference/reduction-strategies/)
- [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
- [`flowctl` docs](https://docs.estuary.dev/concepts/flowctl/)
- [Estuary documentation](https://docs.estuary.dev)
