# Stream MongoDB Clickstream Data to Tinybird in Real Time with Estuary

Capture a live e-commerce clickstream from MongoDB Atlas with Estuary's
MongoDB CDC connector and materialize it into [Tinybird](https://www.tinybird.co/)
for real-time analytics. A small data generator continuously inserts clickstream
events into a MongoDB Atlas collection; Estuary captures every change as it
happens and streams it to a Tinybird Data Source, ready for low-latency SQL and
published API endpoints.

## Architecture

The pipeline is a standard Estuary capture-to-materialization flow:

```
click_stream.csv ──▶ datagen ──▶ MongoDB Atlas ──▶ Estuary capture ──▶ collection ──▶ Estuary materialization ──▶ Tinybird
                  (insert_one)   ecommerce.clickstream  (source-mongodb)              (materialize-tinybird)
```

1. **datagen** reads `datagen/data/click_stream.csv` and inserts each row into the
   `ecommerce.clickstream` collection on MongoDB Atlas, one document every 5
   seconds, to simulate a continuous event stream.
2. An Estuary **capture** (`source-mongodb`) tails the collection's change stream
   and writes each event into a real-time **collection** (schematized JSON backed
   by cloud storage).
3. An Estuary **materialization** (`materialize-tinybird`) pushes the collection
   to a Tinybird Data Source, where you can query it with SQL and expose it as an
   API.

Because MongoDB Atlas is already a public, managed endpoint, no ngrok tunnel is
required — Estuary's hosted connector connects directly to your Atlas cluster.

## What's included

- `docker-compose.yml` — defines the `datagen` service (`container_name:
  mongodb-datagen`) and passes the MongoDB Atlas connection settings as
  environment variables.
- `datagen/datagen.py` — connects to Atlas with `pymongo` over a
  `mongodb+srv://` URI and inserts every CSV row into the target collection,
  sleeping 5 seconds between inserts.
- `datagen/data/click_stream.csv` — the source clickstream dataset. Columns:
  `session_id`, `event_name`, `event_time`, `event_id`, `traffic_source`,
  `event_metadata`.
- `datagen/Dockerfile` — builds the generator on `python:3.11` and runs
  `python -u datagen.py`.
- `datagen/requirements.txt` — pins `pymongo==4.10.1`.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose.
- A **MongoDB Atlas** cluster. The included `docker-compose.yml` points at a
  placeholder host (`cluster0.vun5h.mongodb.net`); replace it with your own.
- A free **Estuary** account: https://dashboard.estuary.dev
- A free **Tinybird** account and a Workspace: https://www.tinybird.co/

## MongoDB Atlas configuration

The capture relies on MongoDB change streams, which require a replica set (every
Atlas cluster is a replica set by default) and a database user with read access
to the target database. Before running anything:

1. Create a database user in Atlas (Database Access) with read access to the
   `ecommerce` database.
2. Allow Estuary's IP addresses through **Network Access** (or temporarily allow
   `0.0.0.0/0` for testing). See the
   [MongoDB connector docs](https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/)
   for the current allowlist.

## Running the data generator

Edit the environment block in `docker-compose.yml` to match your Atlas cluster
and credentials:

```yaml
environment:
  MONGODB_HOST: "cluster0.vun5h.mongodb.net"   # your Atlas SRV host
  MONGODB_PORT: "27017"
  MONGODB_USER: "mongo"
  MONGODB_PASSWORD: "mongo"
  MONGODB_DB: "ecommerce"
  MONGODB_COLLECTION: "clickstream"
```

> The generator builds the connection string as
> `mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_HOST}/`, so
> `MONGODB_HOST` must be the Atlas SRV hostname (the `MONGODB_PORT` value is not
> used by the `+srv` scheme).

Then build and start the generator:

```bash
docker compose up -d --build
```

Watch it insert events:

```bash
docker compose logs -f datagen
```

Each run inserts the full `click_stream.csv` dataset into
`ecommerce.clickstream`, one document every 5 seconds.

## Configure the Estuary capture (MongoDB)

Create the capture in the [Estuary dashboard](https://dashboard.estuary.dev/captures)
(**Sources → New Capture → MongoDB**) using the
[`source-mongodb`](https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/)
connector, or via [flowctl](https://docs.estuary.dev/concepts/flowctl/).

Use the same values you set in `docker-compose.yml`:

| Setting    | Value                                          |
| ---------- | ---------------------------------------------- |
| Address    | `mongodb+srv://cluster0.vun5h.mongodb.net`     |
| User       | `mongo`                                        |
| Password   | `mongo`                                        |
| Database   | `ecommerce`                                    |

The connector discovers the `ecommerce.clickstream` collection and creates a
binding that streams its change stream into an Estuary collection (for example
`your-prefix/mongodb-clickstream/ecommerce/clickstream`).

To authenticate flowctl for CLI-based deploys:

```bash
flowctl auth login
```

## Configure the Estuary materialization (Tinybird)

Create the materialization in the
[Estuary dashboard](https://dashboard.estuary.dev/materializations)
(**Destinations → New Materialization → Tinybird**) using the
[`materialize-tinybird`](https://docs.estuary.dev/reference/Connectors/materialization-connectors/tinybird/)
connector.

You'll need, from your Tinybird Workspace:

- Your Tinybird **region/host** (e.g. `api.us-east.tinybird.co`).
- A Tinybird **Auth Token** with permission to create and append to Data Sources.

Bind the clickstream collection from the capture above to a Tinybird Data Source.
Estuary streams new events directly into Tinybird, where each clickstream event
becomes a row you can query with SQL and expose as a published API endpoint.

## Verify data is flowing

Confirm events are reaching the Estuary collection:

```bash
flowctl collections read \
  --collection your-prefix/mongodb-clickstream/ecommerce/clickstream \
  --uncommitted | head
```

You can also watch live throughput on the capture and materialization tiles in
the [Estuary dashboard](https://dashboard.estuary.dev). On the Tinybird side,
query the Data Source from the Tinybird UI to see clickstream rows arriving in
near real time, for example:

```sql
SELECT event_name, count() AS events
FROM clickstream
GROUP BY event_name
ORDER BY events DESC
```

## Next steps

- Add an Estuary **derivation** (SQL, TypeScript, or Python) to sessionize or
  aggregate events before they reach Tinybird —
  https://docs.estuary.dev/concepts/derivations/
- Fan the same MongoDB collection out to additional destinations (warehouses,
  lakehouses, vector stores) by adding more materializations — a collection can
  power many destinations at once.
- Build Tinybird Pipes and API endpoints on top of the materialized Data Source
  to serve real-time clickstream metrics to your application.

## Resources

- Estuary docs: https://docs.estuary.dev
- MongoDB capture connector: https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/
- Tinybird materialization connector: https://docs.estuary.dev/reference/Connectors/materialization-connectors/tinybird/
- flowctl: https://docs.estuary.dev/concepts/flowctl/
