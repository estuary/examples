# Real-Time Freight Shipment ETA Tracking with Estuary, MongoDB, and Tinybird

Stream live freight shipment events out of MongoDB with Estuary's change data capture (CDC), consume those collections from Tinybird over Estuary's Kafka-compatible Dekaf API, compute updated ETAs and delay analytics in ClickHouse SQL, and visualize everything in a real-time Next.js dashboard.

This example simulates a logistics workload: shipments are continuously inserted, updated (locations, statuses, delays), and enriched with traffic/weather data in MongoDB. Estuary captures every change in real time, materializes it into Tinybird, and a published Tinybird Pipe joins shipments against current traffic/weather to recompute each shipment's expected delivery time on the fly.

Reference article: https://estuary.dev/real-time-freight-tracking-estuary-tinybird/

## Architecture

```
MongoDB (shipping db)                Estuary                       Tinybird                       Next.js dashboard
┌─────────────────────┐    capture    ┌──────────────────┐   Dekaf      ┌──────────────────────┐       ┌────────────────┐
│ shipments           │ ───────────▶  │ collections      │  (Kafka API) │ Data Sources         │       │ Tremor charts  │
│ checkpoints         │  source-      │ shipping/...     │ ───────────▶ │ shipments            │       │ - delayed cust │
│ traffic_weather     │  mongodb      │ (real-time lake) │              │ checkpoints          │  HTTP │ - route perf   │
│ (datagen CDC sim)   │               │                  │              │ traffic_weather      │ ◀──── │ - status dist  │
└─────────────────────┘               └──────────────────┘              │ + Shipping.pipe      │       └────────────────┘
                                                                        └──────────────────────┘
```

Data flow in Estuary terms:

- **Capture (source):** the `source-mongodb` connector reads the `shipping` database and streams inserts/updates/deletes from the `shipments`, `checkpoints`, and `traffic_weather` collections.
- **Collections:** each MongoDB collection lands in an Estuary collection (a real-time data lake of schematized JSON), e.g. `Dani/shipments-demo/shipping/shipments`.
- **Consumption (Dekaf):** Tinybird reads those collections directly through Estuary's Kafka-compatible **Dekaf** API. Each Tinybird Data Source is bound to a Kafka topic (`KAFKA_TOPIC`) through a named Tinybird Kafka connection (`KAFKA_CONNECTION_NAME`) that maps to an Estuary collection.
- **Transformation (Tinybird):** `Shipping.pipe` and `eta.sql` run ClickHouse SQL to deduplicate to the latest version of each shipment, join against the latest traffic/weather row per route, and recompute ETAs (`expected_delivery_date + INTERVAL impact_on_ETA_minutes MINUTE`).
- **Visualization:** the Next.js dashboard calls published Tinybird Pipe endpoints and renders the metrics with Tremor charts.

## What's included

| Path | Role |
| --- | --- |
| `docker-compose.yml` | Spins up the `datagen` service (container `mongodb-shipping-datagen`) that simulates the live MongoDB CDC workload. |
| `datagen/datagen.py` | Connects to MongoDB and continuously inserts new shipments, updates statuses/locations/delays, and emits traffic/weather rows into the `shipping` database (`shipments`, `checkpoints`, `traffic_weather` collections). |
| `datagen/requirements.txt` | Python deps for the generator: `pymongo[srv]`, `faker`, `python-dotenv`. |
| `datagen/Dockerfile` | Builds the generator image (`python:3.11`, runs `python -u datagen.py`). |
| `tinybird/datasources/shipments.datasource` | Tinybird Data Source bound to the Estuary collection topic `Dani/shipments-demo/shipping/shipments` via Dekaf; flattens nested arrays (`delays[:]`, `events[:]`) and `current_location` into columns. |
| `tinybird/datasources/checkpoints.datasource` | Data Source for the `checkpoints` collection topic. |
| `tinybird/datasources/traffic_weather.datasource` | Data Source for the `traffic_weather` collection topic (route conditions and `impact_on_ETA_minutes`). |
| `tinybird/pipes/Shipping.pipe` | Multi-node Pipe: latest shipment per `shipment_id`, latest traffic/weather per `route_id`, per-shipment delays, route performance, status distribution, and top delayed customers. |
| `tinybird/eta.sql` | Standalone ClickHouse query that joins `shipments` to `traffic_weather` and computes `updated_eta` for in-transit shipments. |
| `dashboard/` | Next.js 15 + Tremor app that queries the published Tinybird Pipe endpoints and renders charts. (See `dashboard/README.md` for the standard Next.js commands.) |

## Prerequisites

- **Docker** with Compose (to run the data generator).
- A **MongoDB** instance Estuary can reach. `datagen/datagen.py` defaults to a MongoDB Atlas `mongodb+srv://` connection; the easiest path is a free [MongoDB Atlas](https://www.mongodb.com/atlas) cluster (which is publicly reachable, so no tunnel is needed). For CDC, MongoDB must run as a replica set (Atlas clusters always do).
- A free **Estuary** account: https://dashboard.estuary.dev
- A **Tinybird** account (free tier is fine): https://www.tinybird.co
- **Node.js 18+** (only for the `dashboard/` app).

## Step 1 — Generate the MongoDB CDC workload

`datagen/datagen.py` reads its MongoDB connection from environment variables (`MONGODB_USER`, `MONGODB_PASSWORD`, `MONGODB_HOST`) and builds a `mongodb+srv://` connection string. The bundled `docker-compose.yml` passes these through from your shell (`${VAR:-default}` substitution), so export them and start the generator:

```bash
# From the shipments_eta/ directory.
export MONGODB_USER=<your-mongodb-user>
export MONGODB_PASSWORD=<your-mongodb-password>
export MONGODB_HOST=<your-cluster-host>   # e.g. cluster0.xxxxx.mongodb.net

docker compose up --build
```

`datagen.py` builds the connection string as:

```
mongodb+srv://${MONGODB_USER}:${MONGODB_PASSWORD}@${MONGODB_HOST}/?retryWrites=true&w=majority&appName=Cluster0
```

It then loops forever in `simulate_cdc_workload()`:

- inserts 2–5 new shipments per cycle into `shipping.shipments`,
- updates non-delivered shipments with new `status`, `current_location`, `events`, and `delays`,
- periodically inserts traffic/weather rows into `shipping.traffic_weather`,
- seeds 10 `checkpoints` once on startup.

Watch the logs to confirm inserts/updates are happening:

```bash
docker compose logs -f datagen
```

> Note: the bundled `docker-compose.yml` defines only the generator and passes `MONGODB_HOST`, `MONGODB_USER`, and `MONGODB_PASSWORD` through from your shell via `${VAR:-default}` substitution, so the `export`s above take effect. The defaults are non-functional placeholders — set all three. Because `datagen.py` connects with `mongodb+srv://`, no port is needed. If you run MongoDB locally instead of Atlas, expose it to Estuary's managed connectors with a publicly reachable host or an ngrok TCP tunnel (`ngrok tcp 27017`).

## Step 2 — Configure the Estuary MongoDB capture

Capture the three collections from your MongoDB `shipping` database into Estuary collections.

Using the dashboard:

1. Open https://dashboard.estuary.dev/captures and click **New Capture**.
2. Choose the **MongoDB** connector (`source-mongodb`).
3. Enter your connection details:
   - **Address:** your MongoDB host (e.g. `cluster0.xxxxx.mongodb.net` for Atlas, or the ngrok host for a local DB).
   - **User / Password:** the `MONGODB_USER` / `MONGODB_PASSWORD` you set above.
   - **Database:** `shipping`.
4. Let the connector discover collections and bind `shipments`, `checkpoints`, and `traffic_weather`.
5. Publish. Estuary begins backfilling and then streaming change events into collections such as `<your-prefix>/shipping/shipments`.

Prefer the CLI? This repo doesn't ship an Estuary spec, so create one yourself: run `flowctl auth login`, scaffold a capture spec with `flowctl raw discover --connector ghcr.io/estuary/source-mongodb:dev` (or `flowctl catalog pull-specs` to edit an existing draft), then publish your spec with `flowctl catalog publish --source <your-spec>.yaml`. See https://docs.estuary.dev/concepts/flowctl/

MongoDB capture connector docs: https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/

> The Data Sources in this repo reference topics under the `Dani/shipments-demo/shipping/...` prefix. Replace `Dani/shipments-demo` with your own Estuary tenant/prefix when you wire up Tinybird.

## Step 3 — Connect Tinybird to Estuary via Dekaf

Tinybird consumes the Estuary collections through Estuary's Kafka-compatible **Dekaf** API. In Tinybird, create a Kafka connection (for example, named `Estuary`) with these settings:

- **Bootstrap servers:** `dekaf.estuary-data.com:9092`
- **Security protocol:** `SASL_SSL`
- **SASL mechanism:** `PLAIN`
- **Username:** your Dekaf task name (or `{}` for public demo topics)
- **Password:** an Estuary access/refresh token (generate one in the Estuary dashboard)
- **Schema registry:** `https://dekaf.estuary-data.com`

Then create the three Data Sources, each bound to the Kafka topic that matches its Estuary collection:

| Data Source | Kafka topic |
| --- | --- |
| `shipments` | `Dani/shipments-demo/shipping/shipments` |
| `checkpoints` | `Dani/shipments-demo/shipping/checkpoints` |
| `traffic_weather` | `Dani/shipments-demo/shipping/traffic_weather` |

The `.datasource` files already declare the JSON-path schema mappings (for example `delays__reason Array(String) json:$.delays[:].reason` and `current_location_latitude Nullable(Float32) json:$.current_location.latitude`), so update each file's `KAFKA_CONNECTION_NAME` (to match the connection you created above) and its `KAFKA_TOPIC` / `KAFKA_GROUP_ID` lines (to your tenant prefix), then push them with the Tinybird CLI:

```bash
# Authenticate the Tinybird CLI first (tb auth), then from the tinybird/ directory:
tb push datasources/shipments.datasource
tb push datasources/checkpoints.datasource
tb push datasources/traffic_weather.datasource
tb push pipes/Shipping.pipe
```

Dekaf docs: https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/

## Step 4 — Build the ETA / analytics transformations

The transformations run as ClickHouse SQL inside Tinybird:

- **`Shipping.pipe`** is the main multi-node Pipe:
  - `latest_shipments` — dedupes to the most recent row per `shipment_id` using `ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY __timestamp DESC)`.
  - `latest_traffic_weather` — most recent traffic/weather row per `route_id`.
  - `delays` — joins latest shipments to latest traffic/weather and computes `updated_eta = expected_delivery_date + INTERVAL impact_on_ETA_minutes MINUTE`, plus total/avg delay minutes per shipment.
  - `route_performance` — congestion insights aggregated per origin–destination route.
  - `shipment_status_distribution` — status counts per route.
  - `top_delayed_customers` — customers with the most cumulative delay minutes.
- **`eta.sql`** is a standalone query showing the core ETA recompute: join `shipments` to `traffic_weather` on `route_id` for `status = 'In Transit'` and return original vs. updated ETA with delay reasons. It uses `arrayStringConcat(s.delays__reason, ', ')` to match the flattened `Array(String)` column `delays__reason` in `shipments.datasource`.

Publish the relevant Pipe nodes as API endpoints (`Shipping.json`, `route_perf.json`, `route_staus_stats.json`) so the dashboard can query them.

## Step 5 — Run the dashboard

The dashboard is a Next.js 15 app that reads the published Tinybird endpoints with Tremor charts.

```bash
# From the dashboard/ directory.
npm install
npm run dev
# open http://localhost:3000
```

Set your Tinybird host and token in `dashboard/src/app/page.tsx` before running. The file currently ships with a `"xyz"` placeholder token — replace it with your Tinybird read token:

```ts
const TINYBIRD_API_BASE = "https://api.us-east.tinybird.co/v0/pipes/Shipping.json";
const TINYBIRD_TOKEN = "xyz" // replace "xyz" with your Tinybird read token
```

The page renders three panels driven by the Tinybird Pipe endpoints:

- **Top Delayed Customers (minutes)** — from `Shipping.json`.
- **Average Delay by Route (minutes)** — from `route_perf.json`.
- **Status Distribution per route** — from `route_staus_stats.json`.

## Verify

- **Generator:** `docker compose logs -f datagen` should show `Inserted N new shipments.` / `Updated N shipments.` lines.
- **Estuary collection:** confirm change events are flowing:

  ```bash
  flowctl collections read --collection <your-prefix>/shipping/shipments --uncommitted | head
  ```

  Or check throughput in the Estuary dashboard.
- **Tinybird:** query a Data Source or the published Pipe endpoint and confirm rows arrive within seconds of the generator's writes. ETAs in the `delays` node should shift as new `traffic_weather` rows change `impact_on_ETA_minutes`.
- **Dashboard:** the charts at http://localhost:3000 should populate and update as data flows.

## Next steps

- Add a `delete` simulation (uncomment `delete_old_shipments()` in `datagen.py`) to exercise Estuary CDC deletes end to end.
- Materialize the same collections into a warehouse (BigQuery, Snowflake, Databricks) directly from Estuary for historical analytics alongside the real-time Tinybird path.
- Add derivations in Estuary (SQL, TypeScript, or Python) to pre-aggregate or enrich shipments before they reach Tinybird.

## Resources

- Blog: [Real-Time Freight Tracking with Estuary and Tinybird](https://estuary.dev/real-time-freight-tracking-estuary-tinybird/)
- Estuary docs: https://docs.estuary.dev
- MongoDB capture connector: https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/
- Reading collections from Kafka (Dekaf): https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/
- flowctl: https://docs.estuary.dev/concepts/flowctl/
- Tinybird: https://www.tinybird.co/docs
