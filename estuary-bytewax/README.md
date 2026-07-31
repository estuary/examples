# Stream MongoDB CDC to a Bytewax Dataflow with Estuary and Dekaf

Stream real-time MongoDB change data capture (CDC) into a [Bytewax](https://bytewax.io/) Python dataflow using [Estuary](https://estuary.dev) and Dekaf, Estuary's Kafka-compatible API. This example captures `bookings` documents from a local MongoDB replica set, lands them in an Estuary collection, and consumes that collection from a Bytewax dataflow that computes tumbling-window booking metrics (total bookings, cancellations, passengers, revenue, and most popular destination) over a simulated space-tourism workload.

## Architecture

The pipeline wires MongoDB to Bytewax through Estuary without writing or running any Kafka brokers:

```
MongoDB (replica set rs0)  ──ngrok TCP tunnel──▶  Estuary capture (source-mongodb)
                                                          │
                                                          ▼
                                          Estuary collection (CDC events)
                                                          │
                                                   Dekaf (Kafka API)
                                                          │
                                                          ▼
                                          Bytewax dataflow (main.py) ──▶ windowed metrics → stdout
```

- A **capture** uses Estuary's `source-mongodb` connector to read the change stream from the `space_tourism.bookings` collection and write CDC events into an Estuary **collection** (a schematized, real-time data lake in cloud storage).
- **Dekaf** exposes that collection over a Kafka-compatible interface, so any Kafka client can subscribe to it as a topic.
- The Bytewax dataflow uses `KafkaSource` to read the collection through Dekaf, parses MongoDB CDC events, groups them into 5-minute tumbling windows keyed on `booking_id`, and emits aggregate metrics.

Because Estuary is fully managed, the locally running MongoDB is exposed to the connector through an ngrok TCP tunnel.

## What's included

- **`docker-compose.yml`** — spins up three services:
  - `mongodb` — `mongo:latest`, container/hostname `mongodb`, run as a single-node replica set `rs0` (required for MongoDB change streams / CDC), with keyfile authentication and root credentials `root` / `password` on port `27017`. A healthcheck calls `rs.initiate(...)` to bootstrap the replica set.
  - `datagen` — built from `datagen/`, container `bytewax-datagen`. Continuously writes inserts, updates, and deletes to MongoDB to simulate live booking traffic.
  - `ngrok` — `ngrok/ngrok:latest`, container `bytewax-ngrok`. Runs `tcp mongodb:27017` to publish a public TCP endpoint for the Estuary capture, with its inspection UI on port `4040`.
- **`datagen/datagen.py`** — connects to `mongodb://root:password@mongodb:27017`, targets database `space_tourism` and collection `bookings`, and emits one random `INSERT` / `UPDATE` / `DELETE` operation per second. Each booking document has `booking_id`, `customer_id`, `destination`, `booking_date`, `passengers`, and `total_price`.
- **`datagen/Dockerfile`** / **`datagen/requirements.txt`** — Python 3.12 image for the generator (`pymongo==4.8.0`, `python-dotenv==1.0.1`).
- **`mongodb/keyfile`** — keyfile mounted into the MongoDB container to enable internal replica-set authentication (`--keyFile`). Demo material only; do not reuse in production.
- **`main.py`** — the Bytewax dataflow consumer. Reads the Estuary collection through Dekaf with `KafkaSource`, parses CDC events, applies an `EventClock` + `TumblingWindower`, and computes per-window booking metrics.
- **`requirements.txt`** — consumer dependencies: `bytewax[confluent_kafka]==0.21`, `python-dotenv==1.0.1`.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose.
- A free [ngrok](https://ngrok.com/) account and authtoken (the local MongoDB must be tunneled so the managed connector can reach it).
- A free [Estuary account](https://dashboard.estuary.dev).
- Python 3.12 to run the Bytewax consumer locally.
- An Estuary access token (refresh token) for Dekaf authentication. Generate one in the dashboard under **Admin → CLI-API → Access Token**.

## Setup

### 1. Start MongoDB, the data generator, and the ngrok tunnel

Export your ngrok authtoken, then bring up the stack:

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
docker compose up -d
```

This starts the `mongodb` replica set, the `bytewax-datagen` generator (which immediately begins writing to `space_tourism.bookings`), and the `bytewax-ngrok` tunnel.

### 2. Get the public MongoDB endpoint

Read the public TCP address ngrok assigned to MongoDB:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'
```

You can also open the ngrok inspection dashboard at <http://localhost:4040>. The value looks like `tcp://0.tcp.ngrok.io:12345`. **Strip the `tcp://` prefix** — you'll paste the host and port separately into Estuary.

## Configure the Estuary capture

Create a MongoDB capture in the [Estuary dashboard](https://dashboard.estuary.dev/captures) using the **MongoDB** connector (`source-mongodb`). Use the values from this stack:

| Field | Value |
| --- | --- |
| Address / Host | the ngrok host (e.g. `0.tcp.ngrok.io:12345`, without `tcp://`) |
| User | `root` |
| Password | `password` |
| Database | `space_tourism` |

The connector reads the MongoDB change stream and writes CDC events for the `bookings` collection into an Estuary collection. Note the resulting collection's full name (e.g. `<your-tenant>/mongodb/space_tourism/bookings`) — you'll point Bytewax at it.

Connector reference: <https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/>

## Configure and run the Bytewax consumer

The dataflow in `main.py` reads the collection through Dekaf. Before running, set two things:

1. **`KAFKA_TOPIC`** in `main.py` — set it to your full Estuary collection name (it ships with a placeholder):

   ```python
   # main.py
   KAFKA_TOPIC = "<your-tenant>/mongodb/space_tourism/bookings"
   ```

2. **`DEKAF_TOKEN`** environment variable — your Estuary access token, used as the SASL password.

The dataflow connects to Dekaf with these settings (already wired in `main.py`):

```python
KAFKA_BOOTSTRAP_SERVERS = ["dekaf.estuary.dev:9092"]
add_config = {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "{}",
    "sasl.password": os.getenv("DEKAF_TOKEN"),
}
```

Install dependencies and run the dataflow:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export DEKAF_TOKEN=<your-estuary-access-token>
python -m bytewax.run main.py
```

> Note: `main.py` points at the `dekaf.estuary.dev:9092` bootstrap with SASL username `{}`. If your account uses the standard Dekaf endpoint, set the bootstrap to `dekaf.estuary-data.com:9092` and the username to your Dekaf task name. See the Dekaf guide below for the configuration that matches your tenant.

### What the dataflow does

- `parse_message` decodes each CDC event. `insert` / `update` events use `fullDocument`; `delete` events use `documentKey`. Other operation types are dropped.
- Events are keyed by `booking_id` and grouped with an `EventClock` (using `booking_date` as event time, with a 10-second system-time grace period) into 5-minute `TumblingWindower` windows aligned to `2024-09-01T00:00:00Z`.
- `calculate_metrics` emits, per window: `total_bookings`, `total_cancellations`, `total_passengers`, `total_revenue`, and `most_popular_destination`.
- `op.inspect(...)` prints incoming and windowed messages; `StdOutSink` prints the computed metrics to stdout.

## Verify

- Confirm CDC is flowing in the Estuary dashboard by checking the capture's read/write stats, or stream the collection directly:

  ```bash
  flowctl collections read --collection <your-tenant>/mongodb/space_tourism/bookings --uncommitted | head
  ```

- In the Bytewax terminal, you should see `op.inspect` output for each parsed message followed by per-window metric dictionaries once a window closes. The `datagen` container produces an operation every second, so events appear continuously.

## Cleanup

```bash
docker compose down -v
```

The `-v` flag also removes the `mongo-data` volume.

## Next steps

- Swap `StdOutSink` for a Kafka, file, or database sink to persist the windowed metrics.
- Add an Estuary [materialization](https://dashboard.estuary.dev/materializations) to land the raw `bookings` collection in a warehouse (BigQuery, Snowflake, etc.) alongside the Bytewax stream processing.
- Point the same Dekaf topic at other Kafka-native tools (Flink, ksqlDB, kcat) to fan out the stream.

## References

- Full how-to guide: <https://bytewax.io/blog/estuary-flow-mongodb-bytewax-real-time-data>
- Estuary documentation: <https://docs.estuary.dev>
- MongoDB capture connector: <https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/>
- Reading collections from Kafka with Dekaf: <https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/>
- Bytewax documentation: <https://docs.bytewax.io/>
