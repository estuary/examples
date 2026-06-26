# Consume an Estuary Collection in Python with Kafka and Avro via Estuary Dekaf

Read a real-time Estuary collection from Python using the `confluent-kafka` client, Estuary's **Dekaf** Kafka-compatible API, and an Avro schema registry. This example consumes the `recentchange-sampled` collection (Wikimedia RecentChange events) over SASL_SSL, deserializes each record against the registry-managed Avro schema, and prints it. No Kafka cluster, no Debezium, no schema-registry to operate — Dekaf exposes any Estuary collection as a Kafka topic.

## How it works

Estuary captures source data into **collections** (a schematized, real-time data lake of JSON in cloud storage). **Dekaf** is Estuary's Kafka-compatible streaming API: it lets any Kafka consumer read a collection as if it were a Kafka topic, complete with a Confluent-style schema registry that serves Avro schemas for each collection.

```
Estuary collection (recentchange-sampled)
        │
        ▼
Dekaf  ──  Kafka-compatible broker  +  Avro schema registry
  broker:          dekaf.estuary-data.com:9092  (SASL_SSL / PLAIN)
  schema registry: https://dekaf.estuary-data.com
        │
        ▼
main.py  ──  confluent-kafka Consumer + AvroDeserializer  ──►  prints records
```

`main.py`:
1. Connects a `confluent_kafka.Consumer` to `dekaf.estuary-data.com:9092` using `security.protocol=SASL_SSL`, `sasl.mechanism=PLAIN`, username = the Dekaf task name, password = an Estuary access token.
2. Connects a `SchemaRegistryClient` to `https://dekaf.estuary-data.com` with the same credentials (`basic.auth.user.info`).
3. Fetches the latest Avro schema for the `recentchange-sampled-value` subject and builds an `AvroDeserializer`.
4. Subscribes to the `recentchange-sampled` topic and polls in a loop, deserializing each message value and printing `id`, `meta.domain`, `timestamp`, and `title`.

## What's included

- **`main.py`** — the consumer. Holds the Dekaf endpoint, schema registry URL, target topic (`recentchange-sampled`), Kafka/SASL config, Avro deserialization, and the poll loop.
- **`requirements.txt`** — single dependency: `confluent-kafka[avro,schemaregistry,rules]` (the Kafka client plus Avro + schema-registry extras). Note `main.py` also uses `python-dotenv` (`load_dotenv`) to read credentials from a `.env` file — install it as well (see below).

## Prerequisites

- **Python 3.8+** and `pip`.
- A **free Estuary account** — sign up at [https://dashboard.estuary.dev](https://dashboard.estuary.dev).
- The `recentchange-sampled` **collection available in your Estuary account**. This is the Wikimedia RecentChange sample stream. If you don't already have it, create a capture for the Wikimedia / public demo source (or any collection of your own) and update the `topic` variable in `main.py` to match the collection name you want to read.
- A **Dekaf access token** (an Estuary refresh/access token) to authenticate the consumer and schema registry.

> Dekaf authenticates with `sasl.mechanism=PLAIN` where the username is the Dekaf **task name** and the password is an Estuary **access token**. Public demo topics can be read with username `{}` and an empty password, but this example is wired for an authenticated collection via `DEKAF_TASK_NAME` and `DEKAF_ACCESS_TOKEN`.

## Setup

Install the dependencies:

```bash
pip install -r requirements.txt
pip install python-dotenv
```

Create a `.env` file in this directory with your Dekaf credentials:

```bash
# .env
DEKAF_TASK_NAME=your-dekaf-task-name
DEKAF_ACCESS_TOKEN=your-estuary-access-token
```

These map directly to the consumer's `sasl.username` / `sasl.password` and the schema registry's `basic.auth.user.info` in `main.py`.

### Getting your Dekaf credentials

1. In the [Estuary dashboard](https://dashboard.estuary.dev), open the collection you want to consume (here, `recentchange-sampled`).
2. Create or open a **Dekaf** materialization/task for it — its name is your `DEKAF_TASK_NAME`.
3. Generate an **access token** (or refresh token) from your account settings — this is your `DEKAF_ACCESS_TOKEN`.

See the Dekaf guide for the exact steps: [Reading Estuary collections from Kafka (Dekaf)](https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/).

## Running it

```bash
python main.py
```

You should see one line per record as events stream in, for example:

```
('<id>', '<meta.domain>', '<timestamp>', '<title>')
```

Stop with `Ctrl+C` — the script catches `KeyboardInterrupt` and closes the consumer cleanly.

## Configuration reference

All connection settings live at the top of `main.py`:

| Setting | Value | Notes |
| --- | --- | --- |
| `bootstrap.servers` | `dekaf.estuary-data.com` | Dekaf broker (port `9092`) |
| `security.protocol` | `SASL_SSL` | Required by Dekaf |
| `sasl.mechanism` | `PLAIN` | Required by Dekaf |
| `sasl.username` | `DEKAF_TASK_NAME` | Dekaf task name |
| `sasl.password` | `DEKAF_ACCESS_TOKEN` | Estuary access token |
| Schema registry URL | `https://dekaf.estuary-data.com` | Confluent-compatible Avro registry |
| `group.id` | `my-group` | Consumer group |
| `auto.offset.reset` | `latest` | Set to `earliest` to read from the start of the collection |
| `topic` | `recentchange-sampled` | The Estuary collection name |

To consume a different collection, change `topic` to that collection's name; the deserializer automatically resolves the `<topic>-value` subject from the schema registry.

## Verify

- Watch `main.py`'s output — a steady stream of printed records confirms Dekaf is serving the collection and the Avro schema resolved correctly.
- Cross-check against the collection in the [Estuary dashboard](https://dashboard.estuary.dev) (open the collection and view recent documents).
- With [flowctl](https://docs.estuary.dev/concepts/flowctl/) you can read the same collection directly:

  ```bash
  flowctl collections read --collection recentchange-sampled --uncommitted | head
  ```

## Next steps

- Point any other Kafka client (kcat, Kafka Connect, Flink, Spark, Tinybird, ClickPipes, etc.) at the same Dekaf endpoint — the credentials and schema registry are identical across clients.
- Swap the `print` for your own processing: write to a database, push to another stream, or run real-time analytics.
- Build a full pipeline: capture a source into a collection, optionally transform it with a [derivation](https://docs.estuary.dev/concepts/derivations/), and consume it here.

## References

- Dekaf guide: [Reading Estuary collections from Kafka](https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/)
- Estuary docs: [https://docs.estuary.dev](https://docs.estuary.dev)
- Estuary dashboard: [https://dashboard.estuary.dev](https://dashboard.estuary.dev)
- `confluent-kafka` Python client: [https://github.com/confluentinc/confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python)
