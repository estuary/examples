# Consume an Estuary Collection from the CLI with kcat (Kafka) via Dekaf

Read a live Estuary collection straight from your terminal using [kcat](https://github.com/edenhill/kcat) (the Kafka CLI, formerly `kafkacat`) over Estuary's Kafka-compatible **Dekaf** API. The included `consume.sh` connects to a Dekaf bootstrap endpoint over `SASL_SSL` / `PLAIN` and tails the public demo `wikipedia/recentchange` collection — a real-time stream of Wikipedia edit events — with no Estuary-specific tooling required.

Because Dekaf speaks the Kafka wire protocol, any existing Kafka consumer (kcat, the Java client, `confluent-kafka`, Spark, Flink, ksqlDB, etc.) can read an Estuary collection as if it were a Kafka topic.

## How it works

Estuary [captures](https://docs.estuary.dev/concepts/captures/) data from sources into [collections](https://docs.estuary.dev/concepts/collections/) — schematized JSON streams backed by cloud storage. **Dekaf** exposes those collections through a Kafka-compatible endpoint, so a Kafka consumer can subscribe to a collection as a topic.

```
Source ──capture──▶ Estuary collection ──Dekaf (Kafka API)──▶ kcat (this example)
                    (demo/wikipedia/recentchange-sampled)
```

- **Bootstrap server**: the Dekaf endpoint, addressed like a Kafka broker.
- **Topic**: the Estuary collection name.
- **Auth**: SASL `PLAIN` over TLS. Username is the Dekaf task name (or `{}` for public demo topics); password is an Estuary access token (empty for public demo topics).

## What's included

- `consume.sh` — a single `kcat` consumer invocation that connects to Dekaf and prints messages from the public Wikipedia recent-changes demo collection.

## Prerequisites

- **kcat** installed and on your `PATH`:
  - macOS: `brew install kcat`
  - Debian/Ubuntu: `apt-get install kcat`
  - Or see the [kcat install docs](https://github.com/edenhill/kcat#install).
- Nothing else for the public demo topic. To read **your own** collections you need a free [Estuary account](https://dashboard.estuary.dev) and an Estuary access token.

## Running it

The script as committed:

```bash
kcat -C \
  -b dekaf.estuary-data.com:9092 \
  -t demo/wikipedia/recentchange-sampled \
  -X security.protocol=sasl_ssl \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username='{}'  \
  -X sasl.password=''
```

Run it:

```bash
chmod +x consume.sh
./consume.sh
```

`kcat -C` runs in **consumer** mode and streams the Wikipedia `recentchange` events to stdout. Press `Ctrl-C` to stop.

Flags explained:

| Flag | Value | Meaning |
| --- | --- | --- |
| `-C` | — | Consumer mode |
| `-b` | `dekaf.estuary-data.com:9092` | Dekaf bootstrap server (Kafka broker) |
| `-t` | `demo/wikipedia/recentchange-sampled` | Collection name, used as the Kafka topic |
| `-X security.protocol` | `sasl_ssl` | Encrypted connection with SASL auth |
| `-X sasl.mechanisms` | `PLAIN` | SASL PLAIN mechanism |
| `-X sasl.username` | `{}` | Public demo placeholder (use your Dekaf task name for private collections) |
| `-X sasl.password` | (empty) | Public demo placeholder (use your Estuary access token for private collections) |

> **Note on the bootstrap host:** the script uses Estuary's production Dekaf endpoint, `dekaf.estuary-data.com:9092` (with the schema registry at `https://dekaf.estuary-data.com`). A legacy host, `dekaf.fly.dev:9092`, appeared in older versions of this example but no longer resolves — use `dekaf.estuary-data.com:9092`. See the [Dekaf reading guide](https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/) for the authoritative endpoint and connection settings.

## Reading your own collections

To consume a private collection instead of the public demo:

1. Sign in to the [Estuary dashboard](https://dashboard.estuary.dev) and create a [Dekaf materialization](https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/) (or use an existing one) to expose your collection over the Kafka API.
2. Generate an Estuary access token (refresh token) from the dashboard.
3. Run kcat with the production endpoint, your Dekaf task name as the username, and the token as the password:

```bash
export DEKAF_TASK_NAME="your-org/your-dekaf-task"
export DEKAF_ACCESS_TOKEN="your-estuary-access-token"

kcat -C \
  -b dekaf.estuary-data.com:9092 \
  -t your-collection-name \
  -X security.protocol=sasl_ssl \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username="$DEKAF_TASK_NAME" \
  -X sasl.password="$DEKAF_ACCESS_TOKEN"
```

The Kafka topic name is the collection name as exposed by your Dekaf task.

## Verify

If the connection is working, you'll see a continuous stream of JSON Wikipedia edit events printed to your terminal. To consume from the beginning of the available data instead of the live tail, add `-o beginning`:

```bash
kcat -C -o beginning \
  -b dekaf.estuary-data.com:9092 \
  -t demo/wikipedia/recentchange-sampled \
  -X security.protocol=sasl_ssl \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username='{}' \
  -X sasl.password=''
```

To print only a fixed number of messages and exit, add `-c <N>` (for example `-c 10`).

## Next steps

- Consume the same collection from a Python application with the Avro schema registry: see the [`dekaf-python`](../dekaf-python) example in this repository. Note that the Python example subscribes to the topic as `recentchange-sampled` while this kcat example uses the fully-qualified `demo/wikipedia/recentchange-sampled`; both refer to the same demo collection, so use whichever topic string the example you are running already specifies.
- Wire any other Kafka client (Spark, Flink, ksqlDB, the Java client) to an Estuary collection through Dekaf.
- Build your own pipeline: create a [capture](https://dashboard.estuary.dev/captures), land it in a collection, and expose it over Dekaf.

## References

- Dekaf — reading collections from Kafka: https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/
- Estuary documentation: https://docs.estuary.dev
- Estuary dashboard: https://dashboard.estuary.dev
- kcat (Kafka CLI): https://github.com/edenhill/kcat
