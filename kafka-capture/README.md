# Stream Kafka (AWS MSK) IoT Topics to Any Destination with Estuary

Generate and consume real-time IoT data on an [Amazon MSK](https://aws.amazon.com/msk/) (Managed Streaming for Apache Kafka) cluster using **AWS IAM authentication** (`SASL_SSL` + `OAUTHBEARER`), then capture those topics into [Estuary](https://estuary.dev) with the `source-kafka` connector. The Python producer (`datagen/`) emits sensor readings and device metadata to the `iot.readings` and `iot.devices` topics, and the consumer (`consumer/`) reads them back so you can verify connectivity before wiring up the Estuary capture.

Use this example as a working reference for **MSK IAM authentication from Python (`kafka-python` + `aws-msk-iam-sasl-signer`)** and for streaming Kafka topics into a real-time data pipeline.

## Architecture

```
datagen/datagen.py ──► AWS MSK topics ──► Estuary capture ──► Estuary collections ──► materialization ──► destination
  (IoT producer)        iot.readings       (source-kafka)      (real-time JSON)      (any connector)    (warehouse / lake)
                        iot.devices
                              ▲
                              │
                    consumer/consumer.py (verify)
```

- The **producer** authenticates to MSK with AWS IAM (no Kafka passwords), creates the topics if missing, and streams JSON events.
- An **Estuary capture** using the `source-kafka` connector reads the same topics and lands each topic in an [Estuary collection](https://docs.estuary.dev/concepts/collections/) — a real-time, schematized data lake of JSON in cloud storage.
- From there, a [materialization](https://docs.estuary.dev/concepts/materialization/) pushes the collections to any supported destination (Snowflake, BigQuery, Redshift, Iceberg, Postgres, etc.) in real time, with optional [derivations](https://docs.estuary.dev/concepts/derivations/) for SQL/TypeScript/Python transforms in between.

## What's included

| Path | Role |
| --- | --- |
| `datagen/datagen.py` | IoT producer. Creates `iot.readings` (3 partitions, replication factor 2) and `iot.devices` (1 partition, replication factor 2), seeds device metadata, then streams ~10 readings/sec with occasional SCD-2-style metadata changes. |
| `datagen/requirements.txt` | Producer deps: `kafka-python`, `aws-msk-iam-sasl-signer-python`, `faker`, `python-dotenv`. |
| `consumer/consumer.py` | IoT consumer. Reads a topic (`iot.readings` by default), pretty-prints each message, and exits after 10s of inactivity. |
| `consumer/run_consumer.sh` | Convenience wrapper around `consumer.py` with `-t/--topic` and `--from-beginning` flags. |
| `consumer/requirements.txt` | Consumer deps: `kafka-python`, `aws-msk-iam-sasl-signer-python`. |
| `check_setup.py` | Pre-flight checker: validates AWS credentials, region, `MSK_BROKERS`, MSK IAM token generation, and basic `kafka:ListClusters` permission. |
| `test_kafka_connection.py` | Minimal MSK connectivity smoke test (admin client + producer metadata). |

## Data model

**`iot.readings`** (keyed by `device_id`):

| Field | Type | Notes |
| --- | --- | --- |
| `device_id` | string | e.g. `thermo-00001` |
| `ts` | string | ISO-8601 UTC timestamp (millisecond precision) |
| `temperature_c` | number | °C |
| `humidity_pct` | number | % |
| `battery_pct` | number | % |
| `status` | string | `ok` / `warn` / `error` (derived from thresholds) |

**`iot.devices`** (keyed by `device_id`, SCD-2-style — a new record per change):

| Field | Type | Notes |
| --- | --- | --- |
| `device_id` | string | |
| `effective_from` | string | ISO-8601 UTC timestamp of this version |
| `model` | string | `T900`…`T9000` |
| `firmware_version` | string | e.g. `1.3.0` |
| `site` | string | `nyc_manhattan_hq`, `sp_sao_paulo_lab`, `ldn_office` |
| `room` | string | `conf_a`, `conf_b`, `open_floor`, `server_room` |
| `lat` / `lon` | number | coordinates |

## Prerequisites

- **Python 3.7+**
- An **AWS MSK cluster** with **IAM access control** enabled, reachable from where you run the scripts (security groups / VPC / public access).
- **AWS credentials** with the MSK IAM permissions below (the signer uses the standard AWS default credential chain).
- A free **Estuary account** to create the capture: <https://dashboard.estuary.dev>

## AWS credentials configuration

The `aws-msk-iam-sasl-signer` library uses the **AWS default credential chain**. Configure credentials with any standard method.

### Option 1 — Environment variables (recommended for local dev)

```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_REGION="us-east-1"            # your MSK cluster region
export MSK_BROKERS="your-msk-bootstrap-servers"
```

### Option 2 — AWS credentials file

`~/.aws/credentials`:

```ini
[default]
aws_access_key_id = your-access-key-id
aws_secret_access_key = your-secret-access-key
```

`~/.aws/config`:

```ini
[default]
region = us-east-1
```

### Option 3 — Named AWS profile

```bash
export AWS_PROFILE="your-profile-name"
```

To use a named profile inside the token provider, modify the `MSKTokenProvider` class:

```python
class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token_from_profile(AWS_REGION, 'your-profile-name')
        return token
```

### Option 4 — IAM role (EC2/ECS/Lambda)

Running on AWS infrastructure? The scripts automatically use the attached IAM role — no extra config.

### Option 5 — Assume role

```python
class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token_from_role_arn(AWS_REGION, 'arn:aws:iam::account:role/role-name')
        return token
```

## Required IAM permissions

The identity used by the producer/consumer needs these MSK permissions. The same policy (Connect + topic Read/Write + group access) is what an Estuary capture's IAM identity needs to read the topics.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:Connect",
                "kafka-cluster:AlterCluster",
                "kafka-cluster:DescribeCluster"
            ],
            "Resource": "arn:aws:kafka:region:account-id:cluster/cluster-name/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:*Topic*",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData"
            ],
            "Resource": "arn:aws:kafka:region:account-id:topic/cluster-name/*/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup"
            ],
            "Resource": "arn:aws:kafka:region:account-id:group/cluster-name/*/*"
        }
    ]
}
```

## Environment variables

```bash
# Required
export AWS_REGION="us-east-1"            # your MSK cluster region (default in scripts: us-east-1)
export MSK_BROKERS="b-1.cluster.kafka.region.amazonaws.com:9098,b-2.cluster.kafka.region.amazonaws.com:9098"

# Optional — AWS credentials (if not using a profile/role/file)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

> MSK IAM endpoints use port **9098** (private) or **9198** (public). Use the IAM bootstrap brokers, not the TLS/plaintext ones.

> Note: the consumer and producer read `MSK_BROKERS` / accept `--bootstrap`; `datagen.py` and `test_kafka_connection.py` ship with a placeholder `BOOTSTRAP = "borker1,broker2,broker3"` — either pass `--bootstrap` (where supported) or edit the constant to your IAM bootstrap servers. The producer also imports `boto3`, so install it (`pip install boto3`) if it is not already present.

## Setup

```bash
# Producer
cd datagen
pip install -r requirements.txt

# Consumer (in a separate shell)
cd consumer
pip install -r requirements.txt
```

Verify your environment before producing anything:

```bash
# check_setup.py needs boto3/botocore, which aren't in the requirements files.
# (botocore ships with boto3.) The producer also imports boto3.
pip install boto3
python check_setup.py
```

It runs five checks (AWS credentials, region, `MSK_BROKERS`, MSK token generation, and MSK list-clusters permission) and prints a pass/fail summary.

> Note: `check_setup.py` imports `boto3` and `botocore` (and the producer imports `boto3` too), but neither is listed in `datagen/requirements.txt` or `consumer/requirements.txt`. Run `pip install boto3` first or you'll hit `ModuleNotFoundError`.

## Running it

### 1. Produce IoT data

```bash
cd datagen
python datagen.py
# or with explicit brokers:
python datagen.py --bootstrap "b-1.cluster.kafka.us-east-1.amazonaws.com:9098"
```

The producer creates the topics if needed, seeds 20 devices into `iot.devices`, then streams readings to `iot.readings` at ~10 events/sec until you press Ctrl+C.

### 2. Consume to verify

```bash
cd consumer
./run_consumer.sh                                    # latest from iot.readings
./run_consumer.sh -t iot.devices                     # latest from iot.devices
./run_consumer.sh -t iot.readings --from-beginning   # all messages from the beginning
```

Or call Python directly:

```bash
python consumer.py -t iot.readings --from-beginning
```

Consumer flags: `-b/--bootstrap`, `-t/--topic` (default `iot.readings`), `-g/--group` (default `iot-consumer-group`), `--from-beginning`.

### Connection details (matches the scripts)

- **Security protocol:** `SASL_SSL`
- **SASL mechanism:** `OAUTHBEARER`
- **Authentication:** AWS MSK IAM (token from `aws-msk-iam-sasl-signer`)

## Configure the Estuary capture

Once the topics exist and have data, capture them into Estuary with the **`source-kafka`** connector (image `ghcr.io/estuary/source-kafka:v1`).

### Via the dashboard

1. Go to <https://dashboard.estuary.dev/captures> and click **New Capture**.
2. Choose the **Apache Kafka** / **Amazon MSK** (`source-kafka`) connector.
3. Enter the connection settings, using the same values as the scripts:
   - **Bootstrap servers:** your IAM bootstrap brokers (e.g. `b-1.cluster.kafka.us-east-1.amazonaws.com:9098`)
   - **TLS:** enabled (`SASL_SSL`)
   - **Authentication:** AWS MSK IAM — provide the AWS region (`us-east-1`) and the access key / secret of an identity holding the [IAM permissions above](#required-iam-permissions).
4. Discover topics and select `iot.readings` and `iot.devices` to bind. Each becomes an Estuary collection.
5. Save and publish. Estuary backfills existing messages and then streams new ones in real time.

See the connector reference for the full option list (including AWS IAM auth and schema-registry settings): <https://docs.estuary.dev/reference/Connectors/capture-connectors/apache-kafka/>

### Via flowctl

Prefer the CLI? Authenticate, stub a minimal `flow.yaml` with the `source-kafka` config, discover the topics, then publish:

```yaml
# flow.yaml — minimal source-kafka stub for discovery
captures:
  <tenant>/<prefix>/source-kafka:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-kafka:v1
        config:
          bootstrap_servers: "b-1.cluster.kafka.us-east-1.amazonaws.com:9098,b-2.cluster.kafka.us-east-1.amazonaws.com:9098"
          tls: system_certificates
          credentials:
            auth_type: AWS
            aws_access_key_id: "your-access-key-id"
            aws_secret_access_key: "your-secret-access-key"
            region: us-east-1
    bindings: []
```

```bash
flowctl auth login
flowctl discover --source flow.yaml      # fills in bindings for iot.readings / iot.devices
flowctl catalog publish --source flow.yaml --auto-approve
```

`flowctl discover` rewrites `flow.yaml` in place, adding a binding (and discovered collection schema) for each topic. See the [connector reference](https://docs.estuary.dev/reference/Connectors/capture-connectors/apache-kafka/) for the full config and AWS IAM auth option list.

flowctl docs: <https://docs.estuary.dev/concepts/flowctl/>

## Verify the pipeline

Confirm messages are landing in your Estuary collections:

```bash
flowctl collections read --collection <tenant>/<prefix>/iot.readings --uncommitted | head
```

Or watch live throughput and document counts on the capture's page in the [Estuary dashboard](https://dashboard.estuary.dev/captures).

## Next steps

- Add a **materialization** to push the collections to a warehouse or lake: <https://dashboard.estuary.dev/materializations>
- Add a **derivation** to transform `iot.devices` into an SCD-2 dimension or to enrich `iot.readings`: <https://docs.estuary.dev/concepts/derivations/>
- Read collections from any Kafka client (no extra infra) via **Dekaf**, Estuary's Kafka-compatible API: <https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/>

## Troubleshooting

- **Access denied:** verify the [IAM permissions](#required-iam-permissions) and that credentials are loaded (`aws sts get-caller-identity`).
- **Region mismatch:** `AWS_REGION` must match the cluster's region.
- **Network issues:** confirm the MSK security groups allow access from your IP and that you are using the **IAM** bootstrap endpoints (port 9098/9198).
- **Wrong brokers:** `datagen.py` / `test_kafka_connection.py` default to a placeholder; set `MSK_BROKERS` or pass `--bootstrap`.

Debug which identity the signer uses:

```python
token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION, aws_debug_creds=True)
```

## Resources

- Estuary docs: <https://docs.estuary.dev>
- Apache Kafka / Amazon MSK capture connector: <https://docs.estuary.dev/reference/Connectors/capture-connectors/apache-kafka/>
- Dekaf (read collections as Kafka): <https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka/>
- AWS MSK IAM access control: <https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html>
