# Real-Time PostgreSQL CDC to MotherDuck with Estuary

A self-contained demo that streams change data capture (CDC) from a local PostgreSQL database into [MotherDuck](https://motherduck.com) in real time using [Estuary](https://estuary.dev). A data generator continuously writes pet-store transactions and AI-generated product reviews into Postgres; Estuary's PostgreSQL capture connector reads the write-ahead log (WAL) via logical replication, lands the changes in Estuary collections, and a MotherDuck materialization keeps the analytical tables up to date with low latency.

## Architecture

```
┌──────────────┐     INSERTs      ┌──────────────┐   logical    ┌─────────────────┐   materialize   ┌──────────────┐
│   datagen    │ ───────────────► │  PostgreSQL  │ replication  │  Estuary   │ ──────────────► │  MotherDuck  │
│ (Python +    │  transactions/   │ (wal_level=  │ ───────────► │   collections   │                 │   tables     │
│  OpenAI)     │     reviews      │   logical)   │   (CDC)      │ products /      │                 │ products /   │
└──────────────┘                  └──────┬───────┘              │ transactions /  │                 │ transactions/│
                                         │                      │ reviews         │                 │ reviews      │
                                  ngrok TCP tunnel              └─────────────────┘                 └──────────────┘
                                  (public host:port)
```

End-to-end data flow in Estuary terms:

1. **Capture (source):** the [`source-postgres`](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/) connector reads `products`, `transactions`, and `reviews` from the `flow_publication` publication over logical replication.
2. **Collections:** each captured table becomes an Estuary collection — a schematized, real-time data lake of JSON documents in cloud storage.
3. **Materialization (destination):** the [`materialize-motherduck`](https://docs.estuary.dev/reference/Connectors/materialization-connectors/motherduck/) connector continuously pushes those collections into MotherDuck tables.

Because Estuary is fully managed, the locally running Postgres is exposed to the hosted connector through an **ngrok TCP tunnel**.

## What's included

- **`docker-compose.yml`** — spins up three services:
  - `postgres` (container `postgres-cdc-motherduck-postgres`): `postgres:latest` started with `wal_level=logical`, port `5432` published, init script mounted.
  - `datagen` (container `postgres-cdc-motherduck-datagen`): builds the `datagen/` image and continuously writes rows into Postgres.
  - `ngrok` (container `postgres-cdc-motherduck-ngrok`): runs `tcp postgres:5432` to expose Postgres publicly; the ngrok inspection UI is published on port `4040`.
- **`postgres/init.sql`** — runs on first boot. It grants the `postgres` user `REPLICATION` and `pg_read_all_data`, creates the `public.flow_watermarks` table, creates the `flow_publication` publication (with `publish_via_partition_root = true`), creates the `products`, `transactions`, and `reviews` tables, adds them all to the publication, and seeds `products` with ~50 pet-store items (dog and cat supplies).
- **`datagen/datagen.py`** — a Python loop that every 3 seconds inserts either a `transactions` row (60%) or a `reviews` row (40%). Review text is generated with the OpenAI API (`gpt-3.5-turbo-0125`); transaction amounts intentionally include a small fraction of high/low anomalies for demo analytics. (The script also contains an optional Google Cloud SQL connection path gated by `USE_CLOUD_SQL`; it is off by default.)
- **`datagen/Dockerfile`** / **`datagen/requirements.txt`** — build the data-generator image (`python:3.12`, `psycopg2-binary`, `openai`, `Faker`, `python-dotenv`, etc.).

## Prerequisites

- **Docker** and Docker Compose.
- A **verified [ngrok](https://ngrok.com) account** and authtoken — needed to expose the local Postgres to Estuary's hosted connector.
- An **[OpenAI API key](https://platform.openai.com/api-keys)** — the data generator calls OpenAI to write realistic product reviews. (Without it, review inserts fall back to a canned string, but transactions still flow.)
- A free **[Estuary account](https://dashboard.estuary.dev)**.
- A **[MotherDuck account](https://app.motherduck.com)** and a service token (from the MotherDuck console). The MotherDuck materialization stages data through an object-storage bucket, so have a staging bucket and its credentials ready as well — see the [MotherDuck materialization docs](https://docs.estuary.dev/reference/Connectors/materialization-connectors/motherduck/) for the exact requirements.

## Setup

Export the required tokens and start the stack:

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
export OPENAI_API_KEY=<your-openai-api-key>

docker compose up -d
```

On first boot, `postgres/init.sql` provisions replication access, the publication, the three tables, and the seed products. The `datagen` service then begins inserting transactions and reviews every 3 seconds.

### Get the public Postgres endpoint

ngrok exposes Postgres over a public TCP address. Read it from the ngrok inspection UI at [http://localhost:4040](http://localhost:4040), or grab it from the API:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
# e.g. tcp://6.tcp.ngrok.io:12345
```

Strip the `tcp://` prefix — you'll paste `host:port` (e.g. `6.tcp.ngrok.io:12345`) into Estuary.

### Verify data is being generated (optional)

```bash
docker exec -it postgres-cdc-motherduck-postgres \
  psql -U postgres -d postgres -c "SELECT count(*) FROM transactions;"
```

Run it again after a few seconds — the count should increase.

## Configure the Estuary capture

Set up the PostgreSQL source in the [Estuary dashboard](https://dashboard.estuary.dev/captures):

1. Go to **Sources → + New Capture** and choose the **PostgreSQL** connector.
2. Enter the connection details from `docker-compose.yml` and the ngrok endpoint:

   | Field    | Value                                         |
   | -------- | --------------------------------------------- |
   | Server Address | `<ngrok-host>:<ngrok-port>` (from step above, no `tcp://`) |
   | User     | `postgres`                                    |
   | Password | `postgres`                                    |
   | Database | `postgres`                                    |

3. Estuary discovers the `products`, `transactions`, and `reviews` tables (it uses the `flow_publication` publication and the `public.flow_watermarks` table created by `init.sql`). Leave the default bindings and **Save and Publish**.

> The seed `init.sql` grants the `postgres` user `REPLICATION` and `pg_read_all_data` and pre-creates the publication/watermarks table so discovery works out of the box. For production, create a dedicated capture user with least-privilege grants instead — see the [PostgreSQL connector docs](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/).

Connector image: `ghcr.io/estuary/source-postgres:dev`.

## Configure the Estuary materialization

Send the captured collections to MotherDuck from the [Estuary dashboard](https://dashboard.estuary.dev/materializations):

1. Go to **Destinations → + New Materialization** and choose the **MotherDuck** connector.
2. Provide your MotherDuck **service token**, target **database**, and the **staging bucket** credentials (the connector stages files in object storage before loading into MotherDuck).
3. Under **Source Collections**, add the `products`, `transactions`, and `reviews` collections created by the capture.
4. **Save and Publish.** Estuary backfills existing rows, then continuously applies new CDC events to the MotherDuck tables.

Connector image: `ghcr.io/estuary/materialize-motherduck:dev`. See the [MotherDuck materialization docs](https://docs.estuary.dev/reference/Connectors/materialization-connectors/motherduck/) for token, bucket, and sync-schedule options.

## Verify

- In the Estuary dashboard, the capture, collections, and materialization should all show matching document counts and live throughput.
- Peek at documents flowing through a collection with [flowctl](https://docs.estuary.dev/concepts/flowctl/):

  ```bash
  flowctl auth login
  flowctl collections read --collection <your-prefix>/transactions --uncommitted | head
  ```

- Query MotherDuck directly to confirm the data landed:

  ```sql
  SELECT payment_method, count(*), round(avg(amount), 2) AS avg_amount
  FROM transactions
  GROUP BY payment_method
  ORDER BY 2 DESC;
  ```

## Next steps

- Stop the stack with `docker compose down` (add `-v` to remove the Postgres volume and start clean).
- Explore Estuary [derivations](https://docs.estuary.dev/concepts/derivations/) to transform the `transactions` stream in SQL, TypeScript, or Python — for example, flagging the high/low anomaly amounts the generator produces.
- For a guided, multi-materialization version of this pipeline (soft delete, hard delete, and SCD Type 2), see the sibling [`hands-on-lab-postgres-motherduck`](../hands-on-lab-postgres-motherduck) example.

## Resources

- Estuary docs: https://docs.estuary.dev
- PostgreSQL capture connector: https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
- MotherDuck materialization connector: https://docs.estuary.dev/reference/Connectors/materialization-connectors/motherduck/
- flowctl: https://docs.estuary.dev/concepts/flowctl/
- Estuary dashboard: https://dashboard.estuary.dev
