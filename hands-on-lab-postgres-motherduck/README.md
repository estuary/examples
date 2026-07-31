# Real-Time PostgreSQL CDC to MotherDuck Hands-On Lab with Estuary

A guided, hands-on workshop that builds a streaming Change Data Capture (CDC) pipeline from PostgreSQL to MotherDuck using [Estuary](https://dashboard.estuary.dev). You capture change events from a Postgres `products` table into an Estuary collection, then materialize that collection into MotherDuck three different ways — **soft delete** (default), **hard delete**, and **Slowly Changing Dimension Type 2 (SCD2)** — to cover the most common analytics warehousing patterns.

Everything runs from your laptop: a Docker Compose stack spins up Postgres (with logical replication enabled), a fake data generator, and an ngrok TCP tunnel so the fully managed Estuary connector can reach your local database.

## Architecture

```
PostgreSQL (products)          Estuary                         MotherDuck
+-------------------+   CDC   +------------------------------+    +-------------------+
|  postgres_cdc     | ------> |  source-postgres capture     |    |  lab1 (soft del.) |
|  wal_level=logical|         |          |                   | -> |  lab2 (hard del.) |
|  + datagen        |         |          v                   |    |  lab3 (SCD2/delta)|
+-------------------+         |  collection: <tenant>/       |    +-------------------+
        ^                     |    workshop/public/products  |
        |  ngrok tcp 5432     |          |                   |
        +---------------------+          v                   |
                              |  3x materialize-motherduck   |
                              +------------------------------+
```

- **Capture (source):** Estuary's real-time PostgreSQL connector reads the WAL and streams inserts/updates/deletes from the `products` table.
- **Collection:** Change events land in an Estuary collection (a real-time data lake of schematized JSON in cloud storage). One collection feeds all three materializations.
- **Materializations (destinations):** Three MotherDuck materializations read from the same collection, each demonstrating a different delete/history strategy.

## What's included

- `docker-compose.yaml` — spins up three services:
  - `postgres` (container `postgres_cdc`) running `postgres:latest` with `wal_level=logical`, exposed on port `5432`.
  - `datagen` running `materialize/datagen`, generating 10,000 fake product records into Postgres (`-n 10000 -w 1000`).
  - `ngrok` running `ngrok/ngrok:latest`, exposing `postgres:5432` over a public TCP tunnel; the ngrok web UI is on port `4040`.
- `init.sql` — runs on first boot via the Postgres entrypoint and creates the `products` table.
- `schemas/products.sql` — the table schema consumed by the `datagen` tool to generate realistic data. (The actual table is created by `init.sql`; `datagen` only reads this file for column/faker definitions.)

### `products` data dictionary

| Column        | Type        | Notes                                  |
|---------------|-------------|----------------------------------------|
| `id`          | `int`       | Primary key                            |
| `name`        | `varchar`   | `faker.internet.userName()`            |
| `merchant_id` | `int`       | `NOT NULL`, `faker.datatype.number()`  |
| `price`       | `int`       | `faker.datatype.number()`              |
| `status`      | `varchar`   | `faker.datatype.boolean()`             |
| `created_at`  | `timestamp` | `DEFAULT now()`                        |

## Prerequisites

- **Docker** (with `docker compose`) — to run the Postgres + datagen + ngrok stack locally.
- **Estuary account** (free tier) — sign up at [dashboard.estuary.dev](https://dashboard.estuary.dev). The capture and materializations are configured entirely from the Estuary UI.
- **MotherDuck account** (free tier) — the target data warehouse. You'll need an **access token** from the MotherDuck console.
- **AWS S3 bucket** — used by MotherDuck for staging temporary files. An `us-east-1` bucket is recommended for best performance and cost (MotherDuck is currently hosted there). You'll need an `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` with read/write access (e.g. `AmazonS3FullAccess`).
- **Verified ngrok account** (free tier) — Estuary is a fully managed service, so the local database must be exposed to the internet. ngrok provides a TCP tunnel. Grab your **authtoken** from the ngrok dashboard.

> For this lab we use the default `postgres` superuser for simplicity. For production, create a dedicated Estuary replication user with the minimum required grants — see the [PostgreSQL connector docs](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/).

## Setup: start the source environment

1. Get your ngrok **authtoken** and paste it into `docker-compose.yaml`, replacing the `<enter ngrok token here>` placeholder under the `ngrok` service:

   ```yaml
   ngrok:
     image: ngrok/ngrok:latest
     environment:
       NGROK_AUTHTOKEN: <your-ngrok-authtoken>
     command: 'tcp postgres:5432'
     ports:
       - 4040:4040
   ```

2. From this folder, start the stack:

   ```bash
   docker compose up
   ```

   When Postgres is ready you'll see:

   ```
   postgres_cdc  | LOG:  database system is ready to accept connections
   ```

   The `datagen` service is verbose — it prints each record it generates. That's expected and confirms data is flowing.

   > **Note:** On first boot you may briefly see `ERROR: syntax error at or near "COMMENT"` from `init.sql`. Wait a minute and it resolves itself.

3. (Optional) Connect to Postgres to confirm data is being generated:

   ```bash
   docker exec -it postgres_cdc bash
   psql -h postgres_cdc -U postgres -d postgres   # password: postgres
   ```

   ```sql
   SELECT count(*) FROM products;
   ```

   Run the count again after a few seconds — it should increase.

### Get the public database endpoint

The Estuary connector connects to your database through the ngrok tunnel. Get the public `host:port` from the ngrok web UI at [http://localhost:4040](http://localhost:4040), or via:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
```

This returns something like `tcp://0.tcp.ngrok.io:12345`. **Strip the `tcp://` prefix** when pasting the address into Estuary.

### Source connection values

| Setting   | Value                                   |
|-----------|-----------------------------------------|
| Address   | the ngrok host:port (without `tcp://`)  |
| Database  | `postgres`                              |
| User      | `postgres`                              |
| Password  | `postgres`                              |

---

## Lab Exercise 1: End-to-end pipeline (soft delete)

### Step 1 — Create the Estuary capture

1. In the [Estuary dashboard](https://dashboard.estuary.dev), go to **Sources → + New Capture**.
2. Search for `postgres` and select the **real-time PostgreSQL** connector.
3. Name the capture `workshop` and pick the data plane closest to you (e.g. US).
4. Enter the [source connection values](#source-connection-values) above. Enable **History Mode** — you'll need it for Lab Exercise 3. Click **NEXT**.
5. On the binding/schema screen, leave the defaults:
   - **Schema evolution** (enabled): keep schemas up to date, add new collections, re-version collections on primary-key changes.
   - **Bindings**: the `products` table maps to a collection.
   - **Backfill**: Estuary performs an initial load of existing rows, then streams ongoing changes via CDC.
6. Click **NEXT**, then **SAVE AND PUBLISH**.

   > You may get a warning that the watermarks table doesn't exist. Ignore it — Estuary creates it for you.

7. The capture now appears under **Sources** and produces a collection named `<tenant>/workshop/public/products`.

### Step 2 — Inspect the collection

Open **Collections** in the left menu and drill into `<tenant>/workshop/public/products`. The document count should match the capture's metrics. (`Read By` shows `N/A` until a materialization reads from it.)

### Step 3 — Prepare the MotherDuck target

1. **S3 credentials:** In the AWS IAM console, create/select a user with read/write to your S3 bucket (e.g. `AmazonS3FullAccess`) in `us-east-1`. Note the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
2. **MotherDuck access token:** In MotherDuck, go to **Settings → Integrations → Access Token → + Create token**. Save the token string.
3. **MotherDuck S3 secret:** In MotherDuck, go to **Settings → Integrations → Secrets → + Add secrets**. Set Name, Secret Type = **Amazon S3**, Access Key ID, Secret Access Key, and Region `us-east-1`.
4. **Create a database** in MotherDuck called `lab1`.

### Step 4 — Create the MotherDuck materialization

1. Go to **Destinations → + New Materialization**.
2. Search for `motherduck` and select the **real-time MotherDuck** connector.
3. Name it `lab1` and pick your data plane.
4. Enter the MotherDuck connection details (access token), set **Bucket Path** to `lab1`. Click **NEXT**.

   > Below the credentials you can configure a **sync schedule** (time/timezone/days) for micro-batch applies to MotherDuck. Leave it default for the lab.

5. For **Target Resource Naming Convention**, choose **Use Table Name Only** (so it writes to the `main` schema rather than mirroring the source `public` schema).
6. Under **Advanced Options**, click **ADD** and select the `<tenant>/workshop/public/products` collection.
7. Click **NEXT**, then **SAVE AND PUBLISH**.

### Step 5 — Verify in MotherDuck

Open the MotherDuck UI and confirm rows have landed in the `lab1` database. The row count should match your Estuary capture and collection metrics.

### How it behaves (default settings)

**Capture side:**
- By default, Estuary **coalesces** change events — only the latest state per primary key is emitted (4 updates to the same row become 1).
- **History Mode** (enabled here) captures every transaction without reducing to a final state.

**Materialization side:**
- By default, Estuary performs **soft deletes**. On a delete, Estuary adds a metadata column marking the row for deletion (with `_meta/op` indicating the operation) but does not physically remove it.
- **Hard Delete** physically removes deleted rows — see Lab Exercise 2.
- **Delta Update** (combined with History Mode) inserts every change as a new row instead of overwriting — see Lab Exercise 3.

Change operation types in `_meta/op`: `c` = create/insert, `u` = update, `d` = delete.

---

## Lab Exercise 2: One-to-many topology (hard delete)

Add a second materialization that reads from the **same** collection but physically deletes records.

1. In MotherDuck, create a new database called `lab2`.
2. In Estuary, go to **Destinations → + New Materialization** and select the **MotherDuck** connector.
3. Name it `lab2`, pick your data plane.
4. Enter the MotherDuck connection details, **check the Hard Delete checkbox**, and set **Bucket Path** to `lab2`. Click **NEXT**.
5. Set **Target Resource Naming Convention** to **Use Table Name Only**.
6. Under **Advanced Options**, click **ADD** and select the `<tenant>/workshop/public/products` collection.
7. Under **Advanced Options → Config → Field Selection**, find the `_meta/op` column and click **EXCLUDE** (not needed here).
8. Click **NEXT**, then **SAVE AND PUBLISH**. Verify in MotherDuck's `lab2` database.

---

## Lab Exercise 3: Slowly Changing Dimension Type 2 (delta updates)

Add a third materialization that inserts every change (including updates and deletes) as a new row — ideal for audit/history tables in a warehouse.

History Mode is already enabled (from Lab Exercise 1). You also need full-row replica identity on the source so logical replication carries enough detail for SCD2:

```sql
ALTER TABLE products REPLICA IDENTITY FULL;
```

Then:

1. In MotherDuck, create a new database called `lab3`.
2. In Estuary, go to **Destinations → + New Materialization** and select the **MotherDuck** connector.
3. Name it `lab3`, pick your data plane.
4. Enter the MotherDuck connection details, set **Bucket Path** to `lab3`. Click **NEXT**.
5. Set **Target Resource Naming Convention** to **Use Table Name Only**.
6. Under **Advanced Options**, click **ADD** and select the `<tenant>/workshop/public/products` collection.
7. Under **Advanced Options → Config → Resource Configuration**, **check the Delta Update checkbox**. Click **NEXT**.
8. Click **NEXT**, then **SAVE AND PUBLISH**. Verify in MotherDuck's `lab3` database.

### Test it: update and delete a record

Stop the data generator so the table is stable:

```bash
docker stop datagen
```

Connect to Postgres (`docker exec -it postgres_cdc bash`, then `psql -h postgres_cdc -U postgres -d postgres`) and pick the lowest-id row:

```sql
SELECT * FROM products ORDER BY id ASC LIMIT 1;
```

Update it, then delete it (using the example id `17`):

```sql
UPDATE products SET name = 'SmallData' WHERE id = 17;
DELETE FROM products WHERE id = 17;
```

Check each MotherDuck database to compare behaviors:
- **`lab1` (soft delete):** the row remains with delete metadata set.
- **`lab2` (hard delete):** the row is physically removed.
- **`lab3` (SCD2 / delta update):** the insert, the update, and the delete each appear as separate rows — a full change history.

---

## Cleanup

```bash
docker compose down -v
```

Then disable or delete the capture and materializations from the Estuary dashboard, and drop the `lab1`/`lab2`/`lab3` databases in MotherDuck if you no longer need them.

## Next steps

- Swap the MotherDuck destination for another warehouse — e.g. [BigQuery](https://docs.estuary.dev/reference/Connectors/materialization-connectors/BigQuery/), [Snowflake](https://docs.estuary.dev/reference/Connectors/materialization-connectors/Snowflake/), or [Databricks](https://docs.estuary.dev/reference/Connectors/materialization-connectors/databricks/).
- Add an Estuary [derivation](https://docs.estuary.dev/concepts/derivations/) to transform the `products` collection in SQL, TypeScript, or Python before materializing.
- Point the same capture at additional tables by adding bindings.

## References

- [Estuary documentation](https://docs.estuary.dev)
- [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
- [MotherDuck materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/motherduck/)
- [Estuary dashboard](https://dashboard.estuary.dev) · [New capture](https://dashboard.estuary.dev/captures) · [New materialization](https://dashboard.estuary.dev/materializations)
