# Oracle CDC Capture Demo with Estuary (Free Oracle 23.6 in Docker)

Stream change data capture (CDC) from a free, local **Oracle Database 23.6** into [Estuary](https://estuary.dev) in real time. This example spins up Oracle Database 23ai Free in Docker, configures the LogMiner-based replication user that Estuary's Oracle connector needs, exposes the database to the internet with an ngrok TCP tunnel, and captures live `INSERT`/`UPDATE`/`DELETE` events into an Estuary collection — no Oracle license or Oracle Cloud account required.

Video walkthrough: https://www.youtube.com/watch?v=mE7LFSqfwY8

## Architecture

Estuary is fully managed, so it connects to the Oracle database over the public internet. Because Oracle runs locally in Docker here, an ngrok TCP tunnel publishes port `1521` to a reachable `host:port` that you paste into the capture config.

```
Oracle 23.6 Free (Docker)         Estuary (managed)
  ┌──────────────────┐
  │ FREE database    │            ┌──────────────────────┐
  │ c##estuary_flow_ │  ngrok     │  source-oracle       │
  │   user           │  TCP       │  capture (LogMiner)  │
  │ inventory table  │──tunnel──► │        │             │
  │ FLOW_WATERMARKS  │  :1521     │        ▼             │
  │ ARCHIVELOG mode  │            │  collection (JSON)   │
  └──────────────────┘            │        │             │
                                  │        ▼             │
                                  │  materialization     │
                                  │  (your destination)  │
                                  └──────────────────────┘
```

- **Capture (source):** the `source-oracle` connector uses Oracle LogMiner to read redo/archive logs and emit row-level CDC events.
- **Collection:** each captured table lands in an Estuary collection — a schematized JSON stream backed by cloud storage.
- **Materialization (optional):** push the collection downstream to a warehouse, database, or lake. Or transform it first with a derivation.

## What's included

- **`docker-compose.yaml`** — Brings up two services: `oracle-db` (image `oracle/database:23.6.0-free`, port `1521`, `ENABLE_ARCHIVELOG: true`) and `ngrok` (image `ngrok/ngrok:latest`) running `tcp oracle-db:1521` with its inspector UI on port `4040`. Oracle data persists to `./data` (gitignored).
- **`config/init.sql`** — Runs automatically on first boot (mounted at `/opt/oracle/scripts/setup`). It creates the common replication user `c##estuary_flow_user`, grants the privileges Estuary requires (`SELECT ANY TABLE`, `LOGMINING`, `SELECT_CATALOG_ROLE`, `EXECUTE_CATALOG_ROLE`, `SET CONTAINER`, etc.), enables supplemental logging (`ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`), creates the required `FLOW_WATERMARKS` watermarks table, and seeds a sample `inventory` table with three rows.
- **`.gitignore`** — Excludes `.DS_Store` and the local Oracle data volume (`data/*`).

## Prerequisites

- **Docker** (with `docker compose`).
- A **verified ngrok account** and authtoken — required because the local Oracle database must be reachable by Estuary's managed connector. Sign up at https://dashboard.ngrok.com.
- A free **Estuary account**: https://dashboard.estuary.dev.
- The **Oracle 23.6 Free container image** built locally (see Setup step 1). You do *not* need an Oracle license or Oracle Cloud account.

## Setup

### 1. Build the Oracle 23.6 Free container image

The compose file expects a local image tagged `oracle/database:23.6.0-free`. Oracle publishes the build scripts on GitHub: https://github.com/oracle/docker-images/tree/main/OracleDatabase/SingleInstance.

Clone that repo, switch to `OracleDatabase/SingleInstance`, and build:

```bash
./buildContainerImage.sh -v 23.6.0 -f
```

### 2. Configure secrets in the compose file

Edit `docker-compose.yaml` and set:

- `ORACLE_PWD` — the `SYS`/`SYSTEM` password for the database (replace `YOUR-PW`).
- `NGROK_AUTHTOKEN` — your ngrok authtoken (replace `YOUR-TOKEN`).

Then edit `config/init.sql` and change the password for the Estuary user. By default it is:

```sql
CREATE USER c##estuary_flow_user IDENTIFIED BY test123 CONTAINER=ALL;
```

Replace `test123` with a strong password and remember it — you'll paste it into the capture config.

### 3. Start the stack

```bash
docker compose up
```

Wait for the database to finish initializing (Oracle's first boot is slow). `init.sql` runs automatically once the database is ready, creating the user, grants, watermarks table, and sample `inventory` data.

### 4. Enable backups so LogMiner has archive logs to read

Oracle's CDC needs archived redo logs available. With the container running, open a shell and run an RMAN backup:

```bash
docker exec -it <your-container> bash
rman
```

Inside `rman`, log in as a DBA (enter your `ORACLE_PWD` when prompted) and configure retention + run a backup:

```
CONNECT TARGET "sys@FREE AS SYSDBA"
CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF 7 DAYS;
BACKUP DATABASE PLUS ARCHIVELOG;
```

### 5. Get the public ngrok endpoint

The ngrok inspector is exposed on port `4040`. Open http://localhost:4040 to see the public TCP forwarding address, or grab it from the command line:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r ".tunnels[0].public_url"
```

This returns something like `tcp://6.tcp.ngrok.io:14732`. **Strip the `tcp://` prefix** before pasting it into Estuary — the connector wants `host:port` (e.g. `6.tcp.ngrok.io:14732`).

## Configure the Estuary capture

Create a new capture in the Estuary dashboard with the **Oracle (Real-time)** / `source-oracle` connector: https://dashboard.estuary.dev/captures.

Enter these values (matching `docker-compose.yaml` and `config/init.sql`):

| Field | Value |
| --- | --- |
| **Server address** | Your ngrok endpoint, `host:port` (no `tcp://`) |
| **User** | `c##estuary_flow_user` |
| **Password** | The password you set for `c##estuary_flow_user` in `config/init.sql` |
| **Database** | `FREE` |

Click **Next** to let Estuary discover tables, select the `inventory` table (and any others), then **Save and Publish**. Estuary backfills the existing rows and then tails the redo logs for new changes.

Connector reference: https://docs.estuary.dev/reference/Connectors/capture-connectors/OracleDB/

## Verify

- In the dashboard, open the capture and watch the **bytes/docs read** metrics climb after publishing.
- Browse the captured collection in the Estuary UI — you should see the three seeded `inventory` rows (`Popcorn`, `Caramel corn`, `Cheese popcorn`).
- Test live CDC by inserting a row in Oracle and confirming it appears in the collection:

  ```sql
  INSERT INTO c##estuary_flow_user.inventory VALUES ('3456-nopq', 'Kettle corn', 549, 40);
  COMMIT;
  ```

If you use [flowctl](https://docs.estuary.dev/concepts/flowctl/), you can tail the collection directly:

```bash
flowctl collections read --collection <your-collection-name> --uncommitted | head
```

## Next steps

- **Materialize the data** to a destination (Snowflake, BigQuery, Postgres, MotherDuck, and more): https://dashboard.estuary.dev/materializations.
- **Transform with a derivation** in SQL, TypeScript, or Python: https://docs.estuary.dev/concepts/derivations/.

## References

- Video walkthrough: https://www.youtube.com/watch?v=mE7LFSqfwY8
- Oracle capture connector docs: https://docs.estuary.dev/reference/Connectors/capture-connectors/OracleDB/
- Estuary docs: https://docs.estuary.dev
- flowctl CLI: https://docs.estuary.dev/concepts/flowctl/
- Oracle Database Docker images: https://github.com/oracle/docker-images/tree/main/OracleDatabase/SingleInstance
