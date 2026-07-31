# Real-Time Ad Performance Analytics with Estuary TypeScript Derivations and PostgreSQL CDC

Stream ad impression and ad click events from PostgreSQL into Estuary with change data capture (CDC), then use a stateful **TypeScript derivation** to join the two streams and maintain a running click count per advertising platform in real time. This example shows how to combine multiple captured collections in a single derivation, using `reduce` annotations to incrementally aggregate counts as new events arrive.

Watch the walkthrough video: https://youtu.be/dbHgn-AdVzU

## Architecture

A local PostgreSQL instance is seeded with synthetic ad-tech data and exposed to Estuary's managed control plane over an ngrok TCP tunnel. Estuary captures both tables via Postgres CDC into collections, and a TypeScript derivation reads both collections to emit a per-platform click count.

```
                         ┌──────────────────────────┐
  datagen ──INSERT──▶    │  PostgreSQL (wal_level=   │
  (impressions +         │  logical, flow_publication)│
   ~10% clicks)          │  ad_impressions, ad_clicks │
                         └────────────┬──────────────┘
                                      │ logical replication
                                ngrok tcp postgres:5432
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │ Estuary source-postgres capture│
                      └───────────────┬────────────────┘
                                      │
              ┌───────────────────────┴───────────────────────┐
              ▼                                                 ▼
   .../ad_impressions (collection)                 .../ad_clicks (collection)
              │                                                 │
              │ fromImpressions (click_count: 0)                │ fromClicks (click_count: 1)
              └───────────────────────┬─────────────────────────┘
                                      ▼
            TypeScript derivation: ad-clicks-by-platform
            keyed on /platform, reduce { sum } on click_count
                                      │
                                      ▼
                  (optional) materialize to any destination
```

How the derivation works:

- `fromImpressions` emits `{ platform, click_count: 0 }` for every impression, so every platform that has been seen shows up even with zero clicks.
- `fromClicks` emits `{ platform, click_count: 1 }` for every click.
- The collection is keyed on `/platform` with `reduce: { strategy: sum }` on `click_count`, so Estuary continuously sums the contributions into a live click tally per platform.

## What's included

- **`docker-compose.yml`** — spins up three services: `postgres-ad-performance` (PostgreSQL with `wal_level=logical`, port `5432`), `datagen-ad-performance` (the synthetic data generator), and `ngrok-ad-performance` (a TCP tunnel exposing `postgres:5432`, with the inspector UI on port `4040`).
- **`postgres/init.sql`** — bootstraps the database for CDC: creates the `flow_capture` replication user, the `flow_watermarks` table, the `flow_publication` publication, and the `ad_impressions` and `ad_clicks` tables, then adds all tables to the publication.
- **`datagen/datagen.py`** — continuously inserts fake `ad_impressions` (using `Faker`); for roughly 10% of impressions it also inserts a related `ad_clicks` row. Connection is configured via `POSTGRES_*` environment variables.
- **`datagen/Dockerfile`** / **`datagen/requirements.txt`** — Python 3.12 image with `Faker==25.1.0` and `psycopg2==2.9.9`.
- **`derivation/flow.yaml`** — defines the `ad-clicks-by-platform` derived collection: its schema (with the `sum` reduce on `click_count`), key (`/platform`), and the two TypeScript transforms (`fromImpressions`, `fromClicks`) and their source collections.
- **`derivation/ad-clicks-by-platform.flow.ts`** — the TypeScript transform logic for both `fromImpressions` and `fromClicks`.
- **`derivation/deno.json`** — import map pointing `flow/` at the generated TypeScript types.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- A free [ngrok](https://ngrok.com/) account and authtoken (the local Postgres must be reachable by Estuary's managed connector)
- A free [Estuary account](https://dashboard.estuary.dev)
- [`flowctl`](https://docs.estuary.dev/concepts/flowctl/) installed and authenticated, to publish the derivation

## Setup

### 1. Start the stack

```bash
export NGROK_AUTHTOKEN=<your-ngrok-authtoken>
docker compose up
```

This starts PostgreSQL, runs `postgres/init.sql`, begins generating ad events, and opens the ngrok TCP tunnel.

### 2. Get the public PostgreSQL endpoint

Read the tunnel's public address from the ngrok inspector at http://localhost:4040, or via the API:

```bash
curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'
```

This prints something like `tcp://6.tcp.ngrok.io:18923`. Strip the `tcp://` prefix when pasting host/port into Estuary.

## Configure the Estuary capture

Create a PostgreSQL CDC capture so the `ad_impressions` and `ad_clicks` tables become Estuary collections. You can do this in the [Estuary dashboard](https://dashboard.estuary.dev/captures) with the **source-postgres** connector ([connector docs](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)).

Use these connection values (from `docker-compose.yml` and `postgres/init.sql`):

| Field | Value |
| --- | --- |
| Server Address | the ngrok host:port from step 2 (without `tcp://`) |
| User | `flow_capture` |
| Password | `password` |
| Database | `postgres` |

Discover and enable the `public.ad_impressions` and `public.ad_clicks` bindings. After publishing, the capture writes to two collections (named under your tenant prefix, e.g. `<tenant>/.../ad_impressions` and `<tenant>/.../ad_clicks`).

> The `flow.yaml` in this repo references the source collections as `dani-demo/demo-ad-performance/ad_impressions` and `dani-demo/demo-ad-performance/ad_clicks`. These are placeholders (marked `# Modify this`) — replace them with the actual collection names your capture produces.

## Deploy the TypeScript derivation

The derivation lives in `derivation/`. Before publishing, update `derivation/flow.yaml` to match your environment:

1. Rename the derived collection `dani-demo/demo-ad-performance/ad-clicks-by-platform` to your own tenant prefix (e.g. `<tenant>/ad-performance/ad-clicks-by-platform`).
2. Point the two transform `source.name` fields at the real collection names produced by your capture.
3. Update the import path in `ad-clicks-by-platform.flow.ts` if you renamed the collection.

Then authenticate and publish:

```bash
flowctl auth login
cd derivation
flowctl catalog publish --source flow.yaml --auto-approve
```

`flowctl` generates the TypeScript types (resolved through `deno.json` at `flow/...`), builds the derivation, and deploys it.

## Verify

Confirm the captured streams and the derived aggregate are flowing:

```bash
# Raw click events
flowctl collections read --collection <tenant>/.../ad_clicks --uncommitted | head

# Per-platform click counts (one document per platform, with a running sum)
flowctl collections read --collection <tenant>/.../ad-clicks-by-platform --uncommitted | head
```

You should see one document per `platform` (`Google Ads`, `Facebook Ads`, `Twitter Ads`) with a `click_count` that increases over time as `datagen` produces more clicks. You can also watch document counts climb on the collection's page in the [Estuary dashboard](https://dashboard.estuary.dev).

## Next steps

- Materialize the `ad-clicks-by-platform` collection to a warehouse or database to power a live dashboard — see [materialization connectors](https://docs.estuary.dev/reference/Connectors/materialization-connectors/).
- Extend the derivation to track conversions (the `conversion_flag` column on `ad_clicks`) or compute click-through rate by also summing impressions.
- Learn more about derivations: https://docs.estuary.dev/concepts/derivations/

## References

- Demo video: https://youtu.be/dbHgn-AdVzU
- Estuary docs: https://docs.estuary.dev
- PostgreSQL capture connector: https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
- Derivations: https://docs.estuary.dev/concepts/derivations/
- flowctl: https://docs.estuary.dev/concepts/flowctl/
