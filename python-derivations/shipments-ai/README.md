# Real-Time ML Feature Engineering with Python Derivations in Estuary: Shipment Delay Prediction

A Python [derivation](https://docs.estuary.dev/concepts/derivations/) in [Estuary](https://estuary.dev) that turns a real-time PostgreSQL CDC stream of `shipments` into an append-only, labeled training dataset for **shipment delay prediction**. As each shipment is delivered, the derivation emits exactly one feature row — per-customer aggregate features plus a binary `label` (1 = late, 0 = on time) — into the derived collection `dani-demo/python-derivations/shipment-delay-training`, ready to materialize to a warehouse and feed to a model.

This is a stateful, streaming feature store: features are computed incrementally from the change stream and snapshotted *before* the current delivery is applied to avoid label leakage. No batch jobs, no nightly recompute.

## Architecture

```
Artificial-Industries/postgres-shipments/public/shipments   (source collection, Postgres CDC)
        │  transform "shipments"  (shuffle key: /customer_id)
        ▼
shipment-delay-training.flow.py   (Python derivation: per-customer state + labeling)
        │  one Document per delivered shipment
        ▼
dani-demo/python-derivations/shipment-delay-training   (derived collection, key: /shipment_id)
        │  (optional)
        ▼
materialization → warehouse / feature store → model training
```

- **Source**: the existing collection `Artificial-Industries/postgres-shipments/public/shipments`, produced upstream by a [PostgreSQL CDC capture](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/).
- **Transform**: a single transform named `shipments`, shuffled on `/customer_id` so each customer's running state lives on exactly one worker.
- **Derivation**: maintains per-customer counters (`CustomerState`) restored on startup, updated per CDC event (`op` of `c`/`u`/`d`), and persisted via JSON merge patch in `start_commit()`.
- **Output**: the derived collection `dani-demo/python-derivations/shipment-delay-training`, keyed by `/shipment_id`. Because the key is the shipment and a row is only emitted once per delivery, the collection is effectively append-only training data.

## How the derivation works

The transform logic lives in [`shipment-delay-training.flow.py`](shipment-delay-training.flow.py). For every shipment document it receives:

1. **Skip malformed records** — documents missing `customer_id` or `shipment_status` are ignored.
2. **Snapshot features first** — before applying the current update, it snapshots the customer's current aggregates (`total_shipments`, `on_time_count`, `late_count`, `active_shipments`, `avg_delivery_days`). Snapshotting before the update prevents the delivery being scored from leaking into its own features.
3. **Apply the CDC update to state** — increments `total_shipments` for newly seen shipments, adjusts `active_shipments` on status transitions (active = `In Transit` / `At Checkpoint` / `Out for Delivery`), and on a first-time transition to `Delivered` updates the delivery counters via `_record_delivery()` (computing delivery days from `created_at` → `updated_at`, and on-time vs. late from `expected_delivery_date`).
4. **Emit one training row per delivered shipment** — only when a shipment transitions to `Delivered` does it `yield` a `Document` carrying the snapshotted features and a `label`. The label is computed by `_late_label()`: `expected_delivery_date` is treated as end-of-day (23:59:59 UTC) and compared against the CDC `updated_at` delivery time — `1` if delivered after that, else `0`.

State persistence:

- `__init__(open)` restores `State` from `open.state`.
- A `touched_customers` set tracks which customers changed during the transaction.
- `start_commit()` returns a `StartedCommit` with `merge_patch=True`, writing only the touched customers' state — not the entire state document. This scales to large customer counts with only the changed keys written per transaction.
- `reset()` clears in-memory state (used for catalog tests).

## What's included

| File | Role |
| --- | --- |
| [`flow.yaml`](flow.yaml) | The catalog spec for the derived collection `dani-demo/python-derivations/shipment-delay-training`: its `schema`, `key` (`/shipment_id`), the `python` derivation module, and the `shipments` transform sourcing `Artificial-Industries/postgres-shipments/public/shipments` shuffled on `/customer_id`. |
| [`shipment-delay-training.flow.py`](shipment-delay-training.flow.py) | The `Derivation(IDerivation)` class — the feature engineering and labeling logic described above. |
| [`shipment-delay-training.schema.yaml`](shipment-delay-training.schema.yaml) | JSON Schema for the emitted documents. Estuary validates every output row against it. |
| [`pyproject.toml`](pyproject.toml) | Python project metadata: `requires-python = ">=3.12"`, dependencies `pydantic>=2.0` and `pyright>=1.1`. |
| [`pyrightconfig.json`](pyrightconfig.json) | Points Pyright at `flow_generated/python` and enables `strict` type checking. |
| `flow_generated/` | Auto-generated typed stubs (`IDerivation`, `Document`, `Request`, `Response`, `SourceShipments`) imported at the top of the `.flow.py`. Regenerate with `flowctl generate`; do not edit by hand. |

### Output document schema

Each emitted training row (see [`shipment-delay-training.schema.yaml`](shipment-delay-training.schema.yaml)):

| Field | Type | Notes |
| --- | --- | --- |
| `shipment_id` | integer | Collection key. |
| `customer_id` | integer | The shipment's customer. |
| `total_shipments` | integer | Feature: customer's shipment count (snapshotted). |
| `on_time_count` | integer | Feature: prior on-time deliveries. |
| `late_count` | integer | Feature: prior late deliveries. |
| `active_shipments` | integer | Feature: currently in-transit shipments. |
| `avg_delivery_days` | number / null | Feature: rolling average delivery days. |
| `label` | integer (0 or 1) | Target: 1 = delivered late, 0 = on time. |

## Prerequisites

- A free [Estuary account](https://dashboard.estuary.dev).
- [`flowctl`](https://docs.estuary.dev/concepts/flowctl/) installed and authenticated. Python derivations are deployed (and their typed stubs generated) through the CLI.
- Python 3.12+ for local editing and `strict` type-checking.
- An existing **source collection** for the derivation to read. This example reads `Artificial-Industries/postgres-shipments/public/shipments`. Stand up the upstream shipments capture (PostgreSQL CDC via the [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)) — see the other examples in this repo for a synthetic source — or repoint the transform's `source` at your own collection.

## Deploy

Authenticate once, then publish the derivation. `flowctl` builds the Python module, generates types, runs any catalog tests, and deploys the derived collection.

```bash
# Authenticate (opens a browser; paste the CLI token)
flowctl auth login

# From this folder, publish the derivation
flowctl catalog publish --source flow.yaml --auto-approve
```

To (re)generate the typed stubs in `flow_generated/` without publishing:

```bash
flowctl generate --source flow.yaml
```

### Adapt to your tenant

Before publishing under your own account, edit [`flow.yaml`](flow.yaml):

1. Rename the derived collection from `dani-demo/python-derivations/shipment-delay-training` to a name under your tenant.
2. Point the `shipments` transform's `source` at the real collection produced by your capture (replacing `Artificial-Industries/postgres-shipments/public/shipments`).
3. Keep the `import` path in `shipment-delay-training.flow.py` aligned with the collection name — the generated package mirrors the collection path (`dani_demo.python_derivations.shipment_delay_training`). Run `flowctl generate --source flow.yaml` after renaming to refresh `flow_generated/`.

## Verify

Confirm the derived collection is producing training rows:

```bash
# Stream the derived collection (replace with your renamed collection)
flowctl collections read --collection dani-demo/python-derivations/shipment-delay-training --uncommitted | head

# Check the task's control-plane status
flowctl catalog status dani-demo/python-derivations/shipment-delay-training
```

The collection grows by one document each time a shipment is delivered. You can also watch document counts climb on the collection's page in the [Estuary dashboard](https://dashboard.estuary.dev).

## Next steps

- **Materialize the feature store**: push `shipment-delay-training` to a warehouse or database to power model training and dashboards — see the [materialization connectors](https://docs.estuary.dev/reference/Connectors/materialization-connectors/) (e.g. [BigQuery](https://docs.estuary.dev/reference/Connectors/materialization-connectors/BigQuery/), [Snowflake](https://docs.estuary.dev/reference/Connectors/materialization-connectors/Snowflake/)).
- **Extend the feature set or labeling rule**: add features to `CustomerState`/`_snapshot_features()`, or change `_late_label()` (for example, a grace window or a multi-class label).
- **Compare patterns**: this folder is one of several Python derivation examples — see the [parent README](../README.md) for the stateless, stateful aggregation, and streaming-join variants that read the same `shipments` source.

## References

- Estuary docs: https://docs.estuary.dev
- Derivations concept: https://docs.estuary.dev/concepts/derivations/
- flowctl CLI: https://docs.estuary.dev/concepts/flowctl/
- Collections: https://docs.estuary.dev/concepts/collections/
- PostgreSQL capture connector (upstream source): https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
- Materialization connectors (downstream destinations): https://docs.estuary.dev/reference/Connectors/materialization-connectors/
