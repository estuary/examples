# Python Derivations in Estuary: Stateless Transforms, Stateful Aggregation, Streaming Joins, and ML Feature Engineering

Four self-contained example projects that show how to transform real-time collections with **Python derivations** in [Estuary](https://estuary.dev). Each project reads the same captured PostgreSQL `shipments` collection and demonstrates a distinct derivation pattern: a stateless map/transform, a stateful per-customer aggregation with persisted state and JSON merge-patch, a streaming left join that enriches shipments with reference data, and an append-only feature pipeline that emits labeled training rows for shipment delay prediction.

These examples assume you already have an upstream capture producing a shipments collection (see [`../shipments-datagen`](../shipments-datagen) for the synthetic PostgreSQL source). Every derivation here sources from the collection `Artificial-Industries/postgres-shipments/public/shipments` and writes into the `dani-demo/python-derivations/...` namespace — rename both to match your own tenant before publishing.

## What is a Python derivation?

A [derivation](https://docs.estuary.dev/concepts/derivations/) is an Estuary collection that is continuously computed from one or more source collections. As new documents arrive in a source, Estuary runs your transform and produces output documents in the derived collection. Derivations can be written in SQL, TypeScript, or **Python** — these examples use Python.

A Python derivation is a class implementing the generated `IDerivation` interface, with one `async` method per named transform. Each method receives a `read.doc` (a typed Pydantic model of the source document, including CDC metadata at `doc._meta`/`doc.m_meta`) and `yield`s output `Document`s. Derivations can be:

- **Stateless** — pure functions of the input document (`shipments-stateless`).
- **Stateful** — they restore persisted state in `__init__(open)`, maintain it in memory, and durably persist it in `start_commit()` (`shipments-stateful`, `shipments-joins`, `shipments-ai`). State is partitioned by the transform's `shuffle.key`, so each worker only manages state for its assigned keys.

Stateful derivations persist their state with a **JSON merge patch** (`merge_patch=True`), so each transaction only writes the keys that changed — not the entire state document. This scales to millions of keys with thousands changing per transaction.

## The four projects

| Folder | Pattern | Source(s) | Derived collection | Key |
| --- | --- | --- | --- | --- |
| [`shipments-stateless/`](shipments-stateless) | Stateless map / field derivation | `shipments` | `processed-shipments` | `/id` |
| [`shipments-stateful/`](shipments-stateful) | Stateful per-customer aggregation + merge-patch | `shipments` | `customer-metrics` | `/customer_id` |
| [`shipments-joins/`](shipments-joins) | Streaming left join (enrichment) | `shipments` + `customer-tiers` (Google Sheet) | `enriched-shipments` | `/shipment_id` |
| [`shipments-ai/`](shipments-ai) | ML feature engineering (append-only training rows) | `shipments` | `shipment-delay-training` | `/shipment_id` |

### `shipments-stateless` — stateless map/transform

The simplest pattern. The `shipments` transform reads each shipment document and emits a reshaped one: it concatenates `street_address` + `city` into `full_address`, derives an `is_urgent` flag (priority shipments or `delayed`/`critical` status), builds a `status_summary` string, and computes `days_until_delivery` from `expected_delivery_date`. No state, no `start_commit`. The transform uses `shuffle: any` and `backfill: 1`. See [`shipments-stateless/processed-shipments.flow.py`](shipments-stateless/processed-shipments.flow.py).

### `shipments-stateful` — stateful aggregation with persisted state

Maintains a running profile per customer (`total_shipments`, `on_time_count`, `late_count`, `active_shipments`, `avg_delivery_days`, `is_vip`, `last_shipment_date`). It restores state in `__init__`, processes CDC operations (`doc._meta.op` of `c`/`u`/`d`) to correctly handle status transitions (e.g. `In Transit` → `Delivered`) and deletions without double-counting, then persists only the customers it touched via a merge patch in `start_commit()`. The transform shuffles on `/customer_id` so each customer's state lives on one worker. This folder ships its own [`README.md`](shipments-stateful/README.md) with a Mermaid diagram of the full lifecycle. See [`shipments-stateful/customer-metrics.flow.py`](shipments-stateful/customer-metrics.flow.py).

### `shipments-joins` — streaming left join / enrichment

A continuously-maintained LEFT JOIN across two collections. The `shipments` transform (left side, `shuffle: any`) stores each shipment in state and emits it enriched with whatever tier data is known. The `customer_tiers` transform (right side, from the Google Sheet collection `dani-demo/customer-tiers/Sheet1`, shuffled on `/customer_id`) stores tier reference data and **re-emits all of that customer's shipments** with the updated `customer_tier`, `customer_region`, and `account_manager`. Because it is a left join, shipments are always emitted even when no tier row exists yet (enrichment fields are null). See [`shipments-joins/enriched-shipments.flow.py`](shipments-joins/enriched-shipments.flow.py).

### `shipments-ai` — ML feature engineering for delay prediction

An append-only training-data pipeline. It maintains the same per-customer aggregate state as `shipments-stateful`, but emits exactly **one labeled training row per delivered shipment**. The features (`total_shipments`, `on_time_count`, `late_count`, `active_shipments`, `avg_delivery_days`) are snapshotted *before* the current shipment is applied, to avoid label leakage, and the binary `label` (1 = late, 0 = on time) is computed by comparing the CDC `updated_at` delivery time against `expected_delivery_date`. The resulting collection is a ready-to-train feature store you can materialize to a warehouse and feed to a model. See [`shipments-ai/shipment-delay-training.flow.py`](shipments-ai/shipment-delay-training.flow.py).

## Project layout

Each subfolder is a self-contained Estuary Python derivation project with the same structure:

```
shipments-<pattern>/
├── flow.yaml                         # Defines the derived collection: schema, key, transforms, source(s)
├── <name>.flow.py                    # The Derivation class — your transform logic
├── <name>.schema.yaml                # JSON Schema of the derived collection's documents
├── pyproject.toml                    # Python project (pydantic, pyright); requires-python >=3.12
├── pyrightconfig.json                # Strict type-checking against flow_generated/python
└── flow_generated/                   # Auto-generated types (IDerivation, Document, Request, SourceShipments...)
```

- **`flow.yaml`** — the catalog spec. It names the derived collection, its `schema` and `key`, selects `using.python.module: <name>.flow.py`, and lists the `transforms` (each with a `name`, a `source` collection, and a `shuffle` of `any` or a `key`).
- **`<name>.flow.py`** — a `Derivation(IDerivation)` class with one `async def <transform_name>(self, read) -> AsyncIterator[Document]` method per transform. Stateful variants also implement `__init__(open)`, `start_commit(...)`, and `reset()`.
- **`<name>.schema.yaml`** — the JSON Schema for the derived documents. Estuary validates every emitted document against it.
- **`pyproject.toml`** — declares dependencies (`pydantic>=2.0`, `pyright>=1.1`) and `requires-python = ">=3.12"`.
- **`pyrightconfig.json`** — points Pyright at `flow_generated/python` and enables `strict` mode for fully-typed transforms.
- **`flow_generated/`** — generated by `flowctl`. It contains the typed `IDerivation`, `Document`, `Request`, `Response`, and source models (e.g. `SourceShipments`) imported at the top of each `.flow.py`. Do not edit by hand; regenerate with `flowctl generate`.

## Prerequisites

- A free [Estuary account](https://dashboard.estuary.dev).
- [`flowctl`](https://docs.estuary.dev/concepts/flowctl/) installed and authenticated — derivations are deployed and their types generated via the CLI.
- Python 3.12+ (for local editing, type-checking, and the catalog tests).
- An existing **source collection** for these derivations to read from. All four read `Artificial-Industries/postgres-shipments/public/shipments`; `shipments-joins` additionally reads a Google Sheets collection (`dani-demo/customer-tiers/Sheet1`). Stand up the upstream shipments capture with [`../shipments-datagen`](../shipments-datagen) (PostgreSQL CDC via the [PostgreSQL capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)), or repoint the `source` fields at your own collections.

## Deploy a derivation

Pick a project and publish it with `flowctl`. The CLI builds the Python module, generates types, runs any catalog tests, and deploys the derived collection.

```bash
# Authenticate once
flowctl auth login

# Deploy one of the derivations
cd shipments-stateful
flowctl catalog publish --source flow.yaml --auto-approve
```

Before publishing, edit `flow.yaml` to match your environment:

1. Rename the derived collection from `dani-demo/python-derivations/<name>` to a name under your tenant.
2. Point each transform's `source` at the real collection name produced by your capture (replace `Artificial-Industries/postgres-shipments/public/shipments`, and for `shipments-joins` the `dani-demo/customer-tiers/Sheet1` reference).
3. Keep the `import` path in the `.flow.py` aligned with the collection name (the generated package mirrors the collection path, e.g. `dani_demo.python_derivations.customer_metrics`). Run `flowctl generate --source flow.yaml` after renaming to refresh `flow_generated/`.

To (re)generate the typed stubs without publishing:

```bash
flowctl generate --source flow.yaml
```

## Verify

Confirm the derived collection is producing documents:

```bash
# Stream the derived collection (replace with your renamed collection)
flowctl collections read --collection dani-demo/python-derivations/customer-metrics --uncommitted | head

# Check the task's control-plane status
flowctl catalog status dani-demo/python-derivations/customer-metrics
```

You can also watch document counts climb on the collection's page in the [Estuary dashboard](https://dashboard.estuary.dev). For example, `customer-metrics` produces one document per `customer_id` whose counters update as new shipment events arrive, while `shipment-delay-training` grows by one row each time a shipment is delivered.

## Next steps

- Materialize any of these derived collections to a warehouse or database to power dashboards or model training — see the [materialization connectors](https://docs.estuary.dev/reference/Connectors/materialization-connectors/).
- Adapt the patterns: change the aggregation logic in `shipments-stateful`, add more reference sources to `shipments-joins`, or extend the feature set / labeling rule in `shipments-ai`.
- Compare with the TypeScript and SQL derivation examples elsewhere in this repo (e.g. [`../derivations-ad-performance`](../derivations-ad-performance), [`../derivations-sql-full-outer-join`](../derivations-sql-full-outer-join)).

## References

- Estuary docs: https://docs.estuary.dev
- Derivations concept: https://docs.estuary.dev/concepts/derivations/
- flowctl CLI: https://docs.estuary.dev/concepts/flowctl/
- Collections: https://docs.estuary.dev/concepts/collections/
- PostgreSQL capture connector (upstream source): https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
- Materialization connectors (downstream destinations): https://docs.estuary.dev/reference/Connectors/materialization-connectors/
