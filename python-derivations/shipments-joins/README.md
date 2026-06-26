# Streaming Left Join in Python: Enrich Shipments with Customer Reference Data using Estuary Derivations

A **Python derivation** for [Estuary](https://estuary.dev) that maintains a continuously-updated **streaming left join** across two real-time collections. It joins a PostgreSQL `shipments` collection (left side) with a Google Sheets `customer-tiers` reference collection (right side) on `customer_id`, emitting an enriched `enriched-shipments` collection where every shipment carries its customer's `customer_tier`, `customer_region`, and `account_manager`. Because it is a left join, shipments are always emitted — even before a matching tier row exists — and re-emitted whenever either side changes.

This is the streaming-join / enrichment example in the [Python Derivations collection](../README.md). See the parent README for the shared project layout, what a Python derivation is, and how state and merge-patch persistence work.

## Architecture

A derivation is an Estuary collection that is continuously computed from one or more source collections. This one reads from two sources and is **stateful**: it holds both sides of the join in memory (partitioned by join key), persists that state on every transaction, and restores it on restart.

```text
Artificial-Industries/postgres-shipments/public/shipments   (left, CDC)
                                  │
                                  │  transform: shipments  (shuffle: any)
                                  ▼
        ┌──────────────────────────────────────────────────┐
        │  Derivation (enriched-shipments.flow.py)           │
        │  in-memory State: customer_id -> { tier, shipments }│
        │  LEFT JOIN on customer_id                          │
        └──────────────────────────────────────────────────┘
                                  ▲
                                  │  transform: customer_tiers  (shuffle key: /customer_id)
                                  │
        dani-demo/customer-tiers/Sheet1   (right, Google Sheet reference data)
                                  │
                                  ▼
        dani-demo/python-derivations/enriched-shipments   (derived collection, key: /shipment_id)
```

- **Left side — `shipments`** (`Artificial-Industries/postgres-shipments/public/shipments`): each shipment is stored in state and immediately emitted, enriched with whatever tier data is currently known for its `customer_id` (enrichment fields are `null` if no tier row has arrived yet). This is what makes it a *left* join.
- **Right side — `customer_tiers`** (`dani-demo/customer-tiers/Sheet1`): when a tier row arrives or changes, the tier is stored and **all of that customer's shipments are re-emitted** with the updated `customer_tier`, `customer_region`, and `account_manager`.
- **Join state** is keyed by `customer_id`. For each customer it holds one `CustomerTier` (right) and a `dict[shipment_id -> ShipmentData]` (left, one-to-many). State is persisted via a JSON **merge patch** (`merge_patch=True`) so each transaction only writes the customers it touched.
- **Output** lands in `dani-demo/python-derivations/enriched-shipments`, keyed by `/shipment_id`.

### Join and CDC semantics

Both sources are change streams, so the derivation honors the CDC operation at `doc._meta.op` (`c` create, `u` update, `d` delete):

- A shipment **delete** (`op == 'd'`) removes the shipment from state and emits nothing for it.
- A customer-tier **delete** clears the tier and re-emits that customer's shipments with `null` enrichment fields (a Google Sheets row removal arrives as a delete).
- The Google Sheet stores `customer_id` as a string; the derivation casts it to `int` before joining against the integer `customer_id` from shipments.

## What's included

| File | Role |
| --- | --- |
| `flow.yaml` | Catalog spec for the derived collection `dani-demo/python-derivations/enriched-shipments`: its `schema`, `key: [/shipment_id]`, the Python module, and the two `transforms` (`shipments` with `shuffle: any`, `customer_tiers` shuffled on `/customer_id`). |
| `enriched-shipments.flow.py` | The `Derivation(IDerivation)` class implementing the left join: `shipments()` (left), `customer_tiers()` (right), `start_commit()` (merge-patch persistence), `__init__()` (state restore), and `reset()`. |
| `enriched-shipments.schema.yaml` | JSON Schema of the derived documents — shipment fields plus the `customer_tier` / `customer_region` / `account_manager` enrichment fields. `required: [shipment_id, customer_id]`. |
| `pyproject.toml` | Python project metadata: `pydantic>=2.0`, `pyright>=1.1`, `requires-python = ">=3.12"`. |
| `pyrightconfig.json` | Points Pyright at `flow_generated/python` and enables `strict` type checking. |
| `flow_generated/` | Auto-generated types (`IDerivation`, `Document`, `Request`, `Response`, `SourceShipments`, `SourceCustomerTiers`). Regenerate with `flowctl generate`; do not edit by hand. |

### The transform logic

The class keeps a root `State(customers: dict[int, JoinState])`. Each `JoinState` holds:

- `tier: CustomerTier | None` — the right side (one per customer: `tier`, `region`, `account_manager`).
- `shipments: dict[int, ShipmentData]` — the left side, keyed by `shipment_id` for efficient updates.

`shipments(read)` upserts the incoming shipment into state and `yield`s one enriched `Document`. `customer_tiers(read)` upserts the tier and `yield`s one `Document` **per stored shipment** for that customer, propagating the new enrichment to existing rows. A `touched_customers` set tracks which keys changed within a transaction so `start_commit()` writes only those, with `merge_patch=True`.

The derived `Document` fields are: `shipment_id`, `customer_id`, `shipment_status`, `is_priority`, `city`, `expected_delivery_date`, `customer_tier`, `customer_region`, `account_manager`.

## Prerequisites

- A free [Estuary account](https://dashboard.estuary.dev).
- [`flowctl`](https://docs.estuary.dev/concepts/flowctl/) installed and authenticated. Derivations are deployed and their typed stubs generated via the CLI.
- Python 3.12+ for local editing and strict type-checking.
- The two **source collections** this derivation reads from, already present in your tenant:
  - A `shipments` collection. The reference is `Artificial-Industries/postgres-shipments/public/shipments`, produced by a [PostgreSQL CDC capture](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/). Stand one up with [`../../shipments-datagen`](../../shipments-datagen) or repoint the `source`.
  - A `customer-tiers` reference collection. The reference is `dani-demo/customer-tiers/Sheet1`, produced by the [Google Sheets capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/google-sheets/) with columns `customer_id`, `tier`, `region`, `account_manager`.

## Deploy the derivation

Edit `flow.yaml` to match your tenant first (see [Adapt to your environment](#adapt-to-your-environment)), then publish with `flowctl`. The CLI builds the Python module, generates types, runs catalog tests, and deploys the derived collection.

```bash
# Authenticate once
flowctl auth login

# Publish the enriched-shipments derivation
flowctl catalog publish --source flow.yaml --auto-approve
```

To (re)generate the typed stubs in `flow_generated/` without publishing:

```bash
flowctl generate --source flow.yaml
```

### Adapt to your environment

The collection names in `flow.yaml` use a `dani-demo` / `Artificial-Industries` tenant. Before publishing:

1. Rename the derived collection `dani-demo/python-derivations/enriched-shipments` to a name under your tenant.
2. Point the `shipments` transform `source` at your real shipments collection (replace `Artificial-Industries/postgres-shipments/public/shipments`).
3. Point the `customer_tiers` transform `source` at your reference collection (replace `dani-demo/customer-tiers/Sheet1`). Keep its shuffle on `/customer_id`.
4. Keep the `import` path in `enriched-shipments.flow.py` aligned with the derived collection name — the generated package mirrors the collection path. Run `flowctl generate --source flow.yaml` after renaming to refresh `flow_generated/`.

## Verify

Confirm the derived collection is producing enriched documents:

```bash
# Stream the derived collection (use your renamed collection if you changed it)
flowctl collections read --collection dani-demo/python-derivations/enriched-shipments --uncommitted | head

# Check the derivation task's control-plane status
flowctl catalog status dani-demo/python-derivations/enriched-shipments
```

Each output document is a shipment with `customer_tier`, `customer_region`, and `account_manager` populated when a matching `customer-tiers` row exists, or `null` when it does not. Edit a row in the source Google Sheet and you should see that customer's shipments re-emitted with the updated enrichment. You can also watch document counts on the collection's page in the [Estuary dashboard](https://dashboard.estuary.dev).

## Next steps

- Materialize `enriched-shipments` to a warehouse or database to power dashboards or operational queries — see the [materialization connectors](https://docs.estuary.dev/reference/Connectors/materialization-connectors/).
- Add more reference sources (e.g. carrier or SLA tables) as additional right-side transforms to layer in more enrichment.
- Compare with the other patterns in this collection: the [stateless map](../shipments-stateless), the [stateful aggregation](../shipments-stateful), and the [ML feature pipeline](../shipments-ai).

## References

- Estuary docs: https://docs.estuary.dev
- Derivations concept: https://docs.estuary.dev/concepts/derivations/
- flowctl CLI: https://docs.estuary.dev/concepts/flowctl/
- PostgreSQL capture connector (left source): https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
- Google Sheets capture connector (right source): https://docs.estuary.dev/reference/Connectors/capture-connectors/google-sheets/
- Materialization connectors (downstream destinations): https://docs.estuary.dev/reference/Connectors/materialization-connectors/
