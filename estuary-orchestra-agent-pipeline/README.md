# Build a Pipeline by Talking to Your Agent: Estuary + Orchestra Skills

This demo builds a complete, production-shaped data pipeline **without writing
the connectors or the orchestration by hand** — you describe what you want in
plain English and two sets of [agent skills](https://agentskills.io) do the
work:

- **[Estuary agent skills](https://docs.estuary.dev/guides/agent-skills/)** set
  up **ingestion** — real-time CDC from Postgres into Snowflake.
- **[Orchestra agent skills](https://docs.getorchestra.io/docs/guides/orchestra-skills)**
  set up **orchestration** — a scheduled dbt transform plus data-quality checks
  on top of that Snowflake data.

The result is the classic split done right: Estuary streams the raw data
continuously in real time, and Orchestra runs the batch transform-and-validate
layer on a schedule on top of it.

The two aren't just adjacent — they're **wired together**. Orchestra has a
native `ESTUARY` integration, so the first thing the batch pipeline does is ask
Estuary whether ingestion is actually healthy and fresh. If the capture has
stalled, the dbt build and every quality test **skip** instead of quietly
rebuilding dashboards on stale data.

```
 Postgres (orders)                    ┌─ Estuary agent skills build this ─┐
   │  inserts / updates / deletes     │                                   │
   ▼                                  ▼                                   │
 Estuary capture  ───►  Estuary collection  ───►  Snowflake               │
 (source-postgres)      agent-demo/public/orders   (raw mirror)           ┘
                                                            │
                      ┌─ Orchestra agent skills build this ─┤
                      ▼                                     │
        ① ESTUARY_CHECK_FLOW  ──(healthy? fresh?)──┐         │
              on both Estuary tasks                │         │
                                                   ▼         ▼
                                      ② dbt build (stg_orders,
                                         daily_order_metrics)
                                                   │
                                                   ▼
                                      ③ SNOWFLAKE_RUN_TEST × 4
                                    (freshness, revenue, enum, future dates)
                                         scheduled daily by Orchestra
```

## What's in this repo

Everything here is either **scaffolding you run** (the source database) or an
**example of what the agents produce** (the Flow specs, the dbt project, the
Orchestra pipeline) so you can version-control and inspect the output.

| Path | What it is | Who authors it |
|------|------------|----------------|
| `docker-compose.yml` | Postgres (`wal_level=logical`) + order generator | you run it |
| `postgres/init.sql` | `orders` table + CDC prerequisites | you |
| `generator/` | Python order generator (insert/update/delete traffic) | you |
| `snowflake/setup.sql` | Shared Snowflake DB, role, user, warehouse | you |
| `flow/` | Capture + Snowflake materialization specs | **Estuary skills** |
| `dbt/` | Staging + marts models and tests over the streamed data | you (or an agent) |
| `orchestra/daily_orders_pipeline.yaml` | Estuary gate → dbt → quality-test DAG | **Orchestra skills** |

## Prerequisites

- Docker, and [`ngrok`](https://ngrok.com/) (to expose local Postgres to Estuary)
- An [Estuary account](https://dashboard.estuary.dev) and a Snowflake account
- An [Orchestra account](https://app.getorchestra.io) + an Orchestra API key
- **Claude Code** (or Cursor / Codex) — this is where you'll talk to the skills

---

## Step 0 — Install both sets of agent skills

In Claude Code, add both marketplaces and install the plugins:

```text
# Estuary — ingestion
/plugin marketplace add estuary/agent-skills
/plugin install estuary-captures@estuary
/plugin install estuary-materializations@estuary
/plugin install estuary-operations@estuary

# Orchestra — orchestration
/plugin marketplace add orchestra-hq/orchestra-skills
/plugin install orchestra@orchestra-marketplace
```

Orchestra's skills act through its MCP server, which is how the agent lists
runs, reads logs, validates YAML, and publishes pipelines. Add it with your API
key (Orchestra UI → Settings → Workspace):

```bash
claude mcp add orchestra https://mcp.getorchestra.io/orchestra \
  --transport http \
  --header "Authorization: Bearer <YOUR_ORCHESTRA_API_KEY>"
```

Restart Claude Code afterwards so the new plugins and MCP tools load.

> The skills follow the open `SKILL.md` standard, so the same instructions work
> in Cursor or Codex — see each project's docs for the copy-in setup.

## Step 1 — Stand up the source

Bring up Postgres (pre-configured for logical replication) and the generator
that continuously writes orders, so there are always live changes to stream:

```bash
docker compose up -d          # Postgres + traffic generator
ngrok tcp 5432                # note the host:port ngrok prints
```

Then create the Snowflake environment by running `snowflake/setup.sql` in a
Snowflake worksheet. It creates `AGENT_DEMO` (with `PUBLIC` for the raw mirror
and `ANALYTICS` for dbt output), plus the `AGENT_USER` service account and
`AGENT_WH` warehouse used by both tools.

## Step 2 — Ingestion, built by the Estuary skills

Now just describe the pipeline to Claude Code. The skills run the `flowctl`
workflow, generate the specs, and publish them. Typical prompts, in order:

> **"Check that flowctl is installed and authenticated."**
> *(`estuary-flowctl-setup` installs/authenticates the CLI)*

> **"Capture my Postgres orders table into Estuary. It's reachable at the ngrok
> address `4.tcp.ngrok.io:12345`, database `postgres`, user `flow_capture`,
> password `secret`."**
> *(`capture-postgres-create` runs connector discovery, then publishes the
> capture and collection)*

> **"Materialize the orders collection into Snowflake, database `AGENT_DEMO`,
> schema `PUBLIC`, warehouse `AGENT_WH`, user `AGENT_USER` with key-pair auth."**
> *(`materialize-snowflake-create` writes and publishes the destination spec)*

> **"Is my capture healthy and is data flowing to Snowflake?"**
> *(`estuary-task-health` / `estuary-task-stats` confirm the stream)*

Within a minute or two the Snowflake table is a live mirror of the Postgres
table, and stays in sync as the generator keeps writing.

The specs the skills produce look like the ones checked in under [`flow/`](flow/).
Two habits worth copying from them:

- **Let the connector discover the schema.** Write just the `endpoint` block with
  `bindings: []`, then `flowctl discover --source flow/capture.flow.yaml`. It
  fills in bindings and an accurate inferred schema.
- **Validate before publishing.** `flowctl catalog test --source …` connects to
  the real destination and checks the config without writing anything.

## Step 3 — Orchestration, built by the Orchestra skills

Estuary keeps the raw data fresh; Orchestra owns the transform-and-validate
layer on top. This repo ships a small [dbt project](dbt/) that reads the
streamed mirror as a source and builds:

- `stg_orders` — typed, cleaned staging view (Estuary lands `numeric` and
  `timestamptz` as JSON strings, so this casts them)
- `daily_order_metrics` — daily order counts and revenue by status

Point Claude Code at it and let the Orchestra skills author and publish the DAG:

> **"Create an Orchestra pipeline that first checks my Estuary capture and
> materialization are healthy, then runs `dbt build` on the dbt project in
> `./dbt`, then runs Snowflake data-quality tests — daily."**
> *(`create-orchestra-pipeline` authors the YAML and validates it against the
> real API via MCP before publishing)*

> **"Add Snowflake quality tests on the marts: freshness within 24h, no null or
> negative revenue, status values limited to the known enum, and no
> future-dated events."**
> *(`write-snowflake-dq-tests` profiles the tables and writes the assertions)*

The result is [`orchestra/daily_orders_pipeline.yaml`](orchestra/daily_orders_pipeline.yaml).

### The hinge: gating batch work on stream health

Stage one is what makes the two tools a system rather than two pipelines:

```yaml
check_capture:
  integration: ESTUARY
  integration_job: ESTUARY_CHECK_FLOW
  parameters:
    task: yourprefix/agent-demo/source-postgres
    error_threshold: 0        # any new error fails the run
    latency_threshold: 900    # seconds since the task last published data
```

`ESTUARY_CHECK_FLOW` reads the task's OpenMetrics endpoint and compares deltas
since the last check, so a stalled capture is caught **here** — as an explicit
failure with everything downstream marked `SKIPPED` — rather than showing up
days later as quietly stale dashboards.

The quality tests use `SNOWFLAKE_RUN_TEST`, where each `statement` is written so
that **returning rows means something is wrong**, and
`error_threshold_expression: '> 0'` turns any violating row into a failure.

### Connections

Tasks reference credentials as `${{ ENV.* }}` environment variables — never
inline secrets. Create three connections in the Orchestra UI under
**Settings → Connections**, then map them to variables under **Environments**:

| Connection | Variable | Needs |
|---|---|---|
| Estuary | `ESTUARY_CONNECTION` | An Estuary access token |
| dbt Core | `DBT_CONNECTION` | Git repo + read token + `profiles.yml` |
| Snowflake | `SNOWFLAKE_CONNECTION` | Account, user, key-pair, role, warehouse |

Connections are **UI-only** — there is no MCP tool and no public API endpoint
for creating them, so the agent will hand this step back to you. Wiring them to
environment variables afterwards *can* be done by the agent (`update_environment`),
using type `integration_credential` with the connection ID as the value.

The dbt Core connection needs a little more setup:

- **The dbt project must live in a Git repo** that Orchestra clones — it cannot
  read a local directory. Push [`dbt/`](dbt/) to a repo of your own (it works
  as-is at the repo root), and point the connection at that.
- **A `requirements.txt` (or `pyproject.toml`) at the repo root** is required —
  it's what Orchestra installs before running dbt. See
  [`dbt/requirements.txt`](dbt/requirements.txt).
- **The Git token only needs read access** to *Contents* and *Metadata*. A
  fine-grained PAT scoped to the single repo is enough.
- **`profiles.yml` is pasted into the connection**, not committed. Orchestra
  stores it encrypted. Its profile name must match `dbt_project.yml` (`agent_demo`).

## Step 4 — The two working in tandem

That's the whole point of the demo:

- **Estuary (real-time)** — the moment an order changes in Postgres, the change
  is in the Snowflake mirror. No schedule, no polling; latency is seconds.
- **Orchestra (scheduled batch)** — once a day, Orchestra confirms ingestion is
  healthy, rebuilds the dbt marts over that always-fresh data, and gates them
  behind quality checks before anyone downstream trusts them.

And when the batch layer breaks, you fix it the same way you built it:

> **"My Orchestra pipeline `daily_orders_analytics` failed — diagnose and fix
> it."**
> *(`fix-orchestra-pipeline` pulls the run's task logs over MCP, finds the root
> cause, applies a fix, and retries; `identify-pipeline-error` stops at the
> diagnosis if you'd rather approve the change yourself)*

Other skills in the Orchestra plugin worth knowing: `account-health-check`,
`configure-dbt-source-freshness`, `configure-dbt-build-after`,
`orchestra-dbt-slim-ci-setup`, and `build-data-reconciliation-pipeline`.

## Gotchas worth knowing

This demo was built and run end-to-end; these are the things that actually bit,
all already accounted for in the files here.

**Estuary**

- `source-postgres:v3` takes a nested `credentials: {auth_type, password}` block
  and a required `historyMode` — not a flat `password`.
- The Snowflake materialization needs `warehouse` unless the user has a default
  one, and `private_key` (not `privateKey`).
- **Connector configs are sops-encrypted with a MAC over the plaintext fields
  too.** You cannot pull an existing spec, tweak `schema`, and republish — it
  fails with `MAC mismatch`. Write a fresh config instead.
- The Snowflake connector creates *tables*, not *schemas*. Create the target
  schema first or the first commit fails with `Schema ... does not exist`.
- Changing a materialization's config doesn't take effect mid-transaction: a
  connector on a 30m sync will finish its current acknowledgement delay first.
  Disable then re-enable shards (plus bump `backfill`) to force it immediately.
- Keep `flowctl` current. v0.6.0 fails every publish against the current control
  plane with `unknown field createdAt`; v0.6.12 is fine.

**Orchestra**

- `DBT_CORE_EXECUTE.commands` is a **single string, and a single command**.
  Newlines are flattened, so `"dbt deps\ndbt build"` runs as
  `dbt deps dbt build …` and dies on `No such option '--select'`.
- You don't need `dbt deps` — Orchestra installs `packages.yml` itself first.
- `ESTUARY_CHECK_FLOW`'s latency metric tracks commit acknowledgement, not row
  freshness, and reads far higher than real lag (694–945s observed while
  Snowflake was ~9s behind Postgres). Give `latency_threshold` headroom.
- The first `ESTUARY_CHECK_FLOW` run has no baseline, so it reports the task's
  entire failure history as new. Judge from run two.
- Cron is six fields, **minute-first** (`0 6 ? * * *`), not seconds-first Quartz.
- Valid alert statuses are `ANY_COMPLETED, CANCELLED, FAILED, SUCCEEDED,
  WARNING, SKIPPED` — there's no `RUNNING_TIMEOUT`. Pipeline aliases allow only
  letters, numbers, and underscores.
- Validate with the `validate_pipeline` MCP tool before publishing; it catches
  every one of the schema issues above in seconds.

## Tear down

```bash
docker compose down -v            # stop Postgres + generator
```

Then disable or delete the Estuary capture/materialization (dashboard or
`flowctl catalog delete`), pause the Orchestra pipeline, and drop the
`AGENT_DEMO` database in Snowflake.

---

## Learn more

- Estuary agent skills — https://docs.estuary.dev/guides/agent-skills/
- Orchestra agent skills — https://docs.getorchestra.io/docs/guides/orchestra-skills
- Orchestra pipeline YAML schema — https://docs.getorchestra.io/docs/core-concepts/pipelines/schema
- Estuary docs — https://docs.estuary.dev/
- Orchestra docs — https://docs.getorchestra.io/
