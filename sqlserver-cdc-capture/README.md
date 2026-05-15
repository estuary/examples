# SQL Server CDC Capture Demo

A self-contained SQL Server environment with CDC enabled and a data generator
producing continuous inserts, updates, and deletes against a `sales` table.
Useful for demoing or testing an Estuary SQL Server CDC capture end-to-end.

## What's included

- `sqlserver/` — SQL Server 2022 with an init script that enables CDC, creates
  the `sales` table, sets up the `flow_capture` user, and creates the
  `flow_watermarks` table required by the Estuary connector.
- `datagen/` — Python container that inserts, updates, and deletes rows in
  `dbo.sales` once per second.
- `ngrok` — exposes SQL Server publicly over TCP so the hosted Estuary
  connector can reach it.
- `flow.yaml` — Estuary capture spec wiring the SQL Server source to a
  collection in your tenant.

## Running it

Set your ngrok token and start the stack:

```bash
export NGROK_AUTHTOKEN=<your-token>
docker compose up -d
```

Grab the public host/port from the ngrok dashboard at http://localhost:4040.

## Creating the Estuary capture

`flow.yaml` is the spec deployed via [flowctl](https://docs.estuary.dev/concepts/flowctl/).
It defines a SQL Server CDC capture and one binding for the `dbo.sales` table:

```yaml
captures:
  dani-demo/sqlserver-cdc/source-sqlserver:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sqlserver:v0
        config:
          address: <ngrok-host>:<ngrok-port>
          database: SampleDB
          user: flow_capture
          password: Secretsecret1
          historyMode: false
    bindings:
    - resource:
        namespace: dbo
        stream: sales
      target: dani-demo/sqlserver-cdc/dbo/sales
```

Before publishing, edit `address` to match the host/port from the ngrok
dashboard, and replace the `dani-demo/sqlserver-cdc/...` prefix with your own
tenant/prefix.

Then discover, publish, and verify:

```bash
# (Re-)run discovery to refresh bindings from the source — optional once
# bindings are present.
flowctl raw discover --source flow.yaml

# Publish the capture and the generated collection.
flowctl catalog publish --source flow.yaml --auto-approve

# Check status; first transition should be PENDING → BACKFILLING → OK.
flowctl catalog status dani-demo/sqlserver-cdc/source-sqlserver

# Peek at a few documents flowing through the collection.
flowctl collections read \
  --collection dani-demo/sqlserver-cdc/dbo/sales \
  --uncommitted | head
```

### Credentials

The `init.sql` script provisions a dedicated CDC user the capture connects as:

- user: `flow_capture`
- password: `Secretsecret1`
- database: `SampleDB`

It also grants the permissions the connector needs (`SELECT` on `dbo` and `cdc`,
plus `VIEW DATABASE STATE`) and enables CDC on `dbo.sales` and the
`dbo.flow_watermarks` table.
