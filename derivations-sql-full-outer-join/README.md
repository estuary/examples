# Example full outer join implement in SQL

## Setup

1. Start containers: `docker compose up`
2. Get PostgreSQL URL: `curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'`
3. Set up Estuary Flow capture
4. Deploy derivation via `flowctl`
5. ???
5. Profit!

Read the full article here: https://estuary.dev/derivations-join-collections-sql/
