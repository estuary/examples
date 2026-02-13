# How to consume data from Estuary using Bytewax

## Setup

1. docker compose up
2. `curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'`
3. Set up MongoDB Capture in Estuary
4. Start consuming with Dekaf.

Full how-to guide: https://bytewax.io/blog/estuary-flow-mongodb-bytewax-real-time-data
