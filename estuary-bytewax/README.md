# How to consume data from Estuary Flow using Bytewax

## Setup

1. docker compose up
2. `curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'`
3. Set up MongoDB Capture in Estuary Flow
4. Start consuming with Dekaf.

Full how-to guide: 