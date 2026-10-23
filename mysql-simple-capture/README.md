# Demo for a simple MySQL capture

## Setup

1. Start containers: `NGROK_AUTHTOKEN=<your-token> docker compose up`
2. Get MySQL URL: `curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url'`
3. Set up Estuary capture
   - Server Address: the ngrok URL from step 2, without the `tcp://` prefix
   - User: `flow_capture`
   - Password: `password`
