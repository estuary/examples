# Real-Time Snowflake CDC to Pinecone for RAG with Estuary

Stream change data capture (CDC) from Snowflake into a Pinecone vector database in real time with [Estuary](https://estuary.dev), then query it with a Streamlit Retrieval-Augmented Generation (RAG) chatbot. New and updated customer support tickets in Snowflake are captured, embedded into vectors, and materialized into Pinecone so the chat app always answers from fresh data — no batch reindexing.

## Architecture

The pipeline uses Estuary to move data from Snowflake to Pinecone, and a LlamaIndex + OpenAI Streamlit app to query it:

```
Snowflake (SUPPORT_REQUESTS)
        │  CDC capture (source-snowflake)
        ▼
Estuary collection  ──►  text embedding  ──►  materialization (materialize-pinecone)
        │                                              │
        │                                              ▼
        │                                   Pinecone (namespace: Support_Requests)
        │                                              ▲
        ▼                                              │
   datagen/ writes/updates/deletes rows        Streamlit RAG app (app.py / rag.py)
```

End to end, in Estuary terms:

1. A **capture** using the Snowflake source connector streams CDC events from the `SUPPORT_REQUESTS` table into an Estuary **collection** (a schematized, real-time data lake of JSON in cloud storage).
2. A **materialization** using the Pinecone connector embeds the collection's documents and writes the resulting vectors to a Pinecone index under the `Support_Requests` namespace.
3. The **Streamlit app** retrieves the most relevant vectors from Pinecone and feeds them to an OpenAI chat model to answer questions about the support tickets.

## What's included

- `docker-compose.yml` — spins up two services: `snowflake-cdc-datagen` (seeds and mutates the Snowflake table) and `snowflake-cdc-streamlit` (the RAG chat UI on port `8501`).
- `datagen/datagen.py` — connects to Snowflake, creates the `SUPPORT_REQUESTS` table if missing, and continuously inserts (70%), updates (10%), and deletes (20%) realistic support tickets. Descriptions are generated with OpenAI (`gpt-3.5-turbo-0125`) and Faker.
- `datagen/Dockerfile`, `datagen/requirements.txt` — container image and dependencies for the data generator.
- `app.py` — the Streamlit front end ("Chat with Snowflake"): session handling, chat history, prompt input, and source attribution.
- `rag.py` — wires up LlamaIndex: a `PineconeVectorStore` (namespace `Support_Requests`, text key `flow_document`), a top-5 retriever, an OpenAI `gpt-3.5-turbo` LLM, and a `CondensePlusContextChatEngine`.
- `Dockerfile` — builds the Streamlit container (`streamlit run app.py --server.port 8501`).
- `requirements.txt` — Streamlit app dependencies (Streamlit, LlamaIndex, the Pinecone vector store integration, python-dotenv).
- `.streamlit/config.toml` — enables static serving and the light theme.
- `estuary_logo.png` — branding shown in the chat UI.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose.
- A free [Estuary account](https://dashboard.estuary.dev).
- A **Snowflake** account with a warehouse, database, schema, role, and a user/password that can read and write the `SUPPORT_REQUESTS` table.
- A **GCP service account JSON key** for Snowflake-to-Estuary staging (mounted by the datagen service — see `docker-compose.yml`). Snowflake captures stage data through cloud storage; see the connector docs below.
- A **Pinecone** account with an index, plus its API key and index host URL.
- An **OpenAI** API key (used by both the data generator and the RAG app).

## Setup

### 1. Configure environment variables

Edit `docker-compose.yml` and fill in the empty values.

For the `datagen` service (Snowflake source):

```yaml
environment:
  SNOWFLAKE_ACCOUNT: "<your-account>"
  SNOWFLAKE_USER: "<your-user>"
  SNOWFLAKE_PASSWORD: "<your-password>"
  SNOWFLAKE_ROLE: "<your-role>"
  SNOWFLAKE_WAREHOUSE: "<your-warehouse>"
  SNOWFLAKE_DATABASE: "<your-database>"
  SNOWFLAKE_SCHEMA: "<your-schema>"
  SNOWFLAKE_TABLE: "SUPPORT_REQUESTS"
```

The datagen service also needs an `OPENAI_API_KEY` to generate ticket descriptions — add it under the `datagen` service `environment` block, or place it in a `.env` file that Compose loads. Update the volume mount to point at your GCP service-account credentials file:

```yaml
volumes:
  - /absolute/path/to/gcp-service-account-cred.json:/credentials.json
```

For the `streamlit` service (RAG app):

```yaml
environment:
  PINECONE_API_KEY: "<your-pinecone-api-key>"
  PINECONE_HOST: "<your-pinecone-index-host>"
  OPENAI_API_KEY: "<your-openai-api-key>"
```

### 2. Generate data in Snowflake

Start the data generator so the `SUPPORT_REQUESTS` table exists and begins filling with rows:

```bash
docker compose up -d datagen
```

Watch it work:

```bash
docker compose logs -f datagen
```

You should see `Inserted new support request`, `Updated support request`, and `Deleted support request` messages every couple of seconds. The table schema is:

| Column        | Type   |
| ------------- | ------ |
| REQUEST_ID    | INT    |
| CUSTOMER_ID   | INT    |
| REQUEST_DATE  | STRING |
| REQUEST_TYPE  | STRING |
| STATUS        | STRING |
| DESCRIPTION   | STRING |

## Configure the Estuary capture (Snowflake CDC)

Create the capture in the [Estuary dashboard](https://dashboard.estuary.dev/captures):

1. Click **New Capture** and choose the **Snowflake** source connector.
2. Enter your Snowflake connection details — the same `SNOWFLAKE_ACCOUNT`, user, password, role, warehouse, database, and schema you set in `docker-compose.yml`.
3. Select the `SUPPORT_REQUESTS` table to bind it to an Estuary collection.
4. Save and publish. Estuary begins streaming the table's change data into the collection.

Connector reference: [Snowflake source connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/snowflake/).

## Configure the Estuary materialization (Pinecone)

Create the materialization in the [Estuary dashboard](https://dashboard.estuary.dev/materializations):

1. Click **New Materialization** and choose the **Pinecone** destination connector.
2. Provide your Pinecone API key, index, and embedding configuration (the connector embeds documents before upserting vectors).
3. Bind the Snowflake collection from the capture above to the Pinecone index, using the namespace **`Support_Requests`** so it matches what `rag.py` queries.
4. Save and publish. Estuary embeds and upserts vectors into Pinecone in real time as rows change in Snowflake.

> The RAG app reads vectors from the `Support_Requests` namespace and uses `flow_document` as the text key (`rag.py`). Keep these aligned with the materialization config.

Connector reference: [Pinecone materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/pinecone/).

## Running the RAG app

Once data is flowing into Pinecone, start the Streamlit chat UI:

```bash
docker compose up -d streamlit
```

Open [http://localhost:8501](http://localhost:8501) and ask questions about the customer support tickets, for example:

- "What are customers complaining about most?"
- "Summarize the open billing issues."
- "Are there any authentication failures reported recently?"

The app retrieves the top 5 matching support tickets from Pinecone and answers with OpenAI `gpt-3.5-turbo`, citing the source documents it used.

To run everything at once:

```bash
docker compose up -d
```

## Verify

- **Snowflake**: query `SELECT COUNT(*) FROM SUPPORT_REQUESTS;` and confirm the count grows as datagen runs.
- **Estuary**: in the dashboard, check the capture and materialization task metrics for non-zero documents and bytes flowing. You can also tail the collection:

  ```bash
  flowctl collections read --collection <your/collection/name> --uncommitted | head
  ```

- **Pinecone**: confirm vectors appear in the index under the `Support_Requests` namespace.
- **App**: ask a question about a ticket you can see in Snowflake and verify the answer reflects it.

## Next steps

- Swap the OpenAI chat model in `rag.py` (`gpt-3.5-turbo`) for a different model.
- Point the same Snowflake collection at additional destinations (a warehouse, search index, etc.) via more materializations — no re-capture needed.
- Add a [derivation](https://docs.estuary.dev/concepts/derivations/) in SQL, TypeScript, or Python to clean or enrich tickets before they reach Pinecone.

## Resources

- [Estuary documentation](https://docs.estuary.dev)
- [Estuary dashboard](https://dashboard.estuary.dev)
- [Snowflake source connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/snowflake/)
- [Pinecone materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/pinecone/)
- [flowctl CLI](https://docs.estuary.dev/concepts/flowctl/)
