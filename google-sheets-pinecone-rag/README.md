# Real-Time RAG: Stream Google Sheets to Pinecone for a Streamlit Chatbot with Estuary

This example builds an end-to-end, real-time Retrieval-Augmented Generation (RAG) pipeline. Estuary captures rows from a **Google Sheet** of customer support tickets, generates **OpenAI embeddings**, and materializes the vectors into a **Pinecone** index. A **Streamlit** chat app (LlamaIndex + OpenAI) then answers natural-language questions over the always-current support data — new, updated, and deleted rows in the sheet propagate to Pinecone continuously, so the chatbot's knowledge base stays fresh without batch reloads.

Reference walkthrough: https://estuary.dev/google-sheets-to-pinecone-rag/

## Architecture

The data flow is:

```
Google Sheet ──capture──▶ Estuary collection ──materialize (embed)──▶ Pinecone index ──retrieve──▶ Streamlit RAG chat (LlamaIndex + OpenAI)
("Fake Customer Support" /
 "Support Requests")
```

- **Capture (source):** The Estuary **Google Sheets** source connector polls the worksheet and streams every insert/update/delete into an Estuary **collection** — a schematized, real-time copy of the sheet backed by cloud storage.
- **Materialization (destination):** The Estuary **Pinecone** materialization connector takes each document from the collection, calls **OpenAI** to produce an embedding, and upserts the vector (with the original row text) into a Pinecone index. In this example the vectors land in the `Support_Requests` namespace, and the raw row text is stored under the `flow_document` key.
- **Retrieval / app:** `rag.py` wires LlamaIndex's `PineconeVectorStore` to that namespace and `flow_document` text key, retrieves the top-5 most similar tickets, and feeds them to an OpenAI chat model. `app.py` is the Streamlit front end.

Because Estuary is a streaming CDC/ETL platform, the loop is continuous: edit the sheet, and within the connector's polling interval the change is embedded and searchable in Pinecone.

## What's included

- **`docker-compose.yml`** — Spins up two services: `datagen` (container `gsheet-ai-datagen`, generates fake support tickets into the Google Sheet) and `streamlit` (container `streamlit`, serves the RAG chat UI on port `8501`).
- **`datagen/`** — Synthetic data generator.
  - `datagen.py` — Uses `pygsheets` to authenticate to Google Sheets via a service-account JSON, and OpenAI (`gpt-3.5-turbo-0125`) to write realistic customer-support ticket descriptions. Loops continuously, weighting `insert`/`update`/`delete` operations 70/10/20 so the sheet changes constantly (exercising CDC). Columns: `request_id`, `customer_id`, `request_date`, `request_type`, `status`, `description`.
  - `Dockerfile` — `python:3.12` image that runs `datagen.py`.
  - `requirements.txt` — `Faker`, `pygsheets`, `python-dotenv`, `openai`.
- **`app.py`** — Streamlit app titled "Real-time RAG with Estuary"; renders the chat interface ("Chat with Google Sheets") and streams responses from the LlamaIndex chat engine.
- **`rag.py`** — Builds the LlamaIndex retriever and chat engine: connects to Pinecone (`PINECONE_API_KEY`, `PINECONE_HOST`), opens the `Support_Requests` namespace with `text_key="flow_document"`, retrieves `similarity_top_k=5`, and answers with OpenAI `gpt-3.5-turbo`.
- **`Dockerfile`** — `python:3.11` image that runs `streamlit run app.py` on port `8501`.
- **`requirements.txt`** — `streamlit`, `llama-index`, `llama-index-vector-stores-pinecone`, `python-dotenv`.
- **`.streamlit/config.toml`** — Streamlit config (static serving enabled, light theme).
- **`estuary_logo.png`** — Logo shown in the app.

> Note: the Estuary capture and materialization in this example are configured in the **Estuary dashboard**, not committed as a `flow.yaml` here. The sections below walk through that wiring.

## Prerequisites

- **Docker** and Docker Compose.
- A **Google Cloud service account** with a JSON key, and the **Google Sheets API** and **Google Drive API** enabled. Share the target Google Sheet with the service-account email.
- A **Google Sheet** named `Fake Customer Support` with a worksheet (tab) named `Support Requests`. Add a header row: `request_id`, `customer_id`, `request_date`, `request_type`, `status`, `description`.
- An **OpenAI API key** (used both by the data generator and by the RAG chat app).
- A **Pinecone account**, API key, and an index host URL (`PINECONE_HOST`). Use an embedding dimension that matches the OpenAI embedding model configured in the Estuary materialization (e.g. `text-embedding-3-small` → 1536 dimensions).
- A free **Estuary account**: https://dashboard.estuary.dev

## Setup

### 1. Configure the data generator and app

Edit `docker-compose.yml` and fill in the values marked `# edit`:

```yaml
services:
  datagen:
    environment:
      SHEET_NAME: "Fake Customer Support"
      WORKSHEET_NAME: "Support Requests"
    volumes:
      - /path-to-gcp-service-account-cred.json:/credentials.json  # edit -> point at your service-account JSON

  streamlit:
    ports:
      - 8501:8501
    environment:
      PINECONE_API_KEY: ""  # edit
      PINECONE_HOST: ""     # edit -> your Pinecone index host URL
      OPENAI_API_KEY: ""    # edit
```

The `datagen` service also needs an `OPENAI_API_KEY` to write ticket descriptions; add it to the `datagen` service's `environment:` (or pass it via a `.env` file picked up by `python-dotenv`).

### 2. Start the data generator (and later the app)

You can start the generator on its own first so the sheet begins filling up before you wire up Estuary:

```bash
docker compose up --build datagen
```

`datagen` connects to the Google Sheet and continuously inserts/updates/deletes support requests every ~2 seconds. You should see log lines like `Inserted new support request: [...]`.

Once Pinecone is populated by the Estuary materialization (next sections), bring up the chat app:

```bash
docker compose up --build streamlit
```

The Streamlit UI is then available at http://localhost:8501.

## Configure the Estuary capture (Google Sheets → collection)

1. Open the Estuary dashboard and create a new capture: https://dashboard.estuary.dev/captures
2. Choose the **Google Sheets** source connector.
3. Authenticate to Google (OAuth) or supply the service-account credentials, then point the connector at the spreadsheet URL of your `Fake Customer Support` sheet.
4. Save and publish. Estuary discovers the `Support Requests` worksheet and creates a collection that streams every row change from the sheet.

Connector reference: https://docs.estuary.dev/reference/Connectors/capture-connectors/google-sheets/

## Configure the Estuary materialization (collection → Pinecone)

1. Create a new materialization: https://dashboard.estuary.dev/materializations
2. Choose the **Pinecone** materialization connector.
3. Provide:
   - **Pinecone API key** — same key you put in `PINECONE_API_KEY`.
   - **Pinecone index** / host — the index whose host you put in `PINECONE_HOST`.
   - **OpenAI API key** — the Pinecone connector calls OpenAI to embed each document before upserting it.
   - **Namespace** — `Support_Requests` (this is what `rag.py` reads from; keep them in sync).
4. Bind the Google Sheets collection from the capture above to the Pinecone index and publish.

The connector embeds each collection document with OpenAI and upserts the vector into Pinecone, storing the source text under the `flow_document` metadata key that `rag.py` uses as its `text_key`.

Connector reference: https://docs.estuary.dev/reference/Connectors/materialization-connectors/pinecone/

> Keep the **namespace** (`Support_Requests`), the **embedding model/dimension**, and the **`flow_document` text key** consistent between the Estuary materialization and `rag.py`, or retrieval will return nothing.

## Verify

- **In Estuary:** open the capture and materialization in the dashboard and watch the docs/bytes counters increase as `datagen` mutates the sheet. You can also read the collection directly with flowctl:
  ```bash
  flowctl collections read --collection <your/collection/name> --uncommitted | head
  ```
- **In Pinecone:** check the index's vector count in the Pinecone console under the `Support_Requests` namespace; it should grow as rows are added.
- **In the app:** open http://localhost:8501 and ask, for example, "Show me open billing issues" or "What authentication problems have customers reported?" The chatbot answers from the retrieved support tickets. Insert a new row via `datagen` (or edit the sheet manually) and confirm a follow-up question reflects it.

## Next steps

- Swap the synthetic `datagen` source for a real Google Sheet your team already maintains — the pipeline works unchanged.
- Point the same Estuary collection at additional destinations (a warehouse, another vector store) to reuse the captured data without re-reading the source.
- Tune retrieval (`similarity_top_k`), the embedding model, or the OpenAI chat model in `rag.py` for your use case.

## Resources

- Full walkthrough: https://estuary.dev/google-sheets-to-pinecone-rag/
- Estuary docs: https://docs.estuary.dev
- Google Sheets capture connector: https://docs.estuary.dev/reference/Connectors/capture-connectors/google-sheets/
- Pinecone materialization connector: https://docs.estuary.dev/reference/Connectors/materialization-connectors/pinecone/
- Estuary dashboard: https://dashboard.estuary.dev
