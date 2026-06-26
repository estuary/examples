# Stream MongoDB to Pinecone for Real-Time RAG with Estuary

This example builds an end-to-end, real-time Retrieval-Augmented Generation (RAG) pipeline. It captures e-commerce product reviews from MongoDB with Estuary, materializes them as vector embeddings into a [Pinecone](https://www.pinecone.io/) index, and serves a [Streamlit](https://streamlit.io/) chat app that answers questions about the reviews using [LlamaIndex](https://www.llamaindex.ai/) and OpenAI. New documents written to MongoDB flow into Pinecone within seconds, so the chatbot always queries fresh data — no batch re-indexing jobs.

Full walkthrough: [Real-time RAG with Estuary and Pinecone](https://estuary.dev/real-time-rag-with-estuary-and-pinecone/)

## Architecture

```
MongoDB (ecommerce.reviews)
        │  change stream (CDC)
        ▼
Estuary capture  ──►  Estuary collection (schematized JSON)
                              │
                              ▼
                  Estuary materialization (Pinecone connector)
                  embeds each review's text, upserts vectors
                              │
                              ▼
                  Pinecone index (namespace: reviews)
                              ▲
                              │  similarity search (top_k=5)
                  Streamlit + LlamaIndex + OpenAI chat app
```

- **Capture** — the [source-mongodb](https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/) connector tails the MongoDB change stream for the `ecommerce.reviews` collection and writes each document into an Estuary **collection** (a real-time, schematized data lake in cloud storage).
- **Materialization** — the [materialize-pinecone](https://docs.estuary.dev/reference/Connectors/materialization-connectors/pinecone/) connector reads the collection, generates an embedding for each review (via OpenAI), and upserts the resulting vectors into a Pinecone index under the `reviews` namespace. The original document text is stored on the vector under the `flow_document` key.
- **Query** — the Streamlit app (`app.py` + `rag.py`) uses LlamaIndex to embed the user's question, retrieve the most similar reviews from Pinecone, and feed them to OpenAI's `gpt-3.5-turbo` as grounding context.

## What's included

| Path | Role |
| --- | --- |
| `docker-compose.yml` | Defines two services: `datagen` (container `mongodb-datagen`) seeds MongoDB, and `streamlit` (container `streamlit`) runs the RAG chat app on port `8501`. |
| `datagen/datagen.py` | Reads every CSV in `datagen/data/` and inserts each row into the MongoDB `ecommerce.reviews` collection. |
| `datagen/data/` | Five Amazon product-review CSVs (`amazon_books_Data.csv`, `amazon_ebook_Data.csv`, `amazon_grocery_Data.csv`, `amazon_jwellery_Data.csv`, `amazon_pc_Data.csv`). |
| `datagen/Dockerfile` | Builds the loader image (Python 3.11 + `pymongo`). |
| `app.py` | Streamlit UI: chat interface that queries the RAG engine and renders responses. |
| `rag.py` | Wires up the Pinecone vector store (namespace `reviews`, text key `flow_document`), a `top_k=5` retriever, and a `CondensePlusContextChatEngine` backed by OpenAI `gpt-3.5-turbo`. |
| `Dockerfile` | Builds the Streamlit app image (Python 3.11) and serves it on port `8501`. |
| `requirements.txt` | App dependencies: `streamlit`, `llama-index`, `llama-index-vector-stores-pinecone`, `python-dotenv`. |
| `.streamlit/config.toml` | Streamlit theme + static file serving config. |

### Review document schema

Each row loaded from the CSVs becomes a MongoDB document with these fields:

```
market_place, customer_id, review_id, product_id, product_parent,
product_title, product_category, star_rating, helpful_votes,
total_votes, vine, verified_purchase, review_headline, review_body, review_date
```

## Prerequisites

- **Docker** and Docker Compose.
- A running **MongoDB** instance that Estuary can reach. The simplest path is a free [MongoDB Atlas](https://www.mongodb.com/atlas) cluster (Atlas exposes a public endpoint and meets the change-stream / replica-set requirement of CDC out of the box). The datagen service defaults to `mongo:mongo@localhost:27017` — point it at your own instance via the environment variables below.
- A free **Estuary** account: [https://dashboard.estuary.dev](https://dashboard.estuary.dev).
- A **Pinecone** account with an API key and an index (cosine metric; dimension must match the embedding model you choose in the connector).
- An **OpenAI** API key (used by the Pinecone materialization to create embeddings, and by the Streamlit app to generate chat responses).

> **MongoDB CDC requirements:** the source must be a replica set (or Atlas) so the connector can read change streams, and the capture user needs read access to the target database. See the [source-mongodb prerequisites](https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/).

## Setup

### 1. Configure connection values

Edit the `streamlit` service environment in `docker-compose.yml` with your real keys:

```yaml
  streamlit:
    environment:
      PINECONE_API_KEY: "<pinecone-api-key>"
      PINECONE_HOST: "<pinecone-host>"      # e.g. https://my-index-xxxx.svc.us-east-1-aws.pinecone.io
      OPENAI_API_KEY: "<openai-api-key>"
```

If you are not using the default local MongoDB, point the `datagen` service at your cluster:

```yaml
  datagen:
    environment:
      MONGODB_HOST: "<your-host>"
      MONGODB_PORT: "27017"
      MONGODB_USER: "mongo"
      MONGODB_PASSWORD: "mongo"
      MONGODB_DB: "ecommerce"
      MONGODB_COLLECTION: "reviews"
```

> The bundled `datagen.py` builds the URI as `mongodb://USER:PASSWORD@HOST:PORT/`. For MongoDB Atlas (which requires the `mongodb+srv://` scheme and TLS), seed your cluster directly with `mongoimport`/`mongosh` or adapt the connection string in `datagen/datagen.py`.

### 2. Seed MongoDB

```bash
docker compose up --build datagen
```

This loads all five Amazon review CSVs into the `ecommerce.reviews` collection. Confirm the row count, e.g.:

```bash
# with mongosh against your instance
mongosh "mongodb://mongo:mongo@localhost:27017/" --eval 'db.getSiblingDB("ecommerce").reviews.countDocuments()'
```

## Configure the Estuary capture

Use the Estuary dashboard to create the MongoDB source.

1. Go to [https://dashboard.estuary.dev/captures](https://dashboard.estuary.dev/captures) → **New Capture** → search for **MongoDB**.
2. Enter the connection details for your instance:
   - **Address / Host:** your MongoDB host (Atlas connection string host, or your public endpoint)
   - **User:** `mongo` (or your DB user)
   - **Password:** `mongo` (or your DB password)
   - **Database:** `ecommerce`
3. In the discovery step, select the `reviews` collection to bind.
4. Save and publish. Estuary backfills existing reviews and then tails the change stream, writing each document into an Estuary collection (e.g. `your-prefix/reviews`).

Connector reference: [MongoDB capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/).

## Configure the Estuary materialization

Materialize the collection into Pinecone as embeddings.

1. Go to [https://dashboard.estuary.dev/materializations](https://dashboard.estuary.dev/materializations) → **New Materialization** → search for **Pinecone**.
2. Provide:
   - **Pinecone API key** — same value as `PINECONE_API_KEY`
   - **Pinecone index** — your index name
   - **OpenAI API key** — used by the connector to embed each document
   - **Namespace:** `reviews` (must match the namespace `rag.py` queries)
3. Bind the `reviews` collection from your capture.
4. Save and publish. Estuary now upserts a vector per review into Pinecone, storing the source text under the `flow_document` key.

Connector reference: [Pinecone materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/pinecone/).

> **Why `flow_document`?** The Pinecone connector stores the full source document in vector metadata under `flow_document`. `rag.py` configures the LlamaIndex `PineconeVectorStore` with `text_key="flow_document"` and `namespace="reviews"` so retrieval reads exactly what Estuary wrote.

## Running the RAG app

With the capture and materialization live and vectors landing in Pinecone, start the chat app:

```bash
docker compose up --build streamlit
```

Open [http://localhost:8501](http://localhost:8501) and ask questions about the products, for example:

- "What do reviewers say about laptop sleeves?"
- "Are there any complaints about gluten-free cookie mixes?"
- "Which jewelry products got good reviews for the price?"

The app embeds your question, retrieves the five most similar reviews from Pinecone, and answers with `gpt-3.5-turbo` grounded in that context.

To run everything at once:

```bash
docker compose up --build
```

## Verify the pipeline is real-time

1. Insert a new review into MongoDB:
   ```bash
   mongosh "mongodb://mongo:mongo@localhost:27017/" --eval \
     'db.getSiblingDB("ecommerce").reviews.insertOne({product_title:"Test Widget", review_body:"This widget is amazing and very durable.", star_rating:"5"})'
   ```
2. Watch the capture and materialization metrics update in the [Estuary dashboard](https://dashboard.estuary.dev) (docs read/written counts increase).
3. Optionally read the collection directly with flowctl:
   ```bash
   flowctl collections read --collection your-prefix/reviews --uncommitted | head
   ```
4. Ask the chatbot about the new product — the answer reflects the just-inserted review.

## Next steps

- Swap the source for any of Estuary's [148+ connectors](https://docs.estuary.dev/reference/Connectors/) to power RAG over Postgres, Kafka, S3, or SaaS data.
- Add a [derivation](https://docs.estuary.dev/concepts/derivations/) (SQL, TypeScript, or Python) to clean, chunk, or enrich review text before it is embedded.
- Change the retrieval depth (`similarity_top_k` in `rag.py`) or the LLM (`OpenAI(model=...)`) to tune answer quality.

## References

- Blog: [Real-time RAG with Estuary and Pinecone](https://estuary.dev/real-time-rag-with-estuary-and-pinecone/)
- [Estuary documentation](https://docs.estuary.dev)
- [MongoDB capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/mongodb/)
- [Pinecone materialization connector](https://docs.estuary.dev/reference/Connectors/materialization-connectors/pinecone/)
- [flowctl CLI](https://docs.estuary.dev/concepts/flowctl/)
