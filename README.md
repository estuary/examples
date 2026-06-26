# Estuary Examples

A collection of hands-on, runnable examples for building real-time data pipelines with [Estuary](https://estuary.dev). Each project is self-contained and covers a production-grade pattern: change data capture (CDC) from databases like PostgreSQL, MongoDB, Oracle, and SQL Server; streaming ETL and materializations into warehouses and lakehouses; SQL, TypeScript, and Python derivations; and real-time Retrieval-Augmented Generation (RAG) and AI pipelines. Clone any folder and stream live data in minutes.

## What is Estuary?

Estuary is a real-time data integration platform that captures change data capture (CDC) streams and event data from your databases, SaaS apps, and streams, then materializes them into warehouses, lakehouses, vector databases, and analytics tools with millisecond latency. Learn more at [estuary.dev](https://estuary.dev) and read the [documentation](https://docs.estuary.dev).

## Database CDC Captures

| Example | Description |
| --- | --- |
| [postgres-simple-capture](./postgres-simple-capture) | Minimal, self-contained PostgreSQL CDC demo using Docker and an ngrok tunnel to stream row changes into an Estuary collection. |
| [postgres-cloudsql-simple-capture](./postgres-cloudsql-simple-capture) | Real-time PostgreSQL CDC pipeline targeting Google Cloud SQL for PostgreSQL with the Cloud SQL Python Connector. |
| [oracle-capture](./oracle-capture) | Oracle CDC capture from a free, local Oracle Database 23.6 in Docker using LogMiner-based logical replication. |
| [sqlserver-cdc-capture](./sqlserver-cdc-capture) | Self-contained SQL Server 2022 environment with Change Data Capture enabled and a continuous insert/update/delete data generator. |
| [kafka-capture](./kafka-capture) | Capture real-time IoT topics from an Amazon MSK (Apache Kafka) cluster into Estuary using AWS IAM authentication. |
| [estuary-demo-movies](./estuary-demo-movies) | Seed a `movies` table in any ANSI-SQL database as a ready-to-capture source for an Estuary capture. |
| [shipments-datagen](./shipments-datagen) | Dockerized PostgreSQL data generator that continuously mutates realistic shipments data, pre-wired for Estuary CDC. |
| [postgres-measure-wal-throughput](./postgres-measure-wal-throughput) | Measure PostgreSQL WAL throughput to size and forecast a CDC pipeline's change-event volume before you build it. |

## Materializations & Destinations

| Example | Description |
| --- | --- |
| [estuary-motherduck-demo-2025](./estuary-motherduck-demo-2025) | Stream PostgreSQL CDC into MotherDuck in real time, keeping analytical tables up to date with low latency. |
| [estuary-motherduck-orders](./estuary-motherduck-orders) | Stream a live pet-store order feed from PostgreSQL into MotherDuck (serverless DuckDB) via CDC. |
| [postgres-cdc-bigquery-dbt](./postgres-cdc-bigquery-dbt) | End-to-end ELT pipeline streaming PostgreSQL CDC into Google BigQuery, then modeling it with dbt. |
| [postgresql-cdc-databricks-fraud-detection](./postgresql-cdc-databricks-fraud-detection) | Real-time fraud detection pipeline streaming PostgreSQL CDC into Databricks for SQL-based lakehouse analysis. |
| [singlestore-webinar-2025](./singlestore-webinar-2025) | Stream PostgreSQL CDC into SingleStore in real time for low-latency analytics (Estuary x SingleStore webinar demo). |
| [mongodb-tinybird-clickstream](./mongodb-tinybird-clickstream) | Capture a live e-commerce clickstream from MongoDB Atlas and materialize it into Tinybird. |
| [sqlserver-cdc-materialize](./sqlserver-cdc-materialize) | Stream SQL Server CDC into Materialize via the Dekaf Kafka-compatible API to power an incrementally maintained view. |
| [pyiceberg-aws-glue](./pyiceberg-aws-glue) | Query an Apache Iceberg table that Estuary materialized to S3 using PyIceberg and the AWS Glue Data Catalog. |

## Derivations & Transformations

| Example | Description |
| --- | --- |
| [derivations-ad-performance](./derivations-ad-performance) | Real-time ad performance analytics joining impression and click streams with a stateful TypeScript derivation. |
| [derivations-sql-full-outer-join](./derivations-sql-full-outer-join) | Implement a full outer join across two collections with a SQLite-backed Estuary SQL derivation. |
| [python-derivations](./python-derivations) | Four Python derivation patterns: stateless transforms, stateful aggregation, streaming joins, and ML feature engineering. |

## Real-Time RAG & AI

| Example | Description |
| --- | --- |
| [google-sheets-pinecone-rag](./google-sheets-pinecone-rag) | End-to-end real-time RAG: stream Google Sheets rows to Pinecone embeddings and serve a Streamlit chatbot. |
| [mongodb-pinecone-rag](./mongodb-pinecone-rag) | Stream MongoDB product reviews to a Pinecone vector index for a real-time RAG Streamlit chat app. |
| [snowflake-cdc-pinecone-rag](./snowflake-cdc-pinecone-rag) | Stream Snowflake CDC into Pinecone vectors in real time, queried by a Streamlit RAG chatbot. |

## Streaming, Lakehouse & Stream Processing

| Example | Description |
| --- | --- |
| [dekaf-kcat](./dekaf-kcat) | Consume a live Estuary collection from the CLI with kcat over Estuary's Kafka-compatible Dekaf API. |
| [dekaf-python](./dekaf-python) | Consume a real-time Estuary collection in Python with `confluent-kafka`, Dekaf, and an Avro schema registry. |
| [estuary-bytewax](./estuary-bytewax) | Stream MongoDB CDC into a Bytewax Python dataflow via Dekaf to compute tumbling-window metrics. |
| [streaming-lakehouse-iceberg-duckdb](./streaming-lakehouse-iceberg-duckdb) | Build a streaming lakehouse: PostgreSQL CDC into Apache Iceberg on S3 (AWS Glue), queried with PyIceberg/DuckDB. |
| [shipments_eta](./shipments_eta) | Real-time freight ETA tracking from MongoDB CDC to Tinybird/ClickHouse with a Next.js dashboard via Dekaf. |

## Demos, Workshops & Webinars

| Example | Description |
| --- | --- |
| [hands-on-lab-postgres-motherduck](./hands-on-lab-postgres-motherduck) | Guided hands-on lab: PostgreSQL CDC to MotherDuck with soft delete, hard delete, and SCD2 materialization patterns. |
| [estuary-coaelsce-demo-2025](./estuary-coaelsce-demo-2025) | Self-contained PostgreSQL CDC fraud-detection demo with anomaly injection (Estuary x Coalesce 2025). |

---

Built with [Estuary](https://estuary.dev). Read the [blog](https://estuary.dev/blog/), explore the [documentation](https://docs.estuary.dev), or get started in the [dashboard](https://dashboard.estuary.dev).
