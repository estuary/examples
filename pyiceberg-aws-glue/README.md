# Read Estuary Apache Iceberg Tables with PyIceberg and the AWS Glue Catalog

Query an Apache Iceberg table that Estuary materialized to Amazon S3, using [PyIceberg](https://py.iceberg.apache.org/) with the **AWS Glue Data Catalog**. This is the read/query side of an Estuary Iceberg materialization: Estuary streams your source data into an Iceberg table registered in Glue, and `main.py` loads that table and scans selected columns into a pandas DataFrame — no Spark, no Trino, no query engine to operate.

## How it works

Estuary captures source data into **collections** (a schematized, real-time data lake of JSON in cloud storage), then a **materialization** writes those collections to a destination. With the Amazon S3 Iceberg materialization, each collection lands as an **Apache Iceberg table** in an S3 bucket, with table metadata registered in the **AWS Glue Data Catalog** under a namespace (database). PyIceberg reads that catalog directly.

```
Source ──capture──► Estuary collection ──materialization──► Apache Iceberg table (S3)
                                                              │  metadata registered in
                                                              ▼
                                                       AWS Glue Data Catalog
                                                              │
                                                              ▼
                                              main.py (PyIceberg + Glue) ──► pandas DataFrame
```

`main.py`:
1. Builds a `GlueCatalog` named `catalog`, authenticating with `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` loaded from the environment.
2. Prints `catalog.list_namespaces()` and `catalog.list_tables(namespace=NAMESPACE)` so you can confirm the Glue catalog is reachable and the table exists.
3. Loads the table `{NAMESPACE}.support_requests` with `catalog.load_table(...)`.
4. Runs a `table.scan(...)` selecting the fields `customer_id`, `description`, `request_date`, `request_id`, `request_type`, and `status`, converts the result to a pandas DataFrame with `.to_pandas()`, and prints `df.describe()` and `df.head()`.

## What's included

- **`main.py`** — the reader. Constructs the Glue-backed PyIceberg catalog, loads the `support_requests` Iceberg table, scans the selected fields into pandas, and prints summary stats.
- **`requirements.txt`** — pinned dependencies: `pyiceberg==0.6.1` and `boto3==1.34.134`. `main.py` also calls `load_dotenv()`, so install `python-dotenv` as well (see Setup).
- **`.gitignore`** — ignores the local `.env` file that holds your AWS credentials and namespace.

## Prerequisites

- **Python 3.8+** and `pip`.
- A **free Estuary account** — sign up at [https://dashboard.estuary.dev](https://dashboard.estuary.dev).
- An existing **Amazon S3 Iceberg materialization** in Estuary that writes the `support_requests` table to S3 and registers it in the **AWS Glue Data Catalog** (see "Configure the Estuary materialization" below).
- **AWS credentials** (access key ID + secret access key) with permission to read the Glue catalog and the underlying S3 data — at minimum `glue:GetDatabase*`, `glue:GetTable*`, `glue:GetPartitions`, and `s3:GetObject` / `s3:ListBucket` on the materialization's bucket.
- The **AWS region** and the **Glue namespace (database)** that the materialization writes to.

## Setup

Install the dependencies:

```bash
pip install -r requirements.txt
pip install python-dotenv
```

Create a `.env` file in this directory (it is git-ignored) with your AWS credentials, region, and the Glue namespace used by the materialization:

```bash
# .env
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
NAMESPACE=your_glue_namespace
```

These map directly to the `GlueCatalog(...)` arguments and the `load_table` / `list_tables` calls in `main.py`. `NAMESPACE` must match the Glue database (namespace) that the Estuary materialization writes the `support_requests` table into.

## Running it

```bash
python main.py
```

Expected output (abbreviated):

```
[('your_glue_namespace',), ...]                      # list_namespaces()
[('your_glue_namespace', 'support_requests'), ...]   # list_tables(NAMESPACE)
       customer_id   request_id ...                  # df.describe()
...
   customer_id           description request_date  request_id request_type   status
0          ...                   ...          ...         ...          ...      ...   # df.head()
```

If `list_tables` does not show `support_requests`, double-check that `NAMESPACE` matches the materialization's Glue database and that the materialization has published data.

## Configure the Estuary materialization

This example only **reads** the Iceberg table. To produce it, set up an **Amazon S3 Iceberg** materialization in Estuary that points at the collection you want to expose as `support_requests`:

1. In the [Estuary dashboard](https://dashboard.estuary.dev/materializations), create a new materialization and select the **Amazon S3 Iceberg** connector (`ghcr.io/estuary/materialize-s3-iceberg:dev`).
2. Configure the destination:
   - **Catalog**: AWS Glue Data Catalog.
   - **AWS Region**: the same region you put in `AWS_REGION`.
   - **S3 bucket** and **prefix** for the Iceberg data and metadata.
   - **AWS access key / secret** with write access to the bucket and Glue.
   - **Namespace** (Glue database): the value you put in `NAMESPACE`.
3. Bind your source collection to a table named `support_requests` (the table this example loads). To read different columns, edit the `selected_fields` tuple in `main.py` to match your table's schema.
4. Publish the materialization and let it backfill, then run `python main.py` to query the result.

Connector reference: [Amazon S3 Iceberg materialization](https://docs.estuary.dev/reference/Connectors/materialization-connectors/amazon-s3-iceberg/).

> Don't have a pipeline yet? Create a **capture** first (for example PostgreSQL, MySQL, or MongoDB CDC) at [https://dashboard.estuary.dev/captures](https://dashboard.estuary.dev/captures) so there's a collection to materialize into Iceberg.

## Verify

- The `list_namespaces()` and `list_tables()` output in `main.py` confirms PyIceberg can reach the Glue catalog and that `support_requests` exists.
- A populated `df.head()` confirms the Iceberg data files in S3 are readable end-to-end.
- Cross-check the row counts and freshness against the materialization's metrics in the [Estuary dashboard](https://dashboard.estuary.dev), or read the source collection directly with [flowctl](https://docs.estuary.dev/concepts/flowctl/):

  ```bash
  flowctl collections read --collection <your/collection/name> --uncommitted | head
  ```

## Next steps

- Swap pandas for your engine of choice — PyIceberg can also return [PyArrow](https://py.iceberg.apache.org/api/) tables, or you can point DuckDB, Spark, Trino, or Athena at the same Glue catalog and S3 bucket.
- Adjust the `selected_fields` and add row-level filters with `table.scan(row_filter=...)` to push down predicates.
- Build the full real-time lakehouse: capture a source, optionally transform it with a [derivation](https://docs.estuary.dev/concepts/derivations/), and materialize it to Iceberg for query.

## References

- PyIceberg documentation: [https://py.iceberg.apache.org/](https://py.iceberg.apache.org/)
- Amazon S3 Iceberg materialization connector: [https://docs.estuary.dev/reference/Connectors/materialization-connectors/amazon-s3-iceberg/](https://docs.estuary.dev/reference/Connectors/materialization-connectors/amazon-s3-iceberg/)
- Estuary docs: [https://docs.estuary.dev](https://docs.estuary.dev)
- Estuary dashboard: [https://dashboard.estuary.dev](https://dashboard.estuary.dev)
