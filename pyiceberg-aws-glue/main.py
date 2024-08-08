import os

from pyiceberg.catalog.glue import GlueCatalog

from dotenv import load_dotenv

load_dotenv()

catalog = GlueCatalog(
    name="catalog",
    **{
        "region_name": os.getenv("AWS_REGION"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }
)

print(catalog.list_namespaces())

print(catalog.list_tables(namespace=os.getenv("NAMESPACE")))

table = catalog.load_table(f"{os.getenv('NAMESPACE')}.support_requests")

df = table.scan(
    selected_fields=(
        "customer_id",
        "description",
        "request_date",
        "request_id",
        "request_type",
        "status",
    ),
).to_pandas()

print(df.describe())

print(df.head())
