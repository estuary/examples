import json
import os

import pandas as pd
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

table = catalog.load_table(f"{os.getenv('NAMESPACE')}.transactions")

df = table.scan().to_pandas()

print(df.describe())

print(df.head())

# Function to extract operation type from the flow_document JSON field
def get_operation(flow_document):
    try:
        doc = json.loads(flow_document)
        return doc["_meta"]["op"]
    except (json.JSONDecodeError, KeyError):
        return None

# Add a column for the operation (insert, update, delete)
df['operation'] = df['flow_document'].apply(get_operation)

# Filter updates and deletes
updates_df = df[df['operation'] == 'u']
deletes_df = df[df['operation'] == 'd']

# Filter insertions (operations that aren't 'u' or 'd') - assuming these are additions
insertions_df = df[~df['operation'].isin(['u', 'd'])]

# Remove deleted transactions from the original DataFrame (i.e., filter out 'd' operations)
df_remaining = df[~df['operation'].isin(['d'])]

# Merge updated transactions into the remaining DataFrame
merged_df = pd.concat([df_remaining, updates_df]).drop_duplicates(subset=['transaction_id'], keep='last')

# Now, 'merged_df' contains the latest state of the transactions table
print(merged_df.describe())

print(merged_df[['transaction_id', 'amount', 'user_id']].head())

# Group by user_id and aggregate total transaction amount per user
aggregated_df = df.groupby('user_id')['amount'].sum().reset_index()

# Rename the column to reflect the aggregation
aggregated_df.rename(columns={'amount': 'total_transaction_amount'}, inplace=True)

# Show the aggregated result
print(aggregated_df)