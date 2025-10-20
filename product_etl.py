import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from urllib3.util import url

# google bigquery details that have been replaced for publication. the api url is real

project_id = 'dummied_project_id'
dataset_id = 'dummied_dataset_id'
table_id = 'dummied_table_id'
api_url = 'https://fakestoreapi.com/products'


# initialize bigquery client

try:
    bq_client = bigquery.Client(project=project_id)

except Exception as e:
    print(f"BigQuery client error: {e}")
    bq_client = None

# extract from fake store api

def extract_products_from_api(url: str) -> list[dict]:
    print(f"Extracting products from {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print(f"Extracted {len(data)} products from {url}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"API extraction error: {e}")
        return []

raw_data = extract_products_from_api(api_url)

# clean data

def transform_product_data(raw_data: list[dict]) -> pd.DataFrame:
    if not raw_data:
        print(f"No data found for {url}")
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)
    print(f"Transformed {len(df)} products from {url}")

  # flatten rating & count and rename count to rating_count for clarity
    ratings_df = df['rating'].apply(pd.Series)
    ratings_df.columns = ['rating', 'rating_count']

    df = pd.concat([df.drop('rating', axis=1), ratings_df], axis=1)

  # set data types to match target table
    df['id'] = df['id'].astype('Int64')
    df['price'] = df['price'].astype(float)
    df['rating'] = df['rating'].astype(float)
    df['rating_count'] = df['rating_count'].astype('Int64')
    df['insert_date_time'] = pd.to_datetime('now', utc=True)

    df_clean = df.dropna(subset=['id']).copy()

    print(f"Data transformed and flattened. Final row count: {len(df_clean)}")
    return df_clean

transformed_df = transform_product_data(raw_data)
print("\nTransformed products:")
print(transformed_df.dtypes.to_markdown())

# load to bigquery

def load_to_bigquery(df: pd.DataFrame, project: str, dataset: str, table: str, client: bigquery.Client):
    if df.empty or client is None:
        print("Load skipped: DataFrame is empty or BigQuery client failed to initialize.")
        return

    table = f"{project_id}.{dataset_id}.{table_id}"

    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    print(f"Loading data into {table}")

    job = client.load_table_from_dataframe(df, table, job_config=job_config)

    try:
        job.result()

        table_ref = client.get_table(table)
        print(f"Load complete. {table_ref.num_rows} rows loaded to {table}.")

    except Exception as e:
        print(f"Load failed. {e}")

load_to_bigquery(transformed_df, project_id, dataset_id, table_id, bq_client)
