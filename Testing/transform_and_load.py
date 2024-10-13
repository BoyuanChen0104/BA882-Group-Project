import os
import json
import tempfile
import pandas as pd
from google.cloud import storage
import duckdb  # Import DuckDB

# Set up your environment variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

def download_json_from_gcs(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    _, temp_file = tempfile.mkstemp()
    blob.download_to_filename(temp_file)
    print(f"Downloaded {blob.name} to temporary file.")
    return temp_file

def transform_data(aggregated_file):
    print("Transforming data...")
    with open(aggregated_file, 'r') as f:
        data = json.load(f)

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Extract date from 'publishedAtDate'
    df['publishedAtDate'] = pd.to_datetime(df['publishedAtDate']).dt.date.astype(str)

    # Handle categories
    df['subcategory1'] = df['categories'].apply(lambda x: x[0] if len(x) > 0 else None)
    df['subcategory2'] = df['categories'].apply(lambda x: x[1] if len(x) > 1 else None)
    df = df.drop(columns=['categories'])

    # Basic preprocessing of 'reviewText'
    def preprocess_text(text):
        if text:
            text = text.lower()
            text = text.strip()
            # Remove special characters, numbers, etc.
            text = ''.join(e for e in text if e.isalnum() or e.isspace())
            return text
        else:
            return ''

    df['reviewText'] = df['reviewText'].apply(preprocess_text)

    print("Data transformation complete.")
    return df

def save_df_to_csv(df, output_file):
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}.")

def upload_file_to_gcs(bucket_name, local_file_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_file_name, content_type='text/csv')
    print(f"Uploaded {local_file_name} to {blob.name}")

def load_data_into_motherduck(csv_file):
    print("Loading data into MotherDuck...")

    # Connect to MotherDuck using DuckDB connection string
    con = duckdb.connect(database='md:?motherduck_token={}'.format(MOTHERDUCK_TOKEN))

    # Create or replace the table in MotherDuck and insert data
    con.execute("CREATE OR REPLACE TABLE reviews AS SELECT * FROM read_csv_auto(?);", (csv_file,))

    print("Data loaded into MotherDuck.")

def main():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blobs = list(bucket.list_blobs())

    # Identify the latest aggregated data file
    aggregated_blobs = []
    for blob in blobs:
        if blob.name.startswith('aggregated_data_') and blob.name.endswith('.json'):
            aggregated_blobs.append(blob)

    if not aggregated_blobs:
        print("No aggregated data file found.")
        return

    # Get the latest aggregated data file
    latest_blob = max(aggregated_blobs, key=lambda b: b.name)
    print(f"Processing latest aggregated data file: {latest_blob.name}")

    temp_aggregated_file = download_json_from_gcs(GCS_BUCKET_NAME, latest_blob.name)
    transformed_df = transform_data(temp_aggregated_file)
    os.remove(temp_aggregated_file)

    # Save transformed data to CSV
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv').name
    save_df_to_csv(transformed_df, temp_csv_file)

    # Optionally upload CSV to GCS (if you want to keep a copy)
    csv_blob_name = 'transformed_data.csv'
    upload_file_to_gcs(GCS_BUCKET_NAME, temp_csv_file, csv_blob_name)

    # Load data into MotherDuck
    load_data_into_motherduck(temp_csv_file)

    # Clean up
    os.remove(temp_csv_file)
    print("Transformation and loading process completed.")

if __name__ == "__main__":
    main()
