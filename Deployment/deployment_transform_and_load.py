import os
import json
import tempfile
import pandas as pd
from google.cloud import storage
import duckdb  # Import DuckDB
from prefect import task, flow, get_run_logger

# Set up environment variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

@task
def download_json_from_gcs(bucket_name, blob_name):
    logger = get_run_logger()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    blob.download_to_filename(temp_file.name)
    logger.info(f"Downloaded {blob.name} to temporary file.")
    return temp_file.name

@task
def transform_data(aggregated_file):
    logger = get_run_logger()
    logger.info("Transforming data...")
    with open(aggregated_file, 'r') as f:
        data = json.load(f)

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Extract date from 'publishedAtDate'
    df['publishedAtDate'] = pd.to_datetime(df['publishedAtDate']).dt.date.astype(str)

    # Handle categories
    df['subcategory1'] = df['categoryName']
    df['subcategory2'] = df['categories'].apply(lambda x: x[1] if len(x) > 1 else None)
    df = df.drop(columns=['categories', 'categoryName'])

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

    # --- Begin new transformations ---

    # 1. Split 'reviewDetailedRating' into separate columns
    # Ensure entries are dictionaries or empty dictionaries
    df['reviewDetailedRating'] = df['reviewDetailedRating'].apply(lambda x: x if isinstance(x, dict) else {})
    rating_df = df['reviewDetailedRating'].apply(pd.Series)
    rating_df = rating_df.rename(columns={
        'Food': 'food_rating',
        'Service': 'service_rating',
        'Atmosphere': 'atmosphere_rating'
    })
    df = pd.concat([df, rating_df], axis=1)
    df = df.drop(columns=['reviewDetailedRating'])

    # 2. Separate 'location' into 'latitude' and 'longitude'
    # Ensure entries are dictionaries or empty dictionaries
    df['location'] = df['location'].apply(lambda x: x if isinstance(x, dict) else {})
    location_df = df['location'].apply(pd.Series)
    location_df = location_df.rename(columns={
        'lat': 'latitude',
        'lng': 'longitude'
    })
    df = pd.concat([df, location_df], axis=1)
    df = df.drop(columns=['location'])

    # 3. Separate 'reviewContext' into 'Service_type', 'Meal_type', 'Price_per_person'
    # Ensure entries are dictionaries or empty dictionaries
    df['reviewContext'] = df['reviewContext'].apply(lambda x: x if isinstance(x, dict) else {})
    context_df = df['reviewContext'].apply(pd.Series)
    df['Service_type'] = context_df.get('Service')
    df['Meal_type'] = context_df.get('Meal type')
    df['Price_per_person'] = context_df.get('Price per person')
    df = df.drop(columns=['reviewContext'])

    # --- End new transformations ---

    logger.info("Data transformation complete.")
    return df

@task
def save_df_to_csv(df):
    logger = get_run_logger()
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv').name
    df.to_csv(temp_csv_file, index=False)
    logger.info(f"Data saved to {temp_csv_file}.")
    return temp_csv_file

@task
def upload_file_to_gcs(bucket_name, local_file_name, blob_name):
    logger = get_run_logger()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_file_name, content_type='text/csv')
    logger.info(f"Uploaded {local_file_name} to {blob.name}")

@task
def load_data_into_motherduck(csv_file):
    logger = get_run_logger()
    logger.info("Loading data into MotherDuck...")

    # Connect to MotherDuck using DuckDB connection string
    con = duckdb.connect(database='md:?motherduck_token={}'.format(MOTHERDUCK_TOKEN))

    # Create or replace the table in MotherDuck and insert data
    # The table name is called 'review'
    con.execute("CREATE OR REPLACE TABLE reviews AS SELECT * FROM read_csv_auto(?);", (csv_file,))

    logger.info("Data loaded into MotherDuck.")

@flow(name="Transform and Load Flow")
def transform_and_load_flow():
    logger = get_run_logger()
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blobs = list(bucket.list_blobs())

    # Identify the latest aggregated data file
    aggregated_blobs = []
    for blob in blobs:
        if blob.name.startswith('aggregated_data_') and blob.name.endswith('.json'):
            aggregated_blobs.append(blob)

    if not aggregated_blobs:
        logger.error("No aggregated data file found.")
        return

    # Get the latest aggregated data file
    latest_blob = max(aggregated_blobs, key=lambda b: b.name)
    logger.info(f"Processing latest aggregated data file: {latest_blob.name}")

    temp_aggregated_file = download_json_from_gcs(GCS_BUCKET_NAME, latest_blob.name)
    transformed_df = transform_data(temp_aggregated_file)
    os.remove(temp_aggregated_file)

    # Save transformed data to CSV
    temp_csv_file = save_df_to_csv(transformed_df)

    # Optionally upload CSV to GCS (for back-up)
    csv_blob_name = 'transformed_data.csv'
    upload_file_to_gcs(GCS_BUCKET_NAME, temp_csv_file, csv_blob_name)

    # Load data into MotherDuck
    load_data_into_motherduck(temp_csv_file)

    # Clean up
    os.remove(temp_csv_file)
    logger.info("Transformation and loading process completed.")

if __name__ == "__main__":
    transform_and_load_flow()
