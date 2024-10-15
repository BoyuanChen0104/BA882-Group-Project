import os
import json
import tempfile
import re
from google.cloud import storage
from datetime import datetime
from prefect import task, flow, get_run_logger

# Set up your environment variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

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
def upload_json_to_gcs(bucket_name, local_file_name, blob_name):
    logger = get_run_logger()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_file_name, content_type='application/json')
    logger.info(f"Uploaded {local_file_name} to {blob.name}")

@task
def aggregate_data():
    logger = get_run_logger()
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blobs = list(bucket.list_blobs())

    # Identify the latest aggregated data file
    aggregated_blobs = []
    for blob in blobs:
        match = re.match(r'aggregated_data_(\d{4}-\d{2}-\d{2})\.json', blob.name)
        if match:
            date_str = match.group(1)
            date = datetime.strptime(date_str, '%Y-%m-%d')
            aggregated_blobs.append((date, blob))

    if aggregated_blobs:
        # Get the latest aggregated data file
        latest_aggregated_date, latest_aggregated_blob = max(aggregated_blobs, key=lambda x: x[0])
        logger.info(f"Found existing aggregated data file: {latest_aggregated_blob.name}")
        temp_aggregated_file = download_json_from_gcs.submit(GCS_BUCKET_NAME, latest_aggregated_blob.name).result()
        with open(temp_aggregated_file, 'r') as f:
            try:
                aggregated_data = json.load(f)
                logger.info(f"Loaded {len(aggregated_data)} existing aggregated reviews.")
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from aggregated file: {e}")
                aggregated_data = []
        os.remove(temp_aggregated_file)
    else:
        # No existing aggregated data, check for initial data file
        logger.info("No existing aggregated data file found.")
        initial_blobs = []
        for blob in blobs:
            match = re.match(r'reviews_before_(\d{4}-\d{2}-\d{2})\.json', blob.name)
            if match:
                date_str = match.group(1)
                date = datetime.strptime(date_str, '%Y-%m-%d')
                initial_blobs.append((date, blob))
        if initial_blobs:
            # Get the latest initial data file
            latest_initial_date, latest_initial_blob = max(initial_blobs, key=lambda x: x[0])
            logger.info(f"Found initial data file: {latest_initial_blob.name}")
            temp_aggregated_file = download_json_from_gcs.submit(GCS_BUCKET_NAME, latest_initial_blob.name).result()
            with open(temp_aggregated_file, 'r') as f:
                try:
                    aggregated_data = json.load(f)
                    logger.info(f"Loaded {len(aggregated_data)} initial reviews.")
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON from initial data file: {e}")
                    aggregated_data = []
            os.remove(temp_aggregated_file)
        else:
            logger.error("No initial data file found. Cannot proceed with aggregation.")
            return

    # Identify the latest weekly data file
    weekly_blobs = []
    for blob in blobs:
        match = re.match(r'reviews_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.json', blob.name)
        if match:
            start_date_str = match.group(1)
            end_date_str = match.group(2)
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            weekly_blobs.append((end_date, blob))

    if not weekly_blobs:
        logger.error("No weekly data files found.")
        return

    # Get the latest weekly data file
    latest_weekly_date, latest_weekly_blob = max(weekly_blobs, key=lambda x: x[0])
    logger.info(f"Processing latest weekly data file: {latest_weekly_blob.name}")

    temp_weekly_file = download_json_from_gcs.submit(GCS_BUCKET_NAME, latest_weekly_blob.name).result()
    with open(temp_weekly_file, 'r') as f:
        try:
            weekly_data = json.load(f)
            logger.info(f"Loaded {len(weekly_data)} new reviews from weekly data file.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from weekly data file: {e}")
            weekly_data = []
    os.remove(temp_weekly_file)

    if not weekly_data:
        logger.warning("No new data to aggregate.")
        return

    # Combine existing aggregated data with new weekly data
    combined_data = aggregated_data + weekly_data

    # Remove duplicates based on 'reviewerId' and 'publishedAtDate'
    unique_data = {}
    for item in combined_data:
        reviewer_id = item.get('reviewerId')
        published_at = item.get('publishedAtDate')
        if reviewer_id and published_at:
            unique_key = f"{reviewer_id}_{published_at}"
            unique_data[unique_key] = item
        else:
            unique_data[id(item)] = item  # Use object id as key to avoid duplicates

    aggregated_list = list(unique_data.values())
    logger.info(f"Aggregated {len(aggregated_list)} unique reviews after combining.")

    # Determine new aggregated data file name
    new_aggregated_date_str = latest_weekly_date.strftime('%Y-%m-%d')
    new_aggregated_blob_name = f'aggregated_data_{new_aggregated_date_str}.json'

    # Save aggregated data to a local file
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_aggregated_file:
        json.dump(aggregated_list, temp_aggregated_file)
        temp_aggregated_file_name = temp_aggregated_file.name

    # Upload aggregated data back to GCS
    upload_json_to_gcs.submit(GCS_BUCKET_NAME, temp_aggregated_file_name, new_aggregated_blob_name).result()
    os.remove(temp_aggregated_file_name)
    logger.info(f"New aggregated data file created: {new_aggregated_blob_name}")

@flow(name="Aggregation Flow")
def aggregation_flow():
    aggregate_data()

if __name__ == "__main__":
    aggregation_flow()
