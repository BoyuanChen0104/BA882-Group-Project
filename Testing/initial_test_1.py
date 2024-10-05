import os
import json
import datetime
from prefect import task, flow, get_run_logger
from apify_client import ApifyClient
from google.cloud import storage

# Retrieve environment variables
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

@task
def fetch_all_reviews_before(end_date_str):
    logger = get_run_logger()
    client = ApifyClient(APIFY_API_TOKEN)
    
    # Convert end_date_str to datetime object
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # Set a very old start date to fetch all reviews
    reviews_start_date = "2000-01-01"
    
    # Define your start URLs with complete URLs
    start_urls = [
        {"url": "https://www.google.com/maps/place/Jumbo+Seafood/@42.350931,-71.0627748,17z/data=!4m8!3m7!1s0x89e37a7847ac82ad:0x59ee82a6474ad485!8m2!3d42.350931!4d-71.0601999!9m1!1b1!16s%2Fg%2F1tjt2z0n?hl=en-GB&entry=ttu&g_ep=EgoyMDI0MDkxOC4xIKXMDSoASAFQAw%3D%3D"},
        {"url": "https://www.google.com/maps/place/Rowayton+Seafood/@41.0640248,-74.4434072,9z/data=!4m11!1m3!2m2!1sseafood+restaurant+near+New+England!6e5!3m6!1s0x89e81fc9005d651d:0x197740d3504cf794!8m2!3d41.0640248!4d-73.4443415!15sCiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBOZXcgRW5nbGFuZFolIiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBuZXcgZW5nbGFuZJIBEnNlYWZvb2RfcmVzdGF1cmFudOABAA!16s%2Fg%2F1thvtqxf?authuser=0&entry=ttu&g_ep=EgoyMDI0MDkyMy4wIKXMDSoASAFQAw%3D%3D"},
        {"url": "https://www.google.com/maps/place/Abe+%26+Louie's/@42.349138,-72.2021125,9z/data=!4m11!1m3!2m2!1sseafood+restaurant+near+New+England!6e5!3m6!1s0x89e37a0ef7c51c4d:0x3b643d1ee9cd8345!8m2!3d42.349138!4d-71.081507!15sCiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBOZXcgRW5nbGFuZFolIiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBuZXcgZW5nbGFuZJIBC3N0ZWFrX2hvdXNl4AEA!16s%2Fg%2F1tl1pg4w?authuser=0&entry=ttu&g_ep=EgoyMDI0MDkyMy4wIKXMDSoASAFQAw%3D%3D"}
    ]
    
    run_input = {
        "language": "en",
        "maxReviews": 10,  # Increased to fetch more reviews
        "personalData": True,
        "reviewsStartDate": reviews_start_date,
        "startUrls": start_urls
    }
    
    # Run the Apify actor
    try:
        run = client.actor("Xb8osYTtOjlsgI6k9").call(run_input=run_input)
    except Exception as e:
        logger.error(f"Failed to run Apify actor: {e}")
        raise
    
    # Retrieve data from the dataset
    try:
        all_data = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        logger.info(f"Fetched {len(all_data)} reviews before filtering.")
        if all_data:
            logger.info(f"Sample review data: {all_data[0]}")
    except Exception as e:
        logger.error(f"Failed to retrieve data from Apify dataset: {e}")
        raise
    
    # Filter reviews before the end_date
    filtered_data = []
    for item in all_data:
        review_date_str = item.get("publishedAtDate")
        if review_date_str:
            try:
                # Parse the ISO 8601 date format
                review_date = datetime.datetime.strptime(review_date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                if review_date < end_date:
                    filtered_data.append(item)
            except ValueError as e:
                logger.error(f"Date parsing error for '{review_date_str}': {e}")
                continue
    
    logger.info(f"Filtered down to {len(filtered_data)} reviews before {end_date_str}.")
    return filtered_data, end_date_str

@task
def store_data_in_gcs(data, date_str):
    logger = get_run_logger()
    if not data:
        logger.warning("No data to store in GCS.")
        return
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    # Define the file name in the bucket
    file_name = f"reviews_before_{date_str}.json"
    blob = bucket.blob(file_name)
    
    # Convert data to JSON string
    data_json = json.dumps(data)
    
    # Upload data to GCS
    try:
        blob.upload_from_string(data_json, content_type='application/json')
        logger.info(f"Data stored in GCS bucket '{GCS_BUCKET_NAME}' as '{file_name}'.")
    except Exception as e:
        logger.error(f"Failed to upload data to GCS: {e}")
        raise

@flow(name="Initial Data Ingestion Flow")
def initial_data_ingestion_flow(end_date_str):
    data, date_str = fetch_all_reviews_before(end_date_str)
    store_data_in_gcs(data, date_str)

if __name__ == "__main__":
    # Set the end date for initial data ingestion
    END_DATE_STR = "2024-10-01"
    initial_data_ingestion_flow(END_DATE_STR)
