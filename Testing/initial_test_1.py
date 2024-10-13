import os
import json
import datetime
from dateutil.parser import isoparse  # For robust date parsing
from dateutil import tz  # For timezone handling
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
    
    # Convert end_date_str to datetime object with timezone info
    end_date = isoparse(end_date_str + 'T00:00:00Z')
    reviews_start_date = "2022-01-01"
    
    # Define your start URLs
    start_urls = [
        {"url": "https://www.google.com/maps/place/Jumbo+Seafood/@42.350931,-71.0627748,17z/data=!4m8!3m7!1s0x89e37a7847ac82ad:0x59ee82a6474ad485!8m2!3d42.350931!4d-71.0601999!9m1!1b1!16s%2Fg%2F1tjt2z0n?hl=en-GB&entry=ttu&g_ep=EgoyMDI0MDkxOC4xIKXMDSoASAFQAw%3D%3D"},
        {"url": "https://www.google.com/maps/place/Rowayton+Seafood/@41.0640248,-74.4434072,9z/data=!4m11!1m3!2m2!1sseafood+restaurant+near+New+England!6e5!3m6!1s0x89e81fc9005d651d:0x197740d3504cf794!8m2!3d41.0640248!4d-73.4443415!15sCiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBOZXcgRW5nbGFuZFolIiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBuZXcgZW5nbGFuZJIBEnNlYWZvb2RfcmVzdGF1cmFudOABAA!16s%2Fg%2F1thvtqxf?authuser=0&entry=ttu&g_ep=EgoyMDI0MDkyMy4wIKXMDSoASAFQAw%3D%3D"},
        {"url": "https://www.google.com/maps/place/Lobstah+On+A+Roll/@42.349138,-72.2021125,9z/data=!4m11!1m3!2m2!1sseafood+restaurant+near+New+England!6e5!3m6!1s0x89e37a16b438e3f3:0x427727df885af175!8m2!3d42.3416853!4d-71.0807313!15sCiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBOZXcgRW5nbGFuZFolIiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBuZXcgZW5nbGFuZJIBEnNlYWZvb2RfcmVzdGF1cmFudOABAA!16s%2Fg%2F11f_f6c536?authuser=0&entry=ttu&g_ep=EgoyMDI0MTAwOS4wIKXMDSoASAFQAw%3D%3D"}
    ]
    
    run_input = {
        "language": "en",
        "maxReviews": 20,
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
        logger.info(f"Fetched {len(all_data)} reviews.")
        if all_data:
            logger.info(f"Sample data item: {all_data[0]}")
            logger.info(f"Sample data item keys: {all_data[0].keys()}")
        else:
            logger.warning("No data returned from Apify.")
    except Exception as e:
        logger.error(f"Failed to retrieve data from Apify dataset: {e}")
        raise
    
    # Filter reviews before the end_date
    filtered_data = []
    for item in all_data:
        review_date_str = item.get("publishedAtDate")
        if review_date_str:
            try:
                review_date = isoparse(review_date_str)
                if review_date <= end_date:
                    filtered_data.append(item)
            except ValueError as e:
                logger.error(f"Date parsing error for '{review_date_str}': {e}")
                continue
        else:
            logger.warning("No 'publishedAtDate' field in item.")
    
    logger.info(f"Filtered down to {len(filtered_data)} reviews before {end_date_str}.")
    return filtered_data, end_date_str

@task
def transform_data(data):
    logger = get_run_logger()
    logger.info(f"Transforming {len(data)} data items.")
    # Transform the data according to steps A to D
    transformed_data = []
    columns_to_remove = [
        'cid', 'countryCode', 'fid', 'imageUrl', 'placeId', 'publishAt',
        'reviewId', 'reviewOrigin', 'reviewUrl', 'reviewerPhotoUrl',
        'reviewerUrl', 'reviewsCount', 'searchString', 'url'
    ]
    for item in data:
        # Step A: Remove specified columns
        for col in columns_to_remove:
            item.pop(col, None)
        
        # Step B: Transform specified columns to binary
        binary_columns = ['isAdvertisement', 'isLocalGuide', 'permanentlyClosed', 'temporarilyClosed']
        for col in binary_columns:
            item[col] = int(bool(item.get(col)))
        
        # Step C: Combine 'text' and 'textTranslated' into 'reviewText'
        text = item.get('text', '')
        text_translated = item.get('textTranslated', '')
        item['reviewText'] = text_translated if text_translated else text
        # Remove the original 'text' and 'textTranslated' columns
        item.pop('text', None)
        item.pop('textTranslated', None)
        
        # Step D: Count number of images in 'reviewImageUrls'
        image_urls = item.get('reviewImageUrls', [])
        item['number_review_image'] = len(image_urls)
        # Remove 'reviewImageUrls' as per your instruction
        item.pop('reviewImageUrls', None)
        
        # Append the transformed item to the list
        transformed_data.append(item)
    
    logger.info(f"Transformed data has {len(transformed_data)} items.")
    return transformed_data

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
    transformed_data = transform_data(data)
    store_data_in_gcs(transformed_data, date_str)

if __name__ == "__main__":
    # Set the end date for initial data ingestion
    END_DATE_STR = "2024-10-04"
    initial_data_ingestion_flow(END_DATE_STR)

