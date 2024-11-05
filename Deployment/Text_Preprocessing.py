import os
import duckdb
import pandas as pd
from datetime import datetime
import re
from sklearn.experimental import enable_iterative_imputer  # noqa
from sklearn.impute import IterativeImputer
from sklearn.ensemble import RandomForestRegressor
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import RegexpTokenizer
from prefect import flow, task
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Force download NLTK data files
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)
nltk.download('wordnet', quiet=True)

# Set up MotherDuck token
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')

if not MOTHERDUCK_TOKEN:
    raise ValueError("Please set the MOTHERDUCK_TOKEN environment variable.")

@task
def extract_data():
    # Connect to MotherDuck DB using DuckDB
    conn = duckdb.connect(database='md:?motherduck_token={}'.format(MOTHERDUCK_TOKEN))
    # Extract data from 'reviews' table
    df = conn.execute("SELECT * FROM reviews").fetchdf()
    # Display initial data shape
    logger.info(f"Initial data shape: {df.shape}")
    return df

@task
def preprocess_data(df):
    # Columns to keep
    columns_to_keep = [
        'name',
        'publishedAtDate',
        'likesCount',
        'reviewerId',
        'reviewerNumberOfReviews',
        'isLocalGuide',
        'stars',
        'responseFromOwnerDate',
        'responseFromOwnerText',
        'isAdvertisement',
        'neighborhood',
        'title',
        'totalScore',
        'temporarilyClosed',
        'price',
        'reviewText',
        'number_review_image',
        'food_rating',
        'service_rating',
        'atmosphere_rating',
        'Service_type'
    ]

    # Select only the columns to keep
    df = df[columns_to_keep]

    # Drop records where 'reviewText' is null
    df = df[df['reviewText'].notnull()]

    # Keep records where 'temporarilyClosed' == 0
    df = df[df['temporarilyClosed'] == 0]

    # Transform 'price' column
    valid_prices = ['$10-20', '$20-30', '$30-50', '$50-100', '$100+']

    def transform_price(x):
        if pd.isnull(x):
            return 'Unknown'
        x = x.replace('–', '-')  # Replace en-dash with hyphen
        x = x.strip()  # Remove any leading/trailing whitespace
        if x in valid_prices:
            return x
        else:
            return 'Unknown'

    df['price'] = df['price'].apply(transform_price)

    # Initialize tokenizer and lemmatizer
    tokenizer = RegexpTokenizer(r'\w+')  # Tokenizes by words (alphanumeric characters)
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))

    # Define the text preprocessing function
    def preprocess_text(text):
        if pd.isnull(text):
            return ''
        # Convert to lowercase
        text = text.lower()
        # Tokenize using RegexpTokenizer
        tokens = tokenizer.tokenize(text)
        # Remove stop words
        tokens = [word for word in tokens if word not in stop_words]
        # Lemmatization
        tokens = [lemmatizer.lemmatize(word) for word in tokens]
        # Join tokens back into a string
        return ' '.join(tokens)

    # Apply preprocessing to 'reviewText' and 'responseFromOwnerText'
    df['reviewText'] = df['reviewText'].apply(preprocess_text)
    df['responseFromOwnerText'] = df['responseFromOwnerText'].apply(preprocess_text)

    # Convert 'publishedAtDate' and 'responseFromOwnerDate' to datetime (dates only)
    df['publishedAtDate'] = pd.to_datetime(df['publishedAtDate']).dt.normalize()
    df['responseFromOwnerDate'] = pd.to_datetime(df['responseFromOwnerDate'], errors='coerce').dt.normalize()

    # Create 'review_respond_time' column (difference in days)
    df['review_respond_time'] = (df['responseFromOwnerDate'] - df['publishedAtDate']).dt.days

    # Add 1 to 'review_respond_time' to remove negative values
    df['review_respond_time'] = df['review_respond_time'] + 1

    # Fill NaN values in 'review_respond_time' with -1 (indicating no response)
    df['review_respond_time'] = df['review_respond_time'].fillna(-1).astype(int)

    # Drop 'responseFromOwnerDate' as it's no longer needed
    df = df.drop(columns=['responseFromOwnerDate'])

    # Impute missing values for 'food_rating', 'service_rating', 'atmosphere_rating'
    imputer = IterativeImputer(estimator=RandomForestRegressor(), random_state=0)
    ratings = ['food_rating', 'service_rating', 'atmosphere_rating']
    df[ratings] = imputer.fit_transform(df[ratings])

    # Convert imputed ratings to integers if appropriate
    df[ratings] = df[ratings].round().astype(int)

    # Transform nulls in 'Service_type' to 'Unknown'
    df['Service_type'] = df['Service_type'].fillna('Unknown')

    # Drop 'temporarilyClosed' column
    df = df.drop(columns=['temporarilyClosed'])

    # Keep records where 'review_respond_time' is greater than or equal to -1
    df = df[df['review_respond_time'] >= -1]

    # Transform 'review_respond_time' based on the specified conditions
    def transform_review_respond_time(days):
        if days == -1:
            return 'No Reply'
        elif days == 0:
            return 'Within one day'
        elif 1 <= days <= 7:
            return 'Within one week'
        elif 8 <= days <= 30:
            return 'Within one month'
        elif 31 <= days <= 90:
            return 'Within three months'
        elif 91 <= days <= 180:
            return 'Within six months'
        elif days > 181:
            return 'More than six months'
        else:
            return 'Unknown'  # In case there are unexpected values

    df['review_respond_time'] = df['review_respond_time'].apply(transform_review_respond_time)

    # Display final data shape
    logger.info(f"Final data shape after preprocessing: {df.shape}")

    return df

@task
def save_processed_data(df):
    # Connect to MotherDuck DB using DuckDB
    conn = duckdb.connect(database='md:?motherduck_token={}'.format(MOTHERDUCK_TOKEN))

    # Generate current date string
    current_date_str = datetime.now().strftime('%Y%m%d')

    # Create the table name
    table_name = f'processed_review_{current_date_str}'

    # Save the DataFrame back to MotherDuck DB
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")

    logger.info(f"Processed data saved to MotherDuck DB as table '{table_name}'.")

    return table_name

@flow(name="Data_Extraction_and_Preprocessing")
def data_extraction_and_preprocessing_flow():
    df_raw = extract_data()
    df_processed = preprocess_data(df_raw)
    table_name = save_processed_data(df_processed)

if __name__ == "__main__":
    # Start the flow run
    data_extraction_and_preprocessing_flow()
