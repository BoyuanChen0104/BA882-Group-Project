import os
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
import spacy
from prefect import flow, task
import logging
import re

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up MotherDuck token
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
if not MOTHERDUCK_TOKEN:
    raise ValueError("Please set the MOTHERDUCK_TOKEN environment variable.")

# Load spaCy model
nlp = spacy.load('en_core_web_md')  # Medium model for a balance of speed and accuracy

@task
def load_data():
    conn = duckdb.connect(database='md:?motherduck_token={}'.format(MOTHERDUCK_TOKEN))
    tables_df = conn.execute("SHOW TABLES").fetchdf()
    table_names = tables_df['name'].tolist()
    pattern = r'processed_review_(\d{8})'
    processed_tables = [name for name in table_names if re.match(pattern, name)]
    if not processed_tables:
        raise ValueError("No processed review tables found.")
    latest_table = max(processed_tables)
    df = conn.execute(f"SELECT * FROM {latest_table}").fetchdf()
    logger.info(f"Data loaded from {latest_table}, shape: {df.shape}")
    return df

@task
def perform_topic_modeling(df, n_topics=8):
    documents = df['reviewText'].tolist()
    vectorizer = TfidfVectorizer(max_df=0.95, min_df=2, max_features=5000, stop_words='english')
    tfidf = vectorizer.fit_transform(documents)
    nmf = NMF(n_components=n_topics, random_state=42)
    W = nmf.fit_transform(tfidf)
    feature_names = vectorizer.get_feature_names_out()
    topics = [' '.join([feature_names[i] for i in topic.argsort()[:-11:-1]]) for topic in nmf.components_]
    logger.info("Topics extracted successfully.")
    return W, topics

@task
def perform_sentiment_analysis(df, W, topics):
    df['sentiment'] = df['reviewText'].apply(lambda text: nlp(text)._.polarity_scores['compound'])
    sentiment_matrix = np.tile(df['sentiment'].values, (W.shape[1], 1)).T
    weighted_sentiments = W * sentiment_matrix
    topic_columns = [f'topic_{i}' for i in range(1, W.shape[1]+1)]
    df_result = pd.DataFrame(weighted_sentiments, columns=topic_columns)
    df_result[['name', 'publishedAtDate']] = df[['name', 'publishedAtDate']]
    logger.info("Sentiment analysis completed.")
    return df_result

@task
def save_results(df):
    conn = duckdb.connect(database='md:?motherduck_token={}'.format(MOTHERDUCK_TOKEN))
    date_str = datetime.now().strftime('%Y%m%d')
    table_name = f'sentiment_analysis_{date_str}'
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    logger.info(f"Results saved to {table_name} in MotherDuck DB.")

@flow(name="Topic Modeling and Sentiment Analysis Flow")
def run_flow():
    df = load_data()
    W, topics = perform_topic_modeling(df)
    df_result = perform_sentiment_analysis(df, W, topics)
    save_results(df_result)

if __name__ == "__main__":
    run_flow()
