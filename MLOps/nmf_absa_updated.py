import os
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
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

@task
def load_data():
    # Connect to MotherDuck DB using DuckDB
    conn = duckdb.connect(database=f'md:?motherduck_token={MOTHERDUCK_TOKEN}')

    # Get list of tables matching 'processed_review_%'
    tables_df = conn.execute("SHOW TABLES").fetchdf()
    table_names = tables_df['name'].tolist()

    # Filter for tables that match 'processed_review_<date>'
    pattern = r'processed_review_(\d{8})'
    processed_tables = []
    for name in table_names:
        match = re.match(pattern, name)
        if match:
            date_str = match.group(1)
            processed_tables.append((name, date_str))

    if not processed_tables:
        raise ValueError("No tables found matching pattern 'processed_review_<date>'")

    # Sort tables by date in descending order
    processed_tables.sort(key=lambda x: x[1], reverse=True)

    # Get the most recent table
    latest_table_name, latest_date_str = processed_tables[0]

    # Load data from the most recent table
    df = conn.execute(f"SELECT * FROM {latest_table_name}").fetchdf()

    logger.info(f"Data loaded from table '{latest_table_name}', shape: {df.shape}")

    return df

@task
def topic_modeling(df, n_topics=3):
    # Extract 'reviewText'
    documents = df['reviewText'].tolist()

    # Vectorize the documents using TF-IDF
    vectorizer = TfidfVectorizer(
        max_df=0.95, min_df=2, max_features=5000, stop_words='english'
    )
    tfidf = vectorizer.fit_transform(documents)

    # Apply NMF
    nmf_model = NMF(n_components=n_topics, random_state=42)
    W = nmf_model.fit_transform(tfidf)
    H = nmf_model.components_

    feature_names = vectorizer.get_feature_names_out()

    # Extract top words for each topic
    n_top_words = 10
    topics = []
    topic_dicts = []
    for topic_idx, topic in enumerate(H):
        top_indices = topic.argsort()[:-n_top_words - 1:-1]
        top_words = [feature_names[i] for i in top_indices]
        topics.append(top_words)
        logger.info(f"Topic {topic_idx+1}: {' '.join(top_words)}")
        # Prepare data for topic DataFrame
        topic_dict = {'topic': f'topic{topic_idx+1}'}
        for i, word in enumerate(top_words):
            topic_dict[f'word{i+1}'] = word
        topic_dicts.append(topic_dict)

    # Create DataFrame for topics
    df_topics = pd.DataFrame(topic_dicts)

    return W, topics, df_topics

@task
def sentiment_analysis(df, W):
    # Initialize VADER sentiment analyzer
    nltk.download('vader_lexicon', quiet=True)
    sia = SentimentIntensityAnalyzer()

    # Get overall sentiment scores for each review
    sentiment_scores = []
    for text in df['reviewText']:
        sentiment = sia.polarity_scores(text)
        sentiment_scores.append(sentiment['compound'])  # Use compound score

    sentiment_scores = np.array(sentiment_scores)

    # For each review, compute sentiment scores for each topic
    # Multiply overall sentiment score by topic weights
    topic_sentiments = W * sentiment_scores[:, np.newaxis]

    # Prepare DataFrame with results
    topic_columns = [f'topic_{i+1}' for i in range(W.shape[1])]
    df_topic_sentiments = pd.DataFrame(topic_sentiments, columns=topic_columns)

    # Add 'name' and 'publishedAtDate'
    df_result = pd.concat(
        [df[['name', 'publishedAtDate']].reset_index(drop=True), df_topic_sentiments],
        axis=1
    )

    return df_result

@task
def save_results(df_result):
    # Get current date
    current_date_str = datetime.now().strftime('%Y%m%d')
    table_name = f'sentiment_score_{current_date_str}'

    # Connect to MotherDuck DB using DuckDB
    conn = duckdb.connect(database=f'md:?motherduck_token={MOTHERDUCK_TOKEN}')

    # Register df_result as a DuckDB view
    conn.register('df_result', df_result)

    # Create or replace table in MotherDuck DB
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_result")

    logger.info(f"Sentiment scores saved to MotherDuck DB as table '{table_name}'.")

@task
def save_topics(df_topics):
    # Get current date
    current_date_str = datetime.now().strftime('%Y%m%d')
    table_name = f'topic_{current_date_str}'

    # Connect to MotherDuck DB using DuckDB
    conn = duckdb.connect(database=f'md:?motherduck_token={MOTHERDUCK_TOKEN}')

    # Register df_topics as a DuckDB view
    conn.register('df_topics', df_topics)

    # Create or replace table in MotherDuck DB
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_topics")

    logger.info(f"Topics saved to MotherDuck DB as table '{table_name}'.")

@flow(name="NMF_and_ABSA")
def nmf_and_absa_flow():
    df = load_data()
    W, topics, df_topics = topic_modeling(df, n_topics=3)
    df_result = sentiment_analysis(df, W)
    save_results(df_result)
    save_topics(df_topics)

if __name__ == "__main__":
    nmf_and_absa_flow()
