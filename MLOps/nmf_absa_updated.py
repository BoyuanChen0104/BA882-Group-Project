import os
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import sent_tokenize
from prefect import flow, task
import logging
import re
from sentence_transformers import SentenceTransformer, util

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
    conn = duckdb.connect(database='md:?motherduck_token={}'.format(MOTHERDUCK_TOKEN))

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
def topic_modeling(df, n_topics=4):
    # Extract 'reviewText'
    documents = df['reviewText'].tolist()

    # Vectorize the documents using TF-IDF
    vectorizer = TfidfVectorizer(max_df=0.95, min_df=2, max_features=5000, stop_words='english')
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
def sentiment_analysis(df, W, topics):
    # Initialize NLTK and SentenceTransformer
    nltk.download('punkt', quiet=True)
    nltk.download('vader_lexicon', quiet=True)
    sia = SentimentIntensityAnalyzer()
    model = SentenceTransformer('all-MiniLM-L6-v2')  # Efficient model for embeddings

    # Create embeddings for topic keywords
    topic_embeddings = []
    for top_words in topics:
        # Generate embeddings for each word and average them
        word_embeddings = model.encode(top_words, convert_to_tensor=True)
        topic_embedding = word_embeddings.mean(dim=0)
        topic_embeddings.append(topic_embedding)

    # List to hold sentiment scores for each topic per review
    topic_sentiment_scores = []

    # Iterate over each review
    for idx, review in df['reviewText'].iteritems():
        # Split the review into sentences
        sentences = sent_tokenize(review)
        # Generate embeddings for sentences
        sentence_embeddings = model.encode(sentences, convert_to_tensor=True)
        # Initialize lists to hold sentences per topic
        topic_sentences = {i: [] for i in range(len(topics))}

        # Compute similarity between sentence embeddings and topic embeddings
        for i, topic_embedding in enumerate(topic_embeddings):
            # Compute cosine similarities
            cosine_scores = util.cos_sim(sentence_embeddings, topic_embedding)
            # Threshold for considering a sentence relevant to a topic
            threshold = 0.5  # Adjust as needed
            relevant_indices = (cosine_scores >= threshold).nonzero(as_tuple=True)[0]
            # Add relevant sentences to the topic
            for idx in relevant_indices:
                topic_sentences[i].append(sentences[idx])

        # Calculate sentiment scores for each topic
        topic_scores = []
        for i in range(len(topics)):
            text = ' '.join(topic_sentences[i])
            if text:
                sentiment = sia.polarity_scores(text)
                score = sentiment['compound']
            else:
                score = 0.0  # No relevant sentences, neutral sentiment
            topic_scores.append(score)

        topic_sentiment_scores.append(topic_scores)

    topic_sentiment_scores = np.array(topic_sentiment_scores)

    # Multiply topic sentiment scores by topic weights
    weighted_topic_sentiments = W * topic_sentiment_scores

    # Prepare DataFrame with results
    topic_columns = [f'topic_{i+1}' for i in range(W.shape[1])]
    df_topic_sentiments = pd.DataFrame(weighted_topic_sentiments, columns=topic_columns)

    # Add 'name' and 'publishedAtDate'
    df_result = pd.concat([df[['name', 'publishedAtDate']].reset_index(drop=True), df_topic_sentiments], axis=1)

    return df_result

@task
def save_results(df_result):
    # Get current date
    current_date_str = datetime.now().strftime('%Y%m%d')
    table_name = f'sentiment_score_{current_date_str}'

    # Connect to MotherDuck DB using DuckDB
    conn = duckdb.connect(database='md:?motherduck_token={}'.format(MOTHERDUCK_TOKEN))

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
    conn = duckdb.connect(database='md:?motherduck_token={}'.format(MOTHERDUCK_TOKEN))

    # Register df_topics as a DuckDB view
    conn.register('df_topics', df_topics)

    # Create or replace table in MotherDuck DB
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_topics")

    logger.info(f"Topics saved to MotherDuck DB as table '{table_name}'.")

@flow(name="NMF_and_ABSA")
def nmf_and_absa_flow():
    df = load_data()
    W, topics, df_topics = topic_modeling(df)
    df_result = sentiment_analysis(df, W, topics)
    save_results(df_result)
    save_topics(df_topics)

if __name__ == "__main__":
    nmf_and_absa_flow()
