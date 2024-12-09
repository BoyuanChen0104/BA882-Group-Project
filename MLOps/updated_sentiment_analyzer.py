import torch
import spacy
from transformers import BertTokenizer, BertForSequenceClassification, pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
import pandas as pd
import re
import duckdb
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Replace this with your actual MotherDuck token
MOTHERDUCK_TOKEN = "your_motherduck_token_here"

def load_data():
    # Connect to MotherDuck DB using DuckDB
    conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')

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

def load_bert_model():
    model_name = "nlptown/bert-base-multilingual-uncased-sentiment"
    return pipeline("sentiment-analysis", model=model_name, tokenizer=model_name)

def extract_aspects(text, nlp):
    doc = nlp(text)
    aspects = [chunk.text for chunk in doc.noun_chunks]
    return aspects

def analyze_aspect_sentiment(aspect, context, sentiment_pipeline):
    text = f"Aspect: {aspect} Context: {context}"
    # Encode and truncate the input
    encoded_input = sentiment_pipeline.tokenizer.encode_plus(
        text,
        max_length=512,
        truncation=True,
        padding='max_length',
        return_tensors='pt'
    )

    # Get the model output
    with torch.no_grad():
        output = sentiment_pipeline.model(**encoded_input)

    # Get the predicted class and its probability
    predicted_class = torch.argmax(output.logits, dim=1).item()
    probabilities = torch.nn.functional.softmax(output.logits, dim=1)
    score = probabilities[0][predicted_class].item()

    # Map the predicted class to a label (star rating)
    label_map = {0: '1 star', 1: '2 stars', 2: '3 stars', 3: '4 stars', 4: '5 stars'}
    label = label_map[predicted_class]

    return label, score

def topic_modeling(df, n_topics=3):
    vectorizer = TfidfVectorizer(max_df=0.95, min_df=2, stop_words='english')
    tfidf = vectorizer.fit_transform(df['reviewText'])
    nmf_model = NMF(n_components=n_topics, random_state=42)
    W = nmf_model.fit_transform(tfidf)
    H = nmf_model.components_
    feature_names = vectorizer.get_feature_names_out()
    return W, H, feature_names

def aspect_based_sentiment_analysis(df):
    sentiment_pipeline = load_bert_model()
    nlp = spacy.load("en_core_web_sm")

    W, H, feature_names = topic_modeling(df)

    results = []
    for idx, row in df.iterrows():
        aspects = extract_aspects(row['reviewText'], nlp)
        aspect_sentiments = []
        for aspect in aspects:
            label, score = analyze_aspect_sentiment(aspect, row['reviewText'], sentiment_pipeline)
            aspect_sentiments.append((aspect, label, score))

        topic_sentiments = []
        for topic_idx, topic_weight in enumerate(W[idx]):
            topic_words = [feature_names[i] for i in H[topic_idx].argsort()[:-10 - 1:-1]]
            topic_aspects = [a for a, _, _ in aspect_sentiments if any(word in a.lower() for word in topic_words)]
            if topic_aspects:
                topic_sentiment = sum(score for a, _, score in aspect_sentiments if a in topic_aspects) / len(topic_aspects)
            else:
                topic_sentiment = 0
            topic_sentiments.append(topic_sentiment * topic_weight)

        results.append({
            'name': row['name'],
            'publishedAtDate': row['publishedAtDate'],
            'aspects': aspect_sentiments,
            'topic_sentiments': topic_sentiments
        })

        if idx % 100 == 0:
            logger.info(f"Processed {idx} reviews")

    return pd.DataFrame(results)

if __name__ == "__main__":
    df = load_data()
    df_result = aspect_based_sentiment_analysis(df)
    print(df_result.head())
    # You can add more operations here, such as saving the results to a file
    # df_result.to_csv("sentiment_analysis_results.csv", index=False)
