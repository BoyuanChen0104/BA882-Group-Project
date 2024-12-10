import os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.ensemble import RandomForestRegressor
from pinecone import Pinecone, ServerlessSpec
from tqdm.auto import tqdm
import vertexai
from vertexai.language_models import TextEmbeddingModel
from langchain.text_splitter import RecursiveCharacterTextSplitter

# Configuration
PROJECT_ID = "bold-sorter-435920-d2"
REGION_ID = "us-central1"
FILE_PATH = 'gs://882_project/transformed_data.csv'
PINECONE_API_KEY = os.environ.get('PINECONE_API_KEY')
INDEX_NAME = "ba882-rag-project"

def load_and_preprocess_data():
    """Load and preprocess the data from Google Cloud Storage"""
    # Read the CSV file
    df = pd.read_csv(FILE_PATH)

    # Remove the 'rating' column
    df = df.drop('rating', axis=1)

    # Convert binary columns to int
    binary_columns = ['isLocalGuide', 'isAdvertisement', 'permanentlyClosed', 'temporarilyClosed']
    for col in binary_columns:
        df[col] = df[col].astype(int)

    # Convert rating columns to float
    rating_columns = ['stars', 'totalScore', 'food_rating', 'service_rating', 'atmosphere_rating']
    for col in rating_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convert 'price' to ordinal categories
    price_order = ['$1–10', '$10–20', '$20–30', '$30–50', '$50–100', '$100+']
    df['price'] = pd.Categorical(df['price'], categories=price_order, ordered=True)

    # Convert reviewer columns to int
    df['reviewerNumberOfReviews'] = pd.to_numeric(df['reviewerNumberOfReviews'], errors='coerce').astype('Int64')
    df['number_review_image'] = pd.to_numeric(df['number_review_image'], errors='coerce').astype('Int64')

    # Impute missing values for ratings
    rating_columns_to_impute = ['food_rating', 'service_rating', 'atmosphere_rating']
    imputer = IterativeImputer(estimator=RandomForestRegressor(), max_iter=10, random_state=0)
    df[rating_columns_to_impute] = imputer.fit_transform(df[rating_columns_to_impute])

    # Calculate review length
    df['review_length'] = df['reviewText'].str.len()

    # Drop rows with missing critical information
    df = df.dropna(subset=['reviewText', 'price', 'neighborhood'])

    return df

def create_pinecone_index(pc):
    """Create Pinecone index if it doesn't exist"""
    if not pc.has_index(INDEX_NAME):
        pc.create_index(
            name=INDEX_NAME,
            dimension=768,
            metric="cosine",
            spec=ServerlessSpec(
                cloud='aws',
                region='us-east-1'
            )
        )
    return pc.Index(INDEX_NAME)

def generate_embeddings(df):
    """Generate embeddings and prepare for Pinecone"""
    # Initialize Vertex AI and embedding model
    vertexai.init(project=PROJECT_ID, location=REGION_ID)
    model = TextEmbeddingModel.from_pretrained("text-embedding-005")

    # Setup text splitter
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=350,
        chunk_overlap=75,
        length_function=len,
        is_separator_regex=False,
    )

    chunk_docs = []
    total_reviews = len(df)

    for idx, row in tqdm(df.iterrows(), total=total_reviews, desc="Processing reviews"):
        review_text = row['reviewText']
        chunks = text_splitter.create_documents([review_text])
        
        for cid, chunk in enumerate(chunks):
            chunk_text = chunk.page_content
            embedding = model.get_embeddings([chunk_text])[0]
            
            # Handle date formatting
            formatted_date = row['publishedAtDate'].isoformat() if isinstance(row['publishedAtDate'], datetime) else row['publishedAtDate']
            
            chunk_doc = {
                'id': f"{row.name}_{cid}",
                'values': embedding.values,
                'metadata': {
                    'name': row['name'],
                    'publishedAtDate': formatted_date,
                    'likesCount': int(row['likesCount']),
                    'reviewerId': row['reviewerId'],
                    'stars': float(row['stars']),
                    'title': row['title'],
                    'totalScore': float(row['totalScore']),
                    'chunk_index': cid,
                    'chunk_text': chunk_text,
                    'price': row['price'],
                    'atmosphere_rating': float(row['atmosphere_rating']),
                    'food_rating': float(row['food_rating']),
                    'service_rating': float(row['service_rating']),
                    'neighborhood': row['neighborhood']
                }
            }
            chunk_docs.append(chunk_doc)

    return pd.DataFrame(chunk_docs)

def main():
    # Authenticate and initialize Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    
    # Create or connect to Pinecone index
    index = create_pinecone_index(pc)

    # Load and preprocess data
    df = load_and_preprocess_data()

    # Generate embeddings
    chunks_df = generate_embeddings(df)

    # Upsert to Pinecone
    print("Upserting to Pinecone...")
    index.upsert_from_dataframe(chunks_df, batch_size=100)
    print("Upsert completed successfully!")

if __name__ == "__main__":
    main()
