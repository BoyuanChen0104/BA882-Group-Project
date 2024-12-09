import google.auth
from google.oauth2 import service_account
import streamlit as st
import pandas as pd
import numpy as np
from pinecone import Pinecone, ServerlessSpec
import os
import uuid
from datetime import datetime
from tqdm import tqdm
from sentence_transformers import CrossEncoder
import vertexai
from vertexai.generative_models import GenerativeModel, Part, Content
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from vertexai.generative_models import GenerationConfig

credentials, project = google.auth.default()

# Initialize Vertex AI
project_id = "bold-sorter-435920-d2"  
region_id = "us-central1"             

vertexai.init(project=project_id, location=region_id, credentials=credentials)

# Set up Pinecone
pinecone_api_key = os.environ.get("PINECONE_API_KEY")
pc = Pinecone(api_key=pinecone_api_key)
index_name = "ba882-rag-project"

# Connect to the Pinecone index
if index_name not in pc.list_indexes().names():
    st.error(f"Pinecone index '{index_name}' does not exist.")
    st.stop()

index = pc.Index(index_name)

# Initialize the embedding model
MODEL_NAME = "text-embedding-005"  # Adjust as needed
DIMENSIONALITY = 768
embedder = TextEmbeddingModel.from_pretrained(MODEL_NAME)

# Streamlit app
def main():
    # Display a banner image at the top
    st.image("https://cdn10.bostonmagazine.com/wp-content/uploads/sites/2/2018/02/The-Barking-Crab-patio.jpg", use_column_width=True)
    
    # Add a title and subtitle
    st.title("Restaurant Recommendation App")
    st.subheader("Find the perfect dining experience based on your preferences")

    # Add a horizontal divider and some descriptive text
    st.divider()
    st.markdown("""
    **How it works:**
    1. Enter your dining preferences or needs.
    2. Our model will fetch relevant restaurant recommendations.
    3. Get detailed responses based on actual reviews and contextual data.

    Enjoy exploring Boston's culinary delights!
    """)

    # Input from the user
    query = st.text_input("Enter your preferences:", "")
    
    if st.button("Get Recommendations"):
        if query:
            with st.spinner("Fetching recommendations..."):
                recommendations = get_recommendations(query)
                st.success("Here are your recommendations:")
                st.write(recommendations)
        else:
            st.warning("Please enter your preferences.")

def get_recommendations(query):
    # Embed the query
    input = TextEmbeddingInput(query, "RETRIEVAL_QUERY")
    embedding = embedder.get_embeddings([input])

    # Query Pinecone
    results = index.query(
            vector=embedding[0].values,
            top_k=10,
            include_metadata=True
        )

    # Prepare context
    context = ""
    for match in results.matches:
        context += f"Restaurant: {match.metadata.get('title', 'N/A')}\n"
        context += f"Price: {match.metadata.get('price', 'N/A')}\n"
        context += f"Atmosphere Rating: {match.metadata.get('atmosphere_rating', 'N/A')}\n"
        context += f"Food Rating: {match.metadata.get('food_rating', 'N/A')}\n"
        context += f"Service Rating: {match.metadata.get('service_rating', 'N/A')}\n"
        context += f"Neighborhood: {match.metadata.get('neighborhood', 'N/A')}\n"
        context += f"Categories: {match.metadata.get('subcategory1', 'N/A')}, {match.metadata.get('subcategory2', 'N/A')}\n"
        context += f"Review: {match.metadata.get('chunk_text', 'N/A')}\n\n"

    # Construct the prompt
    prompt_template = f"""
    You are a restaurant search and recommendation assistant trained to provide detailed and accurate answers based on the provided context.
    Use the context below to respond to the query. Pay special attention to the user's preferences.
    If the context does not contain sufficient information to answer fully, provide the best recommendations possible based on the available information and explain any limitations.

    ### Context:
    {context}

    ### Query:
    {query}

    ### Your Response:
    """

    # Initialize Gemini model for retrieval
    model = GenerativeModel(model_name="gemini-1.5-flash-002")

    # we can control the params of the model, again with the sdk
    generation_config = GenerationConfig(temperature=0)
    
    # Generate the response using Vertex AI
    user_prompt_content = Content(
        role="user",
        parts=[
            Part.from_text(prompt_template),
        ],
    )

    # Get the response
    response = model.generate_content(
        user_prompt_content,
        generation_config=generation_config,
    )

    return response.text

if __name__ == "__main__":
    main()
