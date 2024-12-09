import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import json
import os
import re
from datetime import datetime

# Vertex AI and related imports
import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from vertexai.generative_models import GenerativeModel, Part, Content, GenerationConfig

# Initialize Vertex AI
project_id = os.environ.get("PROJECT_ID", "bold-sorter-435920-d2")
region_id = os.environ.get("REGION_ID", "us-central1")

vertexai.init(project=project_id, location=region_id)

# Get MOTHERDUCK_TOKEN from environment variables
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
if not MOTHERDUCK_TOKEN:
    raise ValueError("MOTHERDUCK_TOKEN environment variable is not set.")

# Connect to MotherDuck using duckdb
conn = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")

# Find the latest processed_review_xxxx table and create a view named 'review'
tables = conn.execute("SHOW TABLES").fetchall()
processed_tables = []
for table in tables:
    match = re.match(r'processed_review_(\d{8})', table[0])
    if match:
        date = datetime.strptime(match.group(1), '%Y%m%d')
        processed_tables.append((table[0], date))

processed_tables.sort(key=lambda x: x[1], reverse=True)

if processed_tables:
    latest_table = processed_tables[0][0]
    conn.execute(f"CREATE OR REPLACE VIEW review AS SELECT * FROM {latest_table}")
else:
    st.error("No processed_review_xxxx tables found.")
    st.stop()

# Retrieve schema of 'review'
schema_query = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'review';
"""
schema = conn.execute(schema_query).df()

# Initialize the generative model (You may need to switch to a known model if gemini is unavailable)
model = GenerativeModel(model_name="gemini-1.5-flash-002")
generation_config = GenerationConfig(temperature=0)

# Streamlit UI
def main():

    st.image("https://businessconceptor.com/wp-content/uploads/2024/04/fish-market-swot.jpg", use_column_width=True)
    
    # Add a title and subtitle
    st.title("Restaurant Text-to-SQL App")
    st.subheader("Analyze the Market Trend Using Google Review Data")

    user_query = st.text_input("Enter your question/query about the restaurant reviews:")
    if st.button("Get Results"):
        if user_query.strip():
            # Construct the prompt
            # Provide schema as JSON records for the model
            prompt = f"""
### System prompt
You are a sql expert.  For the user's input, generate a SQL prompt given the schema provided.
You need to return valid JSON with a key SQL.  The value of the SQL key is the query to be
executed against the database.
Here are the tables and columns in the database:
name (reviewer name)
publishedAtDate (review publish date, in a format of YYYY-MM-DD)
likesCount (how many likes does this review received)
reviewerId (unique reviewer ID)
reviewerNumberOfReviews (Number of reviews posted by the reviewer (from his/her Google profile))
isLocalGuide (Whether this review is a local guide (1/0))
stars (The start (1-5) this review has on the restaurants)
responseFromOwnerText (the text response from Owner)
isAdvertisement (whether the review is categorized as an advertisement)
neighborhood (which district in Boston does the restaurant locatesm like Downtown, Backbay, ...)
title (Restaurant's name)
totalScore (the restaurant's rating on Google review, one restaurant only has one totalScore, it's different from the stars column)
price (categorical variable, only has '$20-30', '$10-20', '$30-50', '$50-100', '$100+' and 'Unknown')
reviewText (Review text posted by reviewer)
number_review_imgae (how many images is attached in specific review)
food_rating: the rating of food given by each review 
service_rating: the rating of service given by each review 
atmosphere_rating: the rating of atmosphere given by each review 
review_respond_time: the time taken for restaurant owners to respond to each review, categorical variables
Only use the table names and columns mentioned in the schema and ensure you return valid SQL query.
For aggregations, you need to ensure that you reference columns in the SELECT clause


Here are the tables and columns in the database (table: review):
{schema.to_dict(orient="records")}

### User prompt
{user_query}

### SQL prompt
"""
            user_prompt_content = Content(
                role="user",
                parts=[Part.from_text(prompt)]
            )

            # Get the response from the model
            response = model.generate_content(
                user_prompt_content,
                generation_config=GenerationConfig(temperature=0, response_mime_type="application/json"),
            )

            # Parse the model's JSON response
            try:
                llm_query = json.loads(response.text)
                sql_query = llm_query.get('SQL')
                if not sql_query:
                    st.error("No SQL found in the model's response.")
                    return
            except json.JSONDecodeError:
                st.error("Model response is not valid JSON.")
                return

            # Execute the SQL and display results
            try:
                df = conn.execute(sql_query).df()
                st.dataframe(df)
            except Exception as e:
                st.error(f"Error executing query: {e}")
        else:
            st.warning("Please enter a query.")

if __name__ == "__main__":
    main()
