# Use an official lightweight Python image
FROM python:3.11-slim

# Create app directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bold-sorter-435920-d2-a7aa1762d98e.json /app/bold-sorter-435920-d2-a7aa1762d98e.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/bold-sorter-435920-d2-a7aa1762d98e.json

# Copy the app code into the container
COPY . .

# Expose the port that Cloud Run uses
EXPOSE 8501

# Set environment variables for Streamlit
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Run the app, ensuring it listens on the port provided by Cloud Run
CMD ["streamlit", "run", "app.py", "--server.port", "8080", "--server.address", "0.0.0.0"]


