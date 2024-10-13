In this project we might use the EtTL framework to ingest, process and load the data to datawarehouse (MotherduckDB)

export APIFY_API_TOKEN="apify_api_EkelRouO2zZ7vY0PzeGUJupeTFreRa1o974d"

export GCS_BUCKET_NAME="882_project"

export GOOGLE_APPLICATION_CREDENTIALS="/home/bychan/bold-sorter-435920-d2-a7aa1762d98e.json"

export MOTHERDUCK_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImJ5Y2hhbkBidS5lZHUiLCJzZXNzaW9uIjoiYnljaGFuLmJ1LmVkdSIsInBhdCI6Ikg0Z2xjQVRYTWhCVUlmc0lXWTVXRlp1R3J2UGZQcExjSzhsLUJ1NFNzRXciLCJ1c2VySWQiOiI4Yjg3ZjM0Ny0yNzc0LTQ5YmQtOTY5Ni01Y2JkMTA5OWEwMDkiLCJpc3MiOiJtZF9wYXQiLCJpYXQiOjE3Mjg4Mzg5MjV9.MP7XsXieejItRTPs-F2Gt2Nz5TaOKjmkwYuwCCVx0io"

prefect server start

#and change the port to 4200 to monitor the flow#

