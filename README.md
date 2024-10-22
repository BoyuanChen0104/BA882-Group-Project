# BA882-Group-Project

Collaborator: Mengxin Zhao, Gaurangi Agrawal, Varun Kaza

This repository contains a data pipeline developed to extract, process, and load review and profile data from Google Reviews for 100 Boston seafood restaurants. The pipeline automates data collection using Apify for web scraping, orchestrates tasks with Prefect, stores data in Google Cloud Storage, and loads transformed data into the MotherDuck cloud data warehouse using DuckDB. It includes initial data ingestion of historical reviews, weekly updates to keep the dataset current, data aggregation to combine and deduplicate reviews, and data transformation to prepare the data for analysis.

This project was developed aiming to support business intelligence operations with the potential for future enhancements in machine learning applications. The pipeline leverages tools like Pandas for data manipulation and follows best practices for workflow management and data security. Sensitive information such as API tokens and credentials are securely managed using environment variables.