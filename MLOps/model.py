import os
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from prefect import flow, task
import logging
import re
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.model_selection import KFold
from sklearn.metrics import mean_squared_error, r2_score
import wandb
from wandb.integration.lightgbm import wandb_callback
from hyperopt import fmin, tpe, hp, Trials, STATUS_OK
import shap
from sklearn.preprocessing import LabelEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up MotherDuck token
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
if not MOTHERDUCK_TOKEN:
    raise ValueError("Please set the MOTHERDUCK_TOKEN environment variable.")

# Initialize wandb
wandb.init(project="lightgbm_regression", name="Hyperparameter_Tuning", reinit=True)

@task
def load_and_merge_data():
    # Connect to MotherDuck DB using DuckDB
    conn = duckdb.connect(database=f'md:?motherduck_token={MOTHERDUCK_TOKEN}')

    # Get list of tables
    tables_df = conn.execute("SHOW TABLES").fetchdf()
    table_names = tables_df['name'].tolist()

    # Identify the latest date
    pattern = r'(processed_review|sentiment_score)_(\d{8})'
    date_table_dict = {}
    for name in table_names:
        match = re.match(pattern, name)
        if match:
            table_type = match.group(1)
            date_str = match.group(2)
            if date_str not in date_table_dict:
                date_table_dict[date_str] = {}
            date_table_dict[date_str][table_type] = name

    # Find the latest date with both tables
    latest_date = None
    for date_str in sorted(date_table_dict.keys(), reverse=True):
        tables = date_table_dict[date_str]
        if 'processed_review' in tables and 'sentiment_score' in tables:
            latest_date = date_str
            processed_review_table = tables['processed_review']
            sentiment_score_table = tables['sentiment_score']
            break

    if not latest_date:
        raise ValueError("No matching processed_review and sentiment_score tables found.")

    # Load data
    df_review = conn.execute(f"SELECT * FROM {processed_review_table}").fetchdf()
    df_sentiment = conn.execute(f"SELECT * FROM {sentiment_score_table}").fetchdf()

    # Merge data on 'name' and 'publishedAtDate'
    df_aggregated = pd.merge(df_review, df_sentiment, on=['name', 'publishedAtDate'], how='inner')

    # Save aggregated table back to MotherDuck
    aggregated_table_name = f'aggregated_table_{latest_date}'
    conn.register('df_aggregated', df_aggregated)
    conn.execute(f"CREATE OR REPLACE TABLE {aggregated_table_name} AS SELECT * FROM df_aggregated")
    logger.info(f"Aggregated data saved to MotherDuck DB as table '{aggregated_table_name}'.")

    return df_aggregated, latest_date

@task
def prepare_data(df_aggregated):
    # Feature engineering: intersaction between the sentimental value and total score (overall score for the restaurant)
    df_aggregated['topic_1_totalScore'] = df_aggregated['topic_1'] * df_aggregated['totalScore']
    df_aggregated['topic_2_totalScore'] = df_aggregated['topic_2'] * df_aggregated['totalScore']
    df_aggregated['topic_3_totalScore'] = df_aggregated['topic_3'] * df_aggregated['totalScore']

    # Prepare features and target
    features = ['topic_1', 'topic_2', 'topic_3', 'isLocalGuide', 'isAdvertisement', 'price', 'totalScore', 
                'number_review_image', 'Service_type', 'review_respond_time', 'food_rating', 'atmosphere_rating', 'service_rating',
                'topic_1_totalScore', 'topic_2_totalScore', 'topic_3_totalScore']
    target = 'stars'

    # Encode categorical variables
    le = LabelEncoder()
    for col in ['price', 'Service_type', 'review_respond_time']:
        df_aggregated[col] = le.fit_transform(df_aggregated[col].astype(str))

    X = df_aggregated[features]
    y = df_aggregated[target]

    # Calculate weights (some reviews are more important than others)
    df_aggregated['weight'] = (df_aggregated['reviewerNumberOfReviews'] + df_aggregated['number_review_image']) / (df_aggregated['reviewerNumberOfReviews'].mean() + df_aggregated['number_review_image'].mean())

    return X, y, df_aggregated['weight']

@task
def train_model(X, y, weights):
    # Split the data
    X_train, X_test, y_train, y_test, weights_train, weights_test = train_test_split(X, y, weights, test_size=0.2, random_state=42)

    # Define the hyperparameter search space
    space = {
        'num_leaves': hp.quniform('num_leaves', 20, 100, 1),
        'max_depth': hp.quniform('max_depth', 3, 10, 1),
        'learning_rate': hp.loguniform('learning_rate', np.log(0.01), np.log(0.3)),
        'n_estimators': hp.quniform('n_estimators', 100, 1000, 1),
    }

    # Objective function for hyperopt
    def objective(params):
        params = {
            'num_leaves': int(params['num_leaves']),
            'max_depth': int(params['max_depth']),
            'learning_rate': params['learning_rate'],
            'n_estimators': int(params['n_estimators']),
            'objective': 'regression',
            'metric': 'mse'
        }
        
        model = lgb.LGBMRegressor(**params)
        
        kf = KFold(n_splits=5, shuffle=True, random_state=42)
        mse_scores = []
        
        for train_index, val_index in kf.split(X_train):
            X_train_fold, X_val_fold = X_train.iloc[train_index], X_train.iloc[val_index]
            y_train_fold, y_val_fold = y_train.iloc[train_index], y_train.iloc[val_index]
            weights_train_fold, weights_val_fold = weights_train.iloc[train_index], weights_train.iloc[val_index]
            
            model.fit(X_train_fold, y_train_fold, sample_weight=weights_train_fold)
            preds = model.predict(X_val_fold)
            mse = mean_squared_error(y_val_fold, preds, sample_weight=weights_val_fold)
            mse_scores.append(mse)
        
        avg_mse = np.mean(mse_scores)
        
        # Log to wandb
        wandb.log({
            'num_leaves': params['num_leaves'],
            'max_depth': params['max_depth'],
            'learning_rate': params['learning_rate'],
            'n_estimators': params['n_estimators'],
            'avg_mse': avg_mse
        })
        
        return {'loss': avg_mse, 'status': STATUS_OK}

    # Run hyperparameter optimization
    trials = Trials()
    best = fmin(fn=objective,
                space=space,
                algo=tpe.suggest,
                max_evals=50,
                trials=trials)

    # Train final model with best parameters
    best_params = {
        'num_leaves': int(best['num_leaves']),
        'max_depth': int(best['max_depth']),
        'learning_rate': best['learning_rate'],
        'n_estimators': int(best['n_estimators']),
        'objective': 'regression',
        'metric': 'mse'
    }
    
    final_model = lgb.LGBMRegressor(**best_params)
    final_model.fit(X_train, y_train, sample_weight=weights_train)

    return final_model, X_test, y_test, weights_test

@task
def evaluate_model(model, X_test, y_test, weights_test):
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred, sample_weight=weights_test)
    r2 = r2_score(y_test, y_pred, sample_weight=weights_test)
    adj_r2 = 1 - (1 - r2) * (len(y_test) - 1) / (len(y_test) - X_test.shape[1] - 1)

    return mse, r2, adj_r2

@task
def calculate_shap_values(model, X):
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)
    return shap_values

@task
def save_results(conn, shap_values, X, mse, r2, adj_r2, latest_date):
    # Save SHAP values
    shap_df = pd.DataFrame(shap_values, columns=X.columns)
    shap_df['row_id'] = X.index
    conn.register('shap_df', shap_df)
    conn.execute(f"CREATE OR REPLACE TABLE SHAP_{latest_date} AS SELECT * FROM shap_df")

    # Save the model performance
    performance_df = pd.DataFrame({
        'metric': ['MSE', 'R2', 'Adjusted_R2'],
        'value': [mse, r2, adj_r2]
    })
    conn.register('performance_df', performance_df)
    conn.execute(f"CREATE OR REPLACE TABLE model_performance_{latest_date} AS SELECT * FROM performance_df")

@flow
def main_flow():
    # Connect to MotherDuck
    conn = duckdb.connect(database=f'md:?motherduck_token={MOTHERDUCK_TOKEN}')

    # Load and merge data
    df_aggregated, latest_date = load_and_merge_data()

    # Prepare data
    X, y, weights = prepare_data(df_aggregated)

    # Train model
    model, X_test, y_test, weights_test = train_model(X, y, weights)

    # Evaluate model
    mse, r2, adj_r2 = evaluate_model(model, X_test, y_test, weights_test)

    # Calculate SHAP values
    shap_values = calculate_shap_values(model, X)

    # Save results
    save_results(conn, shap_values, X, mse, r2, adj_r2, latest_date)

    # Log final results to wandb
    wandb.log({
        'final_mse': mse,
        'final_r2': r2,
        'final_adj_r2': adj_r2
    })

    # Close connections
    conn.close()
    wandb.finish()

    logger.info(f"Process completed for date: {latest_date}")

if __name__ == "__main__":
    main_flow()