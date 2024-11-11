from prefect import flow, task
from google.cloud import secretmanager
import os
import importlib

# Initialize the Secret Manager client
client = secretmanager.SecretManagerServiceClient()

@task
def get_secret(secret_id):
    name = f"ba882/bold-sorter-435920-d2/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

@task
def set_environment():
    os.environ['WANDB_API_KEY'] = get_secret("wandb_api_key")
    os.environ['MOTHERDUCK_TOKEN'] = get_secret("motherduck_token")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = get_secret("google_cloud_creds")
    os.environ['APIFY_API_KEY'] = get_secret("apify_api_key")
    os.environ['OPENAI_API_KEY'] = get_secret("openai_api_key")

@task
def run_script(script_name):
    print(f"Running {script_name}")
    module = importlib.import_module(script_name.replace('.py', ''))
    if hasattr(module, 'main'):
        module.main()
    else:
        print(f"Warning: {script_name} does not have a main() function.")

@flow
def weekly_workflow():
    set_environment()
    scripts = [
        "deployment_cycle_scrape.py",
        "deployment_aggregation.py",
        "deployment_transform_and_load.py",
        "text_preprocessing.py",
        "nmf_absa_updated.py",
        "model.py"
    ]
    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    weekly_workflow()