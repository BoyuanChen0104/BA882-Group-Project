from prefect import flow, task
from google.cloud import secretmanager
import os

client = secretmanager.SecretManagerServiceClient()

@task
def get_secret(secret_id):
    name = f"ba882/bold-sorter-435920-d2/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

@task
def set_environment():
    os.environ['WANDB_API_KEY'] = get_secret("WANDB_API_KEY")
    os.environ['MOTHERDUCK_TOKEN'] = get_secret("MOTHERDUCK_TOKEN")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = get_secret("GOOGLE_APPLICATION_CREDENTIALS")
    os.environ['APIFY_API_KEY'] = get_secret("APIFY_API_KEY")
    os.environ['GCS_BUCKET_NAME'] = get_secret("GCS_BUCKET_NAME")

@task
def run_script(script_number):
    exec(open(f'script_{script_number}.py').read())

@flow
def weekly_workflow():
    set_environment()
    for i in range(1, 8):
        run_script(i)

if __name__ == "__main__":
    weekly_workflow()