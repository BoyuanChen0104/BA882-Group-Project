from google.cloud import secretmanager
from prefect.client import get_client
import google.auth

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/your-project-id/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def trigger_prefect_flow(event, context):
    prefect_api_key = get_secret("PREFECT_API_KEY")
    prefect_api_url = get_secret("PREFECT_API_URL")

    client = get_client(api_key=prefect_api_key, api_url=prefect_api_url)

    # Create a flow run
    flow_run = client.create_flow_run(
        flow_name="weekly_workflow",
        deployment_name="weekly_scripts_deployment"
    )

    return f"Flow run {flow_run.id} has been created."