from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_flow import weekly_workflow

deployment = Deployment.build_from_flow(
    flow=weekly_workflow,
    name="weekly_scripts_deployment",
    schedule=CronSchedule(cron="0 1 * * 2"),  # Run at 1 AM every Tuesday
    work_queue_name="default"
)

if __name__ == "__main__":
    deployment.apply()