import dagster as dg
from dagster_project.jobs import monthly_update_job

monthly_schedule = dg.ScheduleDefinition(
    name="monthly_update",
    job=monthly_update_job,
    cron_schedule="0 23 1 * *",
    description="Runs monthly on the 1st at 11pm to fetch new data"
)