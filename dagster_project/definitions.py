import dagster as dg
from dagster_project.assets import download_data, dbt_build
from dagster_project.jobs import monthly_update_job
from dagster_project.schedules import monthly_schedule

defs = dg.Definitions(
    assets=[download_data, dbt_build],
    jobs=[monthly_update_job],
    schedules=[monthly_schedule]
)