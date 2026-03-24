import dagster as dg
from dagster_project.assets import download_data, dbt_build

monthly_update_job = dg.define_asset_job(
    name="monthly_update",
    selection=dg.AssetSelection.assets(download_data, dbt_build),
    description="Monthly job to download fresh data and run dbt build"
)