import dagster as dg
import subprocess
import os
from pathlib import Path

from anaconda_project.internal.conda_api import result

@dg.asset(
    compute_kind="python",
    group_name="data_ingestion",
    description="Downloads latest commodity and aid data"
)
def download_data(context: dg.AssetExecutionContext):
    script_path = Path(__file__).parent.parent / "scripts" / "download_data.py"

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found at {script_path}")

    context.log.info(f"Running download script: {script_path}")

    result = subprocess.run(
        ["python", str(script_path)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent  # Run from project root
    )

    if result.returncode != 0:
        context.log.error(f"Download failed: {result.stderr}")
        raise Exception(f"Download script failed with code {result.returncode}")

    context.log.info(f"Download successful: {result.stdout}")


@dg.asset(
    compute_kind="dbt",
    group_name="transformation",
    deps=[download_data],
    description="Runs dbt build to transform downloaded data"
)
def dbt_build(context: dg.AssetExecutionContext):

    context.log.info("Starting dbt build...")

    result = subprocess.run(
        ["dbt", "build"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )

    if result.returncode != 0:
        context.log.error(f"dbt build failed: {result.stderr}")
        raise Exception(f"dbt build failed with code {result.returncode}")

    context.log.info("dbt build completed successfully")

    return {"status": "success", "output": result.stdout}