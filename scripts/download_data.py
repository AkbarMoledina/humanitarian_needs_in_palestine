from pathlib import Path
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

Configuration.create(
    hdx_site="prod",
    user_agent="humanitarian_needs_palestine",
    hdx_read_only=True
)


# Setting root folder, dataset names, resource names and paths
PROJECT_ROOT = Path(__file__).parent.parent
files_to_download = [
    {
        "dataset": "state-of-palestine-price-of-basic-commodities-in-gaza",
        "resource": "commodity-prices-in-gaza.xlsx",
        "path": PROJECT_ROOT / "data" / "Prices of basic commodities in Gaza"
    },
    {
        "dataset": "state-of-palestine-gaza-aid-truck-data",
        "resource": "Commodities Received.xlsx",
        "path": PROJECT_ROOT / "data" / "Gaza Supplies and Dispatch Tracking"
    }
]

def download_resource(dataset_name: str, resource_name: str, output_path: Path) -> bool:
    ds = Dataset.read_from_hdx(dataset_name)
    if not ds:
        print(f"Dataset not found: {dataset_name}")
        return False

    for res in ds.get_resources():
        if res["name"] == resource_name:
            output_path.mkdir(parents=True, exist_ok=True)
            _, downloaded_path = res.download(folder=str(output_path))
            downloaded = Path(downloaded_path)
            target = output_path / resource_name
            downloaded.replace(target)
            print(f"{resource_name} saved to {target}")
            return True

    print(f"Resource not found: {resource_name}")
    return False

def main():
    for file in files_to_download:
        download_resource(file["dataset"], file["resource"], Path(file["path"]))

if __name__ == "__main__":
    main()