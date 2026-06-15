from pathlib import Path
from ckanapi import RemoteCKAN
import requests

PROJECT_ROOT = Path(__file__).parent.parent
hdx = RemoteCKAN('https://data.humdata.org')

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
    try:
        dataset = hdx.action.package_show(id=dataset_name)

        resource_url = None
        for resource in dataset['resources']:
            if resource['name'] == resource_name:
                resource_url = resource['url']
                break

        if not resource_url:
            print("Resource not found: " + resource_name)
            return False

        output_path.mkdir(parents=True, exist_ok=True)
        response = requests.get(resource_url)
        response.raise_for_status()

        target = output_path / resource_name
        with open(target, 'wb') as f:
            f.write(response.content)

        print(resource_name + "saved to " + target)
        return True

    except Exception as e:
        print("Error downloading: " + dataset_name)
        return False


def main():
    for file in files_to_download:
        download_resource(file["dataset"], file["resource"], Path(file["path"]))


if __name__ == "__main__":
    main()