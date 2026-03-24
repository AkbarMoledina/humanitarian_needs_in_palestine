from pathlib import Path
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

Configuration.create(
    hdx_site="prod",
    user_agent="humanitarian_needs_palestine",
    hdx_read_only=True
)

files_to_download = [
    {
        "dataset": "state-of-palestine-price-of-basic-commodities-in-gaza",
        "resource": "commodity-prices-in-gaza.xlsx",
        "path": "../data/Prices of basic commodities in Gaza/"
    },
    {
        "dataset": "state-of-palestine-gaza-aid-truck-data",
        "resource": "commodities-received-13.xlsx",
        "path": "../data/Gaza Supplies and Dispatch Tracking/"
    }
]

def download_resource(dataset_name: str, resource_name: str, output_path: Path):
    dataset = Dataset.read_from_hdx(dataset_name)
    if not dataset:
        print(dataset_name + " not found")

    resources = dataset.get_resources()

    for resource in resources:
        if resource["name"] == resource_name:
            resource.download(folder=str(output_path))
            print(str(dataset) + " saved to " + str(output_path))


def main():
    for file in files_to_download:
        download_resource(file["dataset"], file["resource"], Path(file["path"]))
        print(file["resource"] + " downloaded!")

if __name__ == "__main__":
    main()