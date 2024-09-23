import os
import shutil
import json
import datetime
import pytz


class KaggleDatasetManager:
    def __init__(self, dataset):
        from kaggle import KaggleApi

        self.api = KaggleApi()
        self.api.authenticate()
        self.dataset = dataset
        self.when = self._now()

    def _now(self):
        return datetime.datetime.now(pytz.timezone("Asia/Jerusalem")).strftime(
            "%d/%m/%Y, %H:%M:%S"
        )

    def compose(self, outputs_folder, status_folder):
        shutil.rmtree(self.dataset, ignore_errors=True)
        os.makedirs(self.dataset, exist_ok=True)
        with open(f"{self.dataset}/dataset-metadata.json", "w") as file:
            json.dump(
                {
                    "title": "Israeli Supermarkets 2024",
                    "id": f"erlichsefi/{self.dataset}",
                    "licenses": [{"name": "CC0-1.0"}],
                },
                file,
            )
        shutil.copytree(outputs_folder, self.dataset, dirs_exist_ok=True)    
        shutil.copytree(status_folder, self.dataset, dirs_exist_ok=True)


        self.increase_index()

    def increase_index(self):
        """
        Download a dataset from Kaggle.

        :param dataset: str, the dataset to download in the format 'owner/dataset-name'
        :param path: str, the path where to save the dataset (default is current directory)
        """

        self.api.dataset_download_cli(self.dataset, file_name="index.json")
        print(f"Dataset '{self.dataset}' downloaded successfully")

        with open("index.json", "r") as file:
            index = json.load(file)

        index[max(map(int, index.keys())) + 1] = self.when

        with open(os.path.join(self.dataset, "index.json"), "w") as file:
            json.dump(index, file)

    def upload_to_dataset(self, version_notes=None):
        """
        Upload a new file to an existing Kaggle dataset.

        :param dataset: str, the dataset to upload to in the format 'owner/dataset-name'
        :param file_path: str, the path to the file to upload
        :param new_file_name: str, optional new name for the file in the dataset
        """
        try:
            self.api.dataset_create_version(
                self.dataset, version_notes=f"{self.when} {version_notes}", delete_old_versions=False
            )  # each day is a version
        except Exception as e:
            print(f"Error uploading file: {e}")

    def clean(self,*folders):
        for folder in folders:
            shutil.rmtree(folder)
        shutil.rmtree(self.dataset)
        os.remove("index.json")

# Example usage:
if __name__ == "__main__":
    manager = KaggleDatasetManager("israeli-supermarkets-2024")

    # Upload a folder to a dataset (make sure you have write permissions)
    manager.upload_to_dataset(version_notes="now")
