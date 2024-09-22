import os

class KaggleDatasetManager:
    def __init__(self, dataset, status_files):
        from kaggle import KaggleApi
        self.api = KaggleApi()
        self.api.authenticate()
        self.dataset = dataset
        self.status_files = status_files

    def download_status_files(self):
        """
        Download a dataset from Kaggle.

        :param dataset: str, the dataset to download in the format 'owner/dataset-name'
        :param path: str, the path where to save the dataset (default is current directory)
        """
        for status_file in self.status_files:
            try:
                self.api.dataset_download_cli(self.dataset, file_name=status_file)
                print(f"Dataset '{self.dataset}' downloaded successfully")
            except Exception as e:
                print(f"Error downloading dataset: {e}")

    def upload_to_dataset(self, version_notes=None):
        """
        Upload a new file to an existing Kaggle dataset.

        :param dataset: str, the dataset to upload to in the format 'owner/dataset-name'
        :param file_path: str, the path to the file to upload
        :param new_file_name: str, optional new name for the file in the dataset
        """
        try:
            self.api.dataset_create_version(self.dataset, version_notes=version_notes,delete_old_versions=False) # each day is a version
        except Exception as e:
            print(f"Error uploading file: {e}")

# Example usage:
if __name__ == "__main__":
    manager = KaggleDatasetManager("israeli-supermarkets-2024")

    # Upload a folder to a dataset (make sure you have write permissions)
    manager.upload_to_dataset(version_notes='now')