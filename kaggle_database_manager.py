import os

class KaggleDatasetManager:
    def __init__(self, username=None, key=None):
        from kaggle import KaggleApi
        self.api = KaggleApi()
        self.api.authenticate()

    def download_dataset(self, dataset, path="."):
        """
        Download a dataset from Kaggle.

        :param dataset: str, the dataset to download in the format 'owner/dataset-name'
        :param path: str, the path where to save the dataset (default is current directory)
        """
        try:
            self.api.dataset_download_files(dataset, path=path, unzip=True)
            print(f"Dataset '{dataset}' downloaded successfully to {path}")
        except Exception as e:
            print(f"Error downloading dataset: {e}")

    def upload_to_dataset(self, dataset, file_path, new_file_name=None):
        """
        Upload a new file to an existing Kaggle dataset.

        :param dataset: str, the dataset to upload to in the format 'owner/dataset-name'
        :param file_path: str, the path to the file to upload
        :param new_file_name: str, optional new name for the file in the dataset
        """
        try:
            if new_file_name is None:
                new_file_name = os.path.basename(file_path)

            metadata = {
                "path": file_path,
                "name": new_file_name,
            }

            self.api.dataset_create_version(dataset, metadata, dir_mode="replace")
            print(
                f"File '{new_file_name}' uploaded successfully to dataset '{dataset}'"
            )
        except Exception as e:
            print(f"Error uploading file: {e}")

    def upload_folder_to_dataset(self, dataset, folder_path):
        """
        Upload all files in a folder to an existing Kaggle dataset.

        :param dataset: str, the dataset to upload to in the format 'owner/dataset-name'
        :param folder_path: str, the path to the folder to upload
        """
        try:
            # Check if folder exists
            if not os.path.isdir(folder_path):
                raise ValueError(f"The path {folder_path} is not a valid folder.")

            # Loop through all files in the folder and upload them one by one
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    new_file_name = os.path.relpath(file_path, folder_path)
                    self.upload_to_dataset(dataset, file_path, new_file_name)

            print(f"All files in folder '{folder_path}' uploaded successfully to dataset '{dataset}'")
        except Exception as e:
            print(f"Error uploading folder: {e}")

# Example usage:
if __name__ == "__main__":
    manager = KaggleDatasetManager()

    # Upload a folder to a dataset (make sure you have write permissions)
    manager.upload_folder_to_dataset("israeli-supermarkets-2024",'now')