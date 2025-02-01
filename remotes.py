from abc import ABC, abstractmethod
from kaggle import KaggleApi
import os
import logging
import json
import shutil


class RemoteDatabaseUploader(ABC):
    """
    Abstract class for uploading data to a remote database.
    """

    @abstractmethod
    def increase_index(self):
        """
        Define the new index.
        """
        pass

    @abstractmethod
    def upload_to_dataset(self, message):
        """
        Upload a dataset to the remote.
        """
        pass

    @abstractmethod
    def clean(self):
        """
        Clean the dataset.
        """
        pass


class Dummy(RemoteDatabaseUploader):
    """
    Uploads data to a remote database.
    """

    def __init__(self, dataset_remote_name, dataset_path, when):
        self.dataset_remote_name = dataset_remote_name
        self.dataset_path = dataset_path
        self.when = when

    def increase_index(self):
        """
        Increase the index.
        """
        pass

    def upload_to_dataset(self, message):
        """
        Upload the dataset.
        """
        logging.info(
            f"Uploading dataset '{self.dataset_remote_name}' to remote database, message {message}"
        )
        server_path = f"remote_{self.dataset_remote_name}"
        os.makedirs(server_path, exist_ok=True)
        for filename in os.listdir(self.dataset_path):
            file_path = os.path.join(self.dataset_path, filename)
            if os.path.isfile(file_path):
                shutil.copy(file_path, server_path)

    def clean(self):
        pass


class KaggleUploader(RemoteDatabaseUploader):

    def __init__(self, dataset_remote_name, dataset_path, when):
        self.dataset_remote_name = dataset_remote_name
        self.dataset_path = dataset_path
        self.when = when
        self.api = KaggleApi()
        self.api.authenticate()

    def increase_index(self):
        """
        Download a dataset from Kaggle.

        :param dataset: str, the dataset to download in the format 'owner/dataset-name'
        :param path: str, the path where to save the dataset (default is current directory)
        """

        self.api.dataset_download_cli(
            f"erlichsefi/{self.dataset_remote_name}", file_name="index.json", force=True
        )
        print(f"Dataset '{self.dataset_remote_name}' downloaded successfully")

        with open("index.json", "r") as file:
            index = json.load(file)

        index[max(map(int, index.keys())) + 1] = self.when

        with open(os.path.join(self.dataset_path, "index.json"), "w+") as file:
            json.dump(index, file)

    def upload_to_dataset(self, message):
        self.api.dataset_create_version(
            folder=self.dataset_path,
            version_notes=message,
            delete_old_versions=False,
        )

    def clean(self):
        os.remove("index.json")
