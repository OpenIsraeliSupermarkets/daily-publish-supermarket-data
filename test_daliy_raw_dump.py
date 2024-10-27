# test_daliy_raw_dump.py
import unittest
from unittest.mock import patch
import shutil
import os
import datetime
from daliy_raw_dump import SupermarketDataPublisher
from il_supermarket_scarper.scrappers_factory import ScraperFactory


class TestSupermarketDataPublisher(unittest.TestCase):

    @patch("daliy_raw_dump.KaggleDatasetManager.clean")
    @patch("daliy_raw_dump.KaggleDatasetManager.upload_to_dataset")
    @patch("daliy_raw_dump.KaggleDatasetManager.increase_index")
    def test_upload_to_kaggle_with_limited_scrapers_and_files(
        self, mock_upload_to_dataset, mock_clean_folders, mock_increase_index
    ):
        # params
        num_of_occasions = 3
        file_per_run = 1
        app_folder = "app_data"
        data_folder = "dumps"

        # run the process for couple of times
        self.publisher = SupermarketDataPublisher(
            app_folder=app_folder,
            data_folder=data_folder,
            enabled_scrapers=ScraperFactory.sample(1),
            limit=file_per_run,
            start_at=datetime.datetime.now(),
            completed_by=datetime.datetime.now()
            + datetime.timedelta(minutes=num_of_occasions),
            num_of_occasions=num_of_occasions,
        )
        self.publisher.run()

        # make sure the kaggle calls was called
        mock_upload_to_dataset.assert_called_once()
        mock_clean_folders.assert_called_once()
        mock_increase_index.assert_called_once()

        # make sure we've downloaded the requested amont of files
        data_folder = os.path.join(app_folder, data_folder)
        folders = os.listdir(data_folder)
        assert "status" in folders, "status folder should be created"
        folders.remove("status")
        selected_chain = folders[0]
        #
        assert (
            len(os.listdir(os.path.join(data_folder, selected_chain)))
            == num_of_occasions * file_per_run
        ), "should download the requested amount of files"

        # clean data
        self.publisher._clean_folders()

        # clean data that we would've upload to kaggle
        shutil.rmtree("israeli-supermarkets-2024")

    @patch("daliy_raw_dump.KaggleDatasetManager.upload_to_dataset")
    def test_upload_to_kaggle(self, upload_to_dataset_mock):
        # params
        num_of_occasions = 3
        file_per_run = 1
        app_folder = "app_data"
        data_folder = "dumps"

        os.makedirs(f"{app_folder}/outputs", exist_ok=True)
        os.makedirs(f"{app_folder}/{data_folder}/status", exist_ok=True)

        # os.mkdir("app_data")
        self.publisher = SupermarketDataPublisher(
            app_folder=app_folder,
            data_folder=data_folder,
            enabled_scrapers=ScraperFactory.sample(1),
            limit=file_per_run,
            start_at=datetime.datetime.now(),
            completed_by=datetime.datetime.now()
            + datetime.timedelta(minutes=num_of_occasions),
            num_of_occasions=num_of_occasions,
        )
        self.publisher._upload_to_kaggle()


if __name__ == "__main__":
    unittest.main()
