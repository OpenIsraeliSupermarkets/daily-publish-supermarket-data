# test_daliy_raw_dump.py
import unittest
from unittest.mock import patch
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
        # arrange
        num_of_occasions = 3
        
        self.publisher = SupermarketDataPublisher(
            enabled_scrapers=ScraperFactory.sample(1),
            limit=1,
            completed_by=datetime.datetime.now() + datetime.timedelta(minutes=num_of_occasions),
            num_of_occasions=num_of_occasions
        )
        self.publisher._clean_folders()
        # Act
        self.publisher.run()

        # Assert
        mock_upload_to_dataset.assert_called_once()
        mock_clean_folders.assert_called_once()
        mock_increase_index.assert_called_once()
        
        self.publisher._clean_folders()


if __name__ == "__main__":
    unittest.main()
