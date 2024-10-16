# test_daliy_raw_dump.py
import unittest
from unittest.mock import patch
import os
import datetime
from daliy_raw_dump import SupermarketDataPublisher
from il_supermarket_scarper.scrappers_factory import ScraperFactory


class TestSupermarketDataPublisher(unittest.TestCase):
        

    @patch("daliy_raw_dump.KaggleDatasetManager")
    def test_upload_to_kaggle_with_limited_scrapers_and_files(
        self, MockKaggleDatasetManager
    ):
        # Arrange
        num_of_occasions = 3
        
        self.publisher = SupermarketDataPublisher(
            enabled_scrapers = ScraperFactory.sample(1),
            limit = 1,
            completed_by = datetime.datetime.now() + datetime.timedelta(minutes=num_of_occasions),
            num_of_occasions=num_of_occasions
        )
        self.publisher._clean_folders()
        # Act
        self.publisher.run()

        # Assert
        mock_manager = MockKaggleDatasetManager.return_value
        mock_manager.compose.assert_called_once_with(
            outputs_folder=self.publisher.outputs_folder,
            status_folder=self.publisher.status_folder,
        )
        mock_manager.upload_to_dataset.assert_called_once()
        mock_manager.clean.assert_called_once()


if __name__ == "__main__":
    unittest.main()
