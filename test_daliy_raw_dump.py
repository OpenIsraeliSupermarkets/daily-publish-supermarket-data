# test_daliy_raw_dump.py
import unittest
from unittest.mock import patch, MagicMock
import os
import datetime
from daliy_raw_dump import SupermarketDataPublisher
from il_supermarket_scarper.scrappers_factory import ScraperFactory


class TestSupermarketDataPublisher(unittest.TestCase):

    @patch.dict(os.environ, {"TZ": "Asia/Jerusalem"})
    def setUp(self):
        self.publisher = SupermarketDataPublisher()

    def test_initialization(self):
        self.assertEqual(self.publisher.number_of_processes, 4)
        self.assertEqual(self.publisher.data_folder, "app_data/dumps")
        self.assertEqual(self.publisher.outputs_folder, "app_data/outputs")
        self.assertEqual(self.publisher.status_folder, "app_data/dumps/status")
        self.assertEqual(self.publisher.occasions, ["12:00", "17:30", "23:00"])
        self.assertEqual(self.publisher.executed_jobs, 0)
        self.assertIsInstance(self.publisher.today, datetime.datetime)

    @patch("daliy_raw_dump.ScarpingTask")
    def test_run_scraping(self, MockScarpingTask):
        mock_task = MockScarpingTask.return_value
        self.publisher.run_scraping()
        mock_task.start.assert_called_once()
        self.assertEqual(self.publisher.executed_jobs, 1)

    @patch("daliy_raw_dump.schedule")
    def test_setup_schedule(self, mock_schedule):
        self.publisher._setup_schedule()
        for occasion in self.publisher.occasions:
            mock_schedule.every().day.at(occasion).do.assert_called_with(
                self.publisher.run_scraping
            )

    @patch("daliy_raw_dump.schedule")
    @patch("daliy_raw_dump.time.sleep", return_value=None)
    def test_execute_scraping(self, mock_sleep, mock_schedule):
        mock_schedule.run_pending.side_effect = lambda: setattr(
            self.publisher, "executed_jobs", len(self.publisher.occasions)
        )
        self.publisher._execute_scraping()
        mock_schedule.run_pending.assert_called()
        mock_sleep.assert_called()

    @patch("daliy_raw_dump.ConvertingTask")
    def test_execute_converting(self, MockConvertingTask):
        mock_task = MockConvertingTask.return_value
        self.publisher._execute_converting()
        mock_task.start.assert_called_once()

    @patch("daliy_raw_dump.KaggleDatasetManager")
    def test_upload_to_kaggle(self, MockKaggleDatasetManager):
        mock_manager = MockKaggleDatasetManager.return_value
        self.publisher._upload_to_kaggle()
        mock_manager.compose.assert_called_once_with(
            outputs_folder=self.publisher.outputs_folder,
            status_folder=self.publisher.status_folder,
        )
        mock_manager.upload_to_dataset.assert_called_once()
        mock_manager.clean.assert_called_once()

    @patch("daliy_raw_dump.shutil.rmtree")
    @patch("daliy_raw_dump.os.path.exists", return_value=True)
    def test_clean_folders(self, mock_exists, mock_rmtree):
        self.publisher._clean_folders()
        self.assertEqual(mock_rmtree.call_count, 3)

    @patch.object(SupermarketDataPublisher, "_clean_folders")
    @patch.object(SupermarketDataPublisher, "_upload_to_kaggle")
    @patch.object(SupermarketDataPublisher, "_execute_converting")
    @patch.object(SupermarketDataPublisher, "_execute_scraping")
    @patch.object(SupermarketDataPublisher, "_setup_schedule")
    def test_run(
        self,
        mock_setup_schedule,
        mock_execute_scraping,
        mock_execute_converting,
        mock_upload_to_kaggle,
        mock_clean_folders,
    ):
        self.publisher.run()
        mock_setup_schedule.assert_called_once()
        mock_execute_scraping.assert_called_once()
        mock_execute_converting.assert_called_once()
        mock_upload_to_kaggle.assert_called_once()
        mock_clean_folders.assert_called_once()

    @patch("daliy_raw_dump.KaggleDatasetManager")
    def test_upload_to_kaggle_with_limited_scrapers_and_files(
        self, MockKaggleDatasetManager
    ):
        # Arrange
        mock_manager = MockKaggleDatasetManager.return_value
        self.publisher.enabled_scrapers = ScraperFactory.sample(1)
        self.publisher.enabled_file_types = None
        self.publisher.limit = 1
        current_time = datetime.datetime.now()
        self.publisher.occasions = [
            (current_time + datetime.timedelta(minutes=i*2)).strftime("%H:%M")
            for i in range(3)
        ]

        # Act
        self.publisher.run()

        # Assert
        mock_manager.compose.assert_called_once_with(
            outputs_folder=self.publisher.outputs_folder,
            status_folder=self.publisher.status_folder,
        )
        mock_manager.upload_to_dataset.assert_called_once()
        mock_manager.clean.assert_called_once()


if __name__ == "__main__":
    unittest.main()
