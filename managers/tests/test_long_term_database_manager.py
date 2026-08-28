import os
import json
from unittest.mock import Mock, patch
import pytest
import tempfile
from remotes import DummyFileStorage
from managers.long_term_database_manager import LongTermDatasetManager
from utils import now


def mock_single_file_data(store, file_path, file_types, files_to_process):
    return {
        "store_enum": store,
        "response": {
            "file_was_created": True,
            "file_created_path": file_path,
            "files_to_process": files_to_process,
            "files_types": file_types,
        },
    }


@pytest.fixture
def mock_db_uploader():
    return Mock()


@pytest.fixture
def sample_manager(mock_db_uploader):
    return LongTermDatasetManager(
        outputs_folder="/test/outputs",
        long_term_db_target=mock_db_uploader,
        enabled_scrapers=["scraper1", "scraper2"],
        enabled_file_types=["type1", "type2"],
        scraping_status_folder="/test/scraping_status",
        converting_status_folder="/test/converting_status",
    )


def expected_app_folder_stracture(folder_path):
    os.makedirs(folder_path, exist_ok=True)
    os.makedirs(os.path.join(folder_path, "scraping_status"), exist_ok=True)
    os.makedirs(os.path.join(folder_path, "converting_status"), exist_ok=True)

    with open(os.path.join(folder_path, "scraping_status", "scraper1.json"), "w") as f:
        f.write("scraper1_logs")
    with open(os.path.join(folder_path, "scraping_status", "scraper2.json"), "w") as f:
        f.write("scraper2_logs")

    os.makedirs(os.path.join(folder_path, "outputs"), exist_ok=True)
    with open(
        os.path.join(folder_path, "converting_status", "scraper_file_type1.json"), "w"
    ) as f:
        f.write("scraper1_parser_logs")
    with open(
        os.path.join(folder_path, "converting_status", "scraper_file_type2.json"), "w"
    ) as f:
        f.write("scraper2_parser_logs")

    with open(os.path.join(folder_path, "outputs", "file1.csv"), "w") as f:
        f.write("file1_content")
    with open(os.path.join(folder_path, "outputs", "file2.csv"), "w") as f:
        f.write("file2_content")


@patch("os.listdir")
def test_read_scraper_status_files(mock_listdir, sample_manager):
    mock_listdir.return_value = ["scraper1.json", "scraper2.json"]
    result = sample_manager._read_scraper_status_files()

    assert len(result) == 2
    assert result[0]["path"] == "scraper1.json"
    assert (
        result[0]["description"] == "Scraper status file for 'scraper1.json' execution."
    )
    assert result[1]["path"] == "scraper2.json"
    assert (
        result[1]["description"] == "Scraper status file for 'scraper2.json' execution."
    )


def test_compose(sample_manager):
    sample_manager.compose()
    sample_manager.remote_database_manager.stage.assert_any_call("/test/outputs")
    sample_manager.remote_database_manager.stage.assert_any_call(
        "/test/scraping_status"
    )
    sample_manager.remote_database_manager.stage.assert_any_call(
        "/test/converting_status"
    )
    sample_manager.remote_database_manager.increase_index.assert_called_once()
    sample_manager.remote_database_manager.pack_staged_files.assert_called_once()


def test_compose_stages_quality_folder(sample_manager):
    sample_manager.quality_folder = "/test/quality"
    with patch("os.path.isdir", return_value=True):
        sample_manager.compose()
    sample_manager.remote_database_manager.stage.assert_any_call("/test/quality")


@patch("utils.logging_config.Logger.critical")
def test_upload_failure(mock_critical, sample_manager):
    sample_manager.remote_database_manager.upload_to_dataset.side_effect = Exception(
        "Upload failed"
    )

    with pytest.raises(ValueError) as exc_info:
        with patch.object(
            sample_manager, "_read_parser_status", return_value=[]
        ) as mock_parser_status, patch.object(
            sample_manager, "_read_scraper_status_files", return_value=[]
        ) as mock_scraper_status:
            sample_manager.upload()

    assert "Error uploading file: Upload failed" in str(exc_info.value)
    mock_critical.assert_called_once()


@patch("logging.critical")
def test_upload_success(mock_critical, sample_manager):
    with patch.object(
        sample_manager, "_read_parser_status", return_value=[]
    ) as mock_parser_status, patch.object(
        sample_manager, "_read_scraper_status_files", return_value=[]
    ) as mock_scraper_status:
        sample_manager.upload()
    sample_manager.remote_database_manager.upload_to_dataset.assert_called_once()


def test_clean(sample_manager):
    sample_manager.clean()
    sample_manager.remote_database_manager.clean.assert_called_once()


def test_integration():
    with tempfile.TemporaryDirectory() as temp_dir:
        expected_app_folder_stracture(temp_dir)
        quality_dir = os.path.join(temp_dir, "quality")
        os.makedirs(quality_dir, exist_ok=True)
        with open(os.path.join(quality_dir, "scraper_quality.json"), "w") as f:
            json.dump({"iterations": []}, f)
        with open(os.path.join(quality_dir, "parser_quality.json"), "w") as f:
            json.dump({"iterations": []}, f)
        with open(os.path.join(quality_dir, "pipeline_health.json"), "w") as f:
            json.dump({"overall_healthy": True, "scraper": {}, "parser": {}}, f)

        remote_name = os.path.join(temp_dir, "test_dataset")

        manager = LongTermDatasetManager(
            outputs_folder=os.path.join(temp_dir, "outputs"),
            long_term_db_target=DummyFileStorage(
                dataset_remote_path=remote_name,
                dataset_path=os.path.join(temp_dir, "dataset"),
                when=now(),
            ),
            enabled_scrapers=["scraper1", "scraper2"],
            enabled_file_types=["type1", "type2"],
            scraping_status_folder=os.path.join(temp_dir, "scraping_status"),
            converting_status_folder=os.path.join(temp_dir, "converting_status"),
            quality_folder=quality_dir,
        )

        manager.compose()
        manager.upload()
        manager.clean()

        assert len(os.listdir(temp_dir)) == 1
        remote_files = sorted(os.listdir(remote_name))
        assert remote_files == sorted(
            [
                "index.json",
                "parser_quality.json",
                "pipeline_health.json",
                "scraper_quality.json",
                "misc.zip",
            ]
        )
