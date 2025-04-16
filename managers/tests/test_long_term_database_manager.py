import os
import json
from unittest.mock import Mock, patch
import pytest
import tempfile
from remotes import DummyFileStorage
from managers.long_term_database_manager import LongTermDatasetManager


def mock_single_file_data(store,file_path,file_types,files_to_process):
    return {
        "store_enum": store,
        "response":{
            "file_was_created": True,
            "file_created_path": file_path,
            "files_to_process": files_to_process,
            "files_types": file_types,
            
            }
        }
    
@pytest.fixture
def mock_db_uploader():
    return Mock()

@pytest.fixture
def sample_manager(mock_db_uploader):
    return LongTermDatasetManager(
        app_folder="/test/app",
        outputs_folder="/test/outputs",
        status_folder="/test/status",
        dataset_remote_name="test_dataset",
        long_term_db_target=mock_db_uploader,
        enabled_scrapers=["scraper1", "scraper2"],
        enabled_file_types=["type1", "type2"]
    )
    
def expected_app_folder_stracture(folder_path):
    os.makedirs(folder_path, exist_ok=True)
    os.makedirs(os.path.join(folder_path, "status"), exist_ok=True)
    
    with open(os.path.join(folder_path, "status", "scraper1.json"), "w") as f:
        f.write("scraper1_logs")
    with open(os.path.join(folder_path, "status", "scraper2.json"), "w") as f:
        f.write("scraper2_logs")
    
    os.makedirs(os.path.join(folder_path, "outputs"), exist_ok=True)
    with open(os.path.join(folder_path, "outputs", "parser-status.json"), "w") as f:
        json.dump([
            mock_single_file_data("store1","/test/outputs/file1.csv","type1",["file1.xml", "file2.xml"]),
            mock_single_file_data("store2","/test/outputs/file2.csv","type2",["file3.xml", "file4.xml"])
            ], f)
    with open(os.path.join(folder_path, "outputs", "file1.csv"), "w") as f:
        f.write("file1_content")
    with open(os.path.join(folder_path, "outputs", "file2.csv"), "w") as f:
        f.write("file2_content")
    
    
@patch('os.listdir')
def test_read_scraper_status_files(mock_listdir, sample_manager):
    mock_listdir.return_value = ["scraper1.json", "scraper2.json"]
    result = sample_manager._read_scraper_status_files()
    
    assert len(result) == 2
    assert result[0]["path"] == "scraper1.json"
    assert result[0]["description"] == "Scraper status file for 'scraper1.json' execution."
    assert result[1]["path"] == "scraper2.json"
    assert result[1]["description"] == "Scraper status file for 'scraper2.json' execution."

@patch('builtins.open')
def test_read_parser_status(mock_open, sample_manager):
    single_file_data = mock_single_file_data("store1","/test/outputs/file1.csv","type1",["file1.xml", "file2.xml"])
    
    
    mock_file = Mock()
    mock_file.read.return_value = json.dumps([single_file_data])
    mock_open.return_value.__enter__.return_value = mock_file

    result = sample_manager._read_parser_status()
    
    assert len(result) == 1
    assert result[0]["path"] == "file1.csv"
    assert result[0]["description"] == "2 XML files from type type1 published by 'store1'"

def test_compose(sample_manager):
    sample_manager.compose()
    sample_manager.remote_database_manager.stage.assert_any_call("/test/outputs")
    sample_manager.remote_database_manager.stage.assert_any_call("/test/status")
    sample_manager.remote_database_manager.increase_index.assert_called_once()

@patch('logging.critical')
def test_upload_failure(mock_critical, sample_manager):
    sample_manager.remote_database_manager.upload_to_dataset.side_effect = Exception("Upload failed")
    
    with pytest.raises(ValueError) as exc_info:
        with patch.object(sample_manager, '_read_parser_status', return_value=[]) as mock_parser_status, \
            patch.object(sample_manager, '_read_scraper_status_files', return_value=[]) as mock_scraper_status:
            sample_manager.upload()
    
    assert "Error uploading file: Upload failed" in str(exc_info.value)
    mock_critical.assert_called_once()

@patch('logging.critical')
def test_upload_success(mock_critical, sample_manager):
    with patch.object(sample_manager, '_read_parser_status', return_value=[]) as mock_parser_status, \
         patch.object(sample_manager, '_read_scraper_status_files', return_value=[]) as mock_scraper_status:
        sample_manager.upload()
    sample_manager.remote_database_manager.upload_to_dataset.assert_called_once()

def test_clean(sample_manager):
    sample_manager.clean()
    sample_manager.remote_database_manager.clean.assert_called_once() 
    
    
def test_integration():
    with tempfile.TemporaryDirectory() as temp_dir:
        expected_app_folder_stracture(temp_dir)
        remote_name = "test_dataset"
        
        manager = LongTermDatasetManager(
            app_folder=temp_dir,
            outputs_folder=os.path.join(temp_dir, "outputs"),
            status_folder=os.path.join(temp_dir, "status"),
            dataset_remote_name=remote_name,
            long_term_db_target=DummyFileStorage,
            enabled_scrapers=["scraper1", "scraper2"],
            enabled_file_types=["type1", "type2"]
        )
        
        manager.compose()
        manager.upload()
        manager.clean()
        
        assert len(os.listdir(temp_dir)) == 0
        assert len(os.listdir(remote_name)) == 6
        
    
    