import os
import glob
import pandas as pd
import shutil
import pytest
import tempfile
from publishers.base import BaseSupermarketDataPublisher
from remotes import DummyFileStorage, DummyDocumentDbUploader
from il_supermarket_scarper import ScraperFactory,DumpFolderNames,FileTypesFilters
from data_models.raw import ScraperStatus,ParserStatus,file_name_to_table
from managers.cache_manager import CacheManager


def validate_scraper_output(data_folder,enabled_scrapers):
    assert os.path.exists(data_folder)
    assert len(os.listdir(data_folder)) == 2
    # status folder
    assert os.path.exists(os.path.join(data_folder, "status"))
    assert len(os.listdir(os.path.join(data_folder, "status"))) == 1
    assert os.path.exists(os.path.join(data_folder, "status", f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json"))
    
    # data folder
    assert os.path.exists(os.path.join(data_folder, DumpFolderNames[enabled_scrapers[0]].value))
    assert len(os.listdir(os.path.join(data_folder, DumpFolderNames[enabled_scrapers[0]].value))) == 1

def validate_state_after_deleted_dump_files(data_folder,outputs_folder,enabled_scrapers):
    assert len(os.listdir(data_folder)) == 1
    
    assert os.path.exists(outputs_folder)
    assert len(os.listdir(outputs_folder)) == 2
    assert os.path.exists(os.path.join(outputs_folder, "parser-status.json"))
    
    assert os.path.exists(os.path.join(data_folder, "status"))
    assert len(os.listdir(os.path.join(data_folder, "status"))) == 1
    assert os.path.exists(os.path.join(data_folder, "status", f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json"))
    
    
def validate_converting_output(data_folder, outputs_folder,enabled_scrapers):
    assert os.path.exists(outputs_folder)
    assert len(os.listdir(outputs_folder)) == 2
    assert os.path.exists(os.path.join(outputs_folder, "parser-status.json"))
    
    downloaded_file = os.listdir(os.path.join(data_folder, DumpFolderNames[enabled_scrapers[0]].value))[0]
    detected_file_type = FileTypesFilters.get_type_from_file(downloaded_file)
    assert os.path.exists(os.path.join(outputs_folder, f"{detected_file_type.name.lower()}_{enabled_scrapers[0].lower()}.csv"))


def validate_state_after_api_update(app_folder,data_folder,outputs_folder,enabled_scrapers,short_term_db_target):
    assert os.path.exists(app_folder)
    
    # dump exist and empty
    assert os.path.exists(data_folder)
    # status folder
    assert len(os.listdir(data_folder)) == 1 
    assert os.path.exists(os.path.join(data_folder, "status"))
    assert len(os.listdir(os.path.join(data_folder, "status"))) == 1
    assert os.path.exists(os.path.join(data_folder, "status", f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json"))
    
    # output folder 
    assert os.path.exists(outputs_folder)
    assert len(os.listdir(outputs_folder)) == 2
    assert os.path.exists(os.path.join(outputs_folder, "parser-status.json"))
    # read the csv file
    csv_file = glob.glob(os.path.join(outputs_folder, "*.csv"))[0]
    df = pd.read_csv(csv_file)
    
    # document_db folder
    assert len(short_term_db_target._get_table_content(ScraperStatus.get_table_name())) == 4
    assert len(short_term_db_target._get_table_content(ParserStatus.get_table_name())) == len(FileTypesFilters) * 1 # limit
    assert len(short_term_db_target._get_table_content(file_name_to_table(csv_file))) == df.shape[0]
    
    # cache
    with CacheManager(app_folder) as cache:
        assert cache.get_last_processed_row(csv_file) == df.shape[0] - 1 # last row index is size -1
   
@pytest.mark.integration
def test_execute_scraping_integration():
    """
    Integration test for the scraping execution.
    
    Note: This test is marked as integration because it would actually invoke
    the ScarpingTask which might do external API calls or other side effects.
    In a CI/CD pipeline, you might want to skip this test with --skip-integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_scraping_")
    
    try:
        # Create a publisher with minimum scraping processes
        enabled_scrapers = ScraperFactory.sample(n=1)
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=1,
            enabled_scrapers=enabled_scrapers,
            limit=1  # Limit to just 1 item for faster tests
        )
        
        # Execute scraping
        publisher._execute_scraping()
        
        # Check if the data folder was created
        validate_scraper_output(publisher.data_folder,enabled_scrapers)

    except Exception as e:
        pytest.fail(f"Scraping function raised an exception: {e}")
    finally:
        # Clean up
        shutil.rmtree(temp_dir)
  
@pytest.mark.integration
def test_execute_converting_integration():
    """
    Integration test for the converting execution.
    
    This test depends on data from scraping, so it would normally be run
    after test_execute_scraping_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_converting_")
    
    try:
        # Create a publisher with minimum processing
        enabled_scrapers = ScraperFactory.sample(n=1)
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=1,
            number_of_parseing_processs=1,
            limit=1,
            enabled_scrapers=enabled_scrapers
        )
        
        # We need to run scraping first to get data to convert
        publisher._execute_scraping()
        
        # Execute converting
        publisher._execute_converting()
        
        # the csv file was created and the parser states
        validate_converting_output(publisher.data_folder,publisher.outputs_folder,enabled_scrapers)
    
        # status didn't changed
        validate_scraper_output(publisher.data_folder,enabled_scrapers)
    
    
    
    except Exception as e:
        pytest.fail(f"Converting function raised an exception: {e}")
    finally:
        # Clean up
        shutil.rmtree(temp_dir)


@pytest.mark.integration
def test_dump_files_clean_integration():
    """
    Integration test for updating the API database.
    
    This test depends on data from converting, so it would normally be run
    after test_execute_converting_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_api_db_")
    
    try:
        # Create a publisher with minimum processing
        enabled_scrapers = ScraperFactory.sample(n=1)
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=1,
            number_of_parseing_processs=1,
            limit=1,
            enabled_scrapers=enabled_scrapers
        )
        
        # We need to run scraping and converting first
        publisher._execute_scraping()
        publisher._execute_converting()
        publisher._clean_all_dump_files()
        
        validate_state_after_deleted_dump_files(publisher.data_folder,publisher.outputs_folder,enabled_scrapers)
        
        # Check if the DummyDocumentDbUploader was updated
        # In a real implementation, we would need to check the actual database state
    except Exception as e:
        pytest.fail(f"API database update function raised an exception: {e}")
    finally:
        # Clean up
        shutil.rmtree(temp_dir)
        
@pytest.mark.integration
def test_update_api_database_integration():
    """
    Integration test for updating the API database.
    
    This test depends on data from converting, so it would normally be run
    after test_execute_converting_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_api_db_")
    
    try:
        # Create a publisher with minimum processing
        enabled_scrapers = ScraperFactory.sample(n=1)
        short_term_db_target = DummyDocumentDbUploader(db_path=temp_dir)
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=1,
            number_of_parseing_processs=1,
            limit=1,
            enabled_scrapers=enabled_scrapers,
            short_term_db_target=short_term_db_target
        )
        
        # We need to run scraping and converting first
        publisher._execute_scraping()
        publisher._execute_converting()
        publisher._clean_all_dump_files()
        publisher._update_api_database()
        
        validate_state_after_api_update(publisher.app_folder,publisher.data_folder,publisher.outputs_folder,enabled_scrapers,short_term_db_target)
        
        
        # Check if the DummyDocumentDbUploader was updated
        # In a real implementation, we would need to check the actual database state

    finally:
        # Clean up
        shutil.rmtree(temp_dir)


@pytest.mark.integration
def test_upload_to_kaggle_integration():
    """
    Integration test for uploading to Kaggle.
    
    This test depends on data from converting, so it would normally be run
    after test_execute_converting_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_kaggle_")
    
    try:
        # Create a publisher with minimum processing
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=1,
            number_of_parseing_processs=1,
            limit=1
        )
        
        # We need to run scraping and converting first
        publisher._execute_scraping()
        publisher._execute_converting()
        
        # Upload to Kaggle
        publisher._upload_to_kaggle()
        
        # Check if the DummyFileStorage was updated
        # In a real implementation, we would need to check the actual Kaggle dataset
    except Exception as e:
        pytest.fail(f"Kaggle upload function raised an exception: {e}")
    finally:
        # Clean up
        shutil.rmtree(temp_dir)


@pytest.mark.integration
def test_upload_and_clean_integration():
    """
    Integration test for uploading to Kaggle and cleaning up.
    
    This test depends on data from converting, so it would normally be run
    after test_execute_converting_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_upload_clean_")
    
    try:
        # Create a publisher with minimum processing
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=1,
            number_of_parseing_processs=1,
            limit=1
        )
        
        # We need to run scraping and converting first
        publisher._execute_scraping()
        publisher._execute_converting()
        
        # Upload to Kaggle and clean
        publisher._upload_and_clean()
        
        # Verify that directories are cleaned
        assert not os.path.exists(publisher.data_folder)
        assert not os.path.exists(publisher.outputs_folder)
        assert not os.path.exists(publisher.status_folder)
    except Exception as e:
        pytest.fail(f"Upload and clean function raised an exception: {e}")
    finally:
        # Clean up anything that might remain
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.integration
def test_end_to_end_process():
    """
    End-to-end functional test for the entire data publishing process.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_e2e_")
    
    try:
        # Create a publisher with minimum processing
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=1,
            number_of_parseing_processs=1,
            limit=1
        )
        
        # Run the entire process
        publisher._execute_scraping()
        publisher._execute_converting()
        publisher._update_api_database()
        publisher._upload_to_kaggle()
        publisher._clean_all_source_data()
        
        # Verify that directories are cleaned
        assert not os.path.exists(publisher.data_folder)
        assert not os.path.exists(publisher.outputs_folder)
        assert not os.path.exists(publisher.status_folder)
    except Exception as e:
        pytest.fail(f"End-to-end process raised an exception: {e}")
    finally:
        # Clean up anything that might remain
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir) 