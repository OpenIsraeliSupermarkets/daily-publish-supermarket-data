from il_supermarket_scarper import DumpFolderNames,FileTypesFilters
from data_models.raw_schema import ScraperStatus,ParserStatus,file_name_to_table
from managers.cache_manager import CacheManager
from access.access_layer import AccessLayer
import os
import glob
import pandas as pd


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
   

def validate_long_term_structure(remote_dataset_path,stage_folder,enabled_scrapers):
    assert os.path.exists(remote_dataset_path)
    assert len(os.listdir(remote_dataset_path)) == 4
    assert os.path.exists(os.path.join(remote_dataset_path, "index.json"))
    assert os.path.exists(os.path.join(remote_dataset_path, "parser-status.json"))
    assert os.path.exists(os.path.join(remote_dataset_path, f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json"))
    csv_file = glob.glob(os.path.join(remote_dataset_path, "*.csv"))[0]
    
    assert not os.path.exists(stage_folder)
    
    assert f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.csv" in csv_file


def validate_cleanup(app_folder,data_folder,outputs_folder,status_folder):
    assert not os.path.exists(data_folder)
    assert not os.path.exists(outputs_folder)
    assert not os.path.exists(status_folder)
    
    with CacheManager(app_folder) as cache:
        assert cache.is_empty()


def validate_api_scan(enabled_scrapers,short_term_database_connector,long_term_database_connector,num_of_expected_files):
    #
    access_layer = AccessLayer(
        short_term_database_connector=short_term_database_connector,
        long_term_database_connector=long_term_database_connector
    )
    #
    files = access_layer.list_files(chain=enabled_scrapers[0])
    assert len(files.processed_files) == num_of_expected_files
    
    for file in files.processed_files:
        content = access_layer.get_file_content(chain=enabled_scrapers[0], file=file.file_name)
        assert len(content.rows) > 0
    