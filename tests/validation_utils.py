"""
Utility functions for validating the state of the system during and after tests.
Provides validation helpers for scraper output, converter output, and database state.
"""
from il_supermarket_scarper import DumpFolderNames, FileTypesFilters
from il_supermarket_scarper.utils import ScraperStatus as ScraperStatusReport
from data_models.raw_schema import ScraperStatus, ParserStatus, file_name_to_table
from managers.cache_manager import CacheManager
from access.access_layer import AccessLayer
import os
import glob
import pandas as pd


def validate_scraper_output(data_folder, enabled_scrapers):
    """
    Validate the output produced by the scraper.
    
    Args:
        data_folder: Folder containing the scraped data
        enabled_scrapers: List of enabled scrapers
    """
    assert os.path.exists(data_folder), f"Data folder {data_folder} does not exist"
    assert len(os.listdir(data_folder)) == len(enabled_scrapers) + 1, f"Expected One folder per chain + status folder, found {len(os.listdir(data_folder))}"
    # status folder
    assert os.path.exists(os.path.join(data_folder, "status")), f"Status folder does not exist in {data_folder}"
    assert len(os.listdir(os.path.join(data_folder, "status"))) == len(enabled_scrapers), f"Expected scraper status file per chain, found {len(os.listdir(os.path.join(data_folder, 'status')))}"
    
    for scraper in enabled_scrapers:
        status_file = os.path.join(
            data_folder,
            "status",
            f"{DumpFolderNames[scraper].value.lower()}.json",
        )
        assert os.path.exists(status_file), f"Status file {status_file} does not exist"

    # data folder
    for scraper in enabled_scrapers:
        chain_folder = os.path.join(data_folder, DumpFolderNames[scraper].value)
        assert os.path.exists(chain_folder), f"Chain folder {chain_folder} does not exist"
        assert len(os.listdir(chain_folder)) == 1, f"Expected 1 file in chain folder, found {len(os.listdir(chain_folder))}"


def validate_state_after_deleted_dump_files(
    data_folder, outputs_folder, enabled_scrapers
):
    """
    Validate the state of the system after dump files have been deleted.
    
    Args:
        data_folder: Folder that contained the scraped data
        outputs_folder: Folder containing the converted output
        enabled_scrapers: List of enabled scrapers
    """
    assert len(os.listdir(data_folder)) == 1, f"Expected only 1 item in data folder after deletion, found {len(os.listdir(data_folder))}"

    assert os.path.exists(outputs_folder), f"Outputs folder {outputs_folder} does not exist"
    assert len(os.listdir(outputs_folder)) == 2, f"Expected 2 items in outputs folder, found {len(os.listdir(outputs_folder))}"
    assert os.path.exists(os.path.join(outputs_folder, "parser-status.json")), f"parser-status.json not found in {outputs_folder}"

    assert os.path.exists(os.path.join(data_folder, "status")), f"Status folder does not exist in {data_folder}"
    assert len(os.listdir(os.path.join(data_folder, "status"))) == 1, f"Expected 1 status file, found {len(os.listdir(os.path.join(data_folder, 'status')))}"
    
    status_file = os.path.join(
        data_folder,
        "status",
        f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json",
    )
    assert os.path.exists(status_file), f"Status file {status_file} does not exist"


def validate_converting_output(data_folder, outputs_folder, enabled_scrapers):
    """
    Validate the output produced by the converter.
    
    Args:
        data_folder: Folder containing the scraped data
        outputs_folder: Folder containing the converted output
        enabled_scrapers: List of enabled scrapers
    """
    assert os.path.exists(outputs_folder), f"Outputs folder {outputs_folder} does not exist"
    assert len(os.listdir(outputs_folder)) == 2, f"Expected 2 items in outputs folder, found {len(os.listdir(outputs_folder))}"
    assert os.path.exists(os.path.join(outputs_folder, "parser-status.json")), f"parser-status.json not found in {outputs_folder}"

    chain_folder = os.path.join(data_folder, DumpFolderNames[enabled_scrapers[0]].value)
    assert os.path.exists(chain_folder), f"Chain folder {chain_folder} does not exist"
    assert len(os.listdir(chain_folder)) > 0, f"No files found in chain folder {chain_folder}"
    
    downloaded_file = os.listdir(chain_folder)[0]
    detected_file_type = FileTypesFilters.get_type_from_file(downloaded_file.replace("NULL", ""))
    
    output_file = os.path.join(
        outputs_folder,
        f"{detected_file_type.name.lower()}_{enabled_scrapers[0].lower()}.csv",
    )
    assert os.path.exists(output_file), f"Expected output file {output_file} does not exist"


def validate_state_after_api_update(
    app_folder, data_folder, outputs_folder, enabled_scrapers, short_term_db_target
):
    """
    Validate the state of the system after API update.
    
    Args:
        app_folder: Base application folder
        data_folder: Folder containing the scraped data
        outputs_folder: Folder containing the converted output
        enabled_scrapers: List of enabled scrapers
        short_term_db_target: The short-term database target
    """
    assert os.path.exists(app_folder), f"App folder {app_folder} does not exist"

    # dump exist and empty
    assert os.path.exists(data_folder), f"Data folder {data_folder} does not exist"
    # status folder
    assert len(os.listdir(data_folder)) == 1, f"Expected 1 item in data folder, found {len(os.listdir(data_folder))}"
    assert os.path.exists(os.path.join(data_folder, "status")), f"Status folder does not exist in {data_folder}"
    assert len(os.listdir(os.path.join(data_folder, "status"))) == 1, f"Expected 1 status file, found {len(os.listdir(os.path.join(data_folder, 'status')))}"
    
    status_file = os.path.join(
        data_folder,
        "status",
        f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json",
    )
    assert os.path.exists(status_file), f"Status file {status_file} does not exist"

    # output folder
    assert os.path.exists(outputs_folder), f"Outputs folder {outputs_folder} does not exist"
    assert len(os.listdir(outputs_folder)) == 2, f"Expected 2 items in outputs folder, found {len(os.listdir(outputs_folder))}"
    assert os.path.exists(os.path.join(outputs_folder, "parser-status.json")), f"parser-status.json not found in {outputs_folder}"
    
    # read the csv file
    csv_files = glob.glob(os.path.join(outputs_folder, "*.csv"))
    assert len(csv_files) > 0, f"No CSV files found in {outputs_folder}"
    csv_file = csv_files[0]
    df = pd.read_csv(csv_file)

    # document_db folder
    scraper_status_table = ScraperStatus.get_table_name()
    scraper_status_count = len(short_term_db_target.get_table_content(scraper_status_table))
    assert scraper_status_count == 4, f"Expected 4 documents in {scraper_status_table}, found {scraper_status_count}"
    
    parser_status_table = ParserStatus.get_table_name()
    parser_status_count = len(short_term_db_target.get_table_content(parser_status_table))
    expected_parser_count = len(FileTypesFilters) * 1  # limit
    assert parser_status_count == expected_parser_count, f"Expected {expected_parser_count} documents in {parser_status_table}, found {parser_status_count}"
    
    data_table = file_name_to_table(csv_file)
    data_count = len(short_term_db_target.get_table_content(data_table))
    assert data_count == df.shape[0], f"Expected {df.shape[0]} rows in {data_table}, found {data_count}"

    # cache
    with CacheManager(app_folder) as cache:
        last_processed = cache.get_last_processed_row(csv_file)
        expected_last_row = df.shape[0] - 1
        assert last_processed == expected_last_row, f"Expected last processed row to be {expected_last_row}, found {last_processed}"


def validate_long_term_structure(long_term_db_target, stage_folder, enabled_scrapers):
    """
    Validate the structure of the long-term dataset.
    
    Args:
        remote_dataset_path: Path to the remote dataset
        stage_folder: Path to the staging folder
        enabled_scrapers: List of enabled scrapers
    """
    assert long_term_db_target.was_updated_in_last(seconds=10*60), f"Long-term database was not updated in the last 10 minutes"
    
    files = long_term_db_target.list_files()
    assert "index.json" in files, f"index.json not found in long-term database files: {files}"
    assert "parser-status.json" in files, f"parser-status.json not found in long-term database files: {files}"
    
    chain_status_file = f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json"
    assert chain_status_file in files, f"{chain_status_file} not found in long-term database files: {files}"

    csv_files = long_term_db_target.list_files(extension="csv")
    assert len(csv_files) > 0, f"No CSV files found in long-term database"
    
    chain_pattern = f"{enabled_scrapers[0].lower()}.csv"
    found_chain_file = False
    for csv_file in csv_files:
        if chain_pattern in csv_file:
            found_chain_file = True
            break
    assert found_chain_file, f"No CSV files for chain {enabled_scrapers[0]} found in {csv_files}"

    assert not os.path.exists(stage_folder), f"Stage folder {stage_folder} should not exist but does"


def validate_local_structure(app_folder, data_folder, outputs_folder, status_folder):
    """
    Validate that cleanup has been performed correctly.
    
    Args:
        app_folder: Base application folder
        data_folder: Folder containing the scraped data
        outputs_folder: Folder containing the converted output
        status_folder: Folder containing status information
    """
    assert not os.path.exists(data_folder), f"Data folder {data_folder} should not exist after cleanup"
    assert not os.path.exists(outputs_folder), f"Outputs folder {outputs_folder} should not exist after cleanup"
    assert not os.path.exists(status_folder), f"Status folder {status_folder} should not exist after cleanup"

    with CacheManager(app_folder) as cache:
        assert cache.is_empty(), f"Cache should be empty after cleanup"


def validate_short_term_structure(
    short_term_db_target,
    enabled_scrapers,
    num_of_occasions
):
    """
    Validate the structure of the short-term database.
    
    Args:
        short_term_db_target: The short-term database target
        enabled_scrapers: List of enabled scrapers
    """
    
    num_of_documents_in_scraper_status_per_chain = len([ScraperStatusReport.STARTED, ScraperStatusReport.COLLECTED, ScraperStatusReport.DOWNLOADED, ScraperStatusReport.ESTIMATED_SIZE])
    num_of_documents_in_parser_status_per_chain = len(FileTypesFilters)
    
    expected_scraper_status_count = num_of_occasions * num_of_documents_in_scraper_status_per_chain * len(enabled_scrapers)
    scraper_status_table = ScraperStatus.get_table_name()
    actual_scraper_status_count = len(short_term_db_target.get_table_content(scraper_status_table))
    assert actual_scraper_status_count == expected_scraper_status_count, f"Expected {expected_scraper_status_count} documents in {scraper_status_table}, found {actual_scraper_status_count}"
    
    expected_parser_status_count = num_of_occasions * len(enabled_scrapers) * num_of_documents_in_parser_status_per_chain
    parser_status_table = ParserStatus.get_table_name()
    actual_parser_status_count = len(short_term_db_target.get_table_content(parser_status_table))
    assert actual_parser_status_count == expected_parser_status_count, f"Expected {expected_parser_status_count} documents in {parser_status_table}, found {actual_parser_status_count}"
    

def validate_api_scan(
    enabled_scrapers,
    short_term_database_connector,
    long_term_database_connector,
    num_of_expected_files,
):
    """
    Validate the API scan results.
    
    Args:
        enabled_scrapers: List of enabled scrapers
        short_term_database_connector: Connector to the short-term database
        long_term_database_connector: Connector to the long-term database
        num_of_expected_files: Expected number of files
    """
    #
    access_layer = AccessLayer(
        short_term_database_connector=short_term_database_connector,
        long_term_database_connector=long_term_database_connector,
    )
    #
    files = access_layer.list_files(chain=enabled_scrapers[0])
    assert len(files.processed_files) == num_of_expected_files, f"Expected {num_of_expected_files} processed files for chain {enabled_scrapers[0]}, found {len(files.processed_files)}"

    entries_in_short_term_db = 0
    for file in files.processed_files:
        content = access_layer.get_file_content(
            chain=enabled_scrapers[0], file=file.file_name
        )
        entries_in_short_term_db += len(content.rows)

    entries_in_long_term_db = 0
    csv_file = long_term_database_connector.list_files(chain=enabled_scrapers[0].lower(), extension="csv") 
    assert len(csv_file) > 0, f"No CSV files found for chain {enabled_scrapers[0].lower()} in long-term database"
    
    for file in csv_file:
        df = long_term_database_connector.get_file_content(file)
        entries_in_long_term_db += df.shape[0]

    assert entries_in_short_term_db == entries_in_long_term_db, f"Number of entries in short-term DB ({entries_in_short_term_db}) does not match long-term DB ({entries_in_long_term_db})"
