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
    assert os.path.exists(data_folder)
    assert len(os.listdir(data_folder)) == 2
    # status folder
    assert os.path.exists(os.path.join(data_folder, "status"))
    assert len(os.listdir(os.path.join(data_folder, "status"))) == 1
    assert os.path.exists(
        os.path.join(
            data_folder,
            "status",
            f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json",
        )
    )

    # data folder
    assert os.path.exists(
        os.path.join(data_folder, DumpFolderNames[enabled_scrapers[0]].value)
    )
    assert (
        len(
            os.listdir(
                os.path.join(data_folder, DumpFolderNames[enabled_scrapers[0]].value)
            )
        )
        == 1
    )


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
    assert len(os.listdir(data_folder)) == 1

    assert os.path.exists(outputs_folder)
    assert len(os.listdir(outputs_folder)) == 2
    assert os.path.exists(os.path.join(outputs_folder, "parser-status.json"))

    assert os.path.exists(os.path.join(data_folder, "status"))
    assert len(os.listdir(os.path.join(data_folder, "status"))) == 1
    assert os.path.exists(
        os.path.join(
            data_folder,
            "status",
            f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json",
        )
    )


def validate_converting_output(data_folder, outputs_folder, enabled_scrapers):
    """
    Validate the output produced by the converter.
    
    Args:
        data_folder: Folder containing the scraped data
        outputs_folder: Folder containing the converted output
        enabled_scrapers: List of enabled scrapers
    """
    assert os.path.exists(outputs_folder)
    assert len(os.listdir(outputs_folder)) == 2
    assert os.path.exists(os.path.join(outputs_folder, "parser-status.json"))

    downloaded_file = os.listdir(
        os.path.join(data_folder, DumpFolderNames[enabled_scrapers[0]].value)
    )[0]
    detected_file_type = FileTypesFilters.get_type_from_file(downloaded_file)
    assert os.path.exists(
        os.path.join(
            outputs_folder,
            f"{detected_file_type.name.lower()}_{enabled_scrapers[0].lower()}.csv",
        )
    )


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
    assert os.path.exists(app_folder)

    # dump exist and empty
    assert os.path.exists(data_folder)
    # status folder
    assert len(os.listdir(data_folder)) == 1
    assert os.path.exists(os.path.join(data_folder, "status"))
    assert len(os.listdir(os.path.join(data_folder, "status"))) == 1
    assert os.path.exists(
        os.path.join(
            data_folder,
            "status",
            f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json",
        )
    )

    # output folder
    assert os.path.exists(outputs_folder)
    assert len(os.listdir(outputs_folder)) == 2
    assert os.path.exists(os.path.join(outputs_folder, "parser-status.json"))
    # read the csv file
    csv_file = glob.glob(os.path.join(outputs_folder, "*.csv"))[0]
    df = pd.read_csv(csv_file)

    # document_db folder
    assert (
        len(short_term_db_target._get_table_content(ScraperStatus.get_table_name()))
        == 4
    )
    assert (
        len(short_term_db_target._get_table_content(ParserStatus.get_table_name()))
        == len(FileTypesFilters) * 1
    )  # limit
    assert (
        len(short_term_db_target._get_table_content(file_name_to_table(csv_file)))
        == df.shape[0]
    )

    # cache
    with CacheManager(app_folder) as cache:
        assert (
            cache.get_last_processed_row(csv_file) == df.shape[0] - 1
        )  # last row index is size -1


def validate_long_term_structure(remote_dataset_path, stage_folder, enabled_scrapers):
    """
    Validate the structure of the long-term dataset.
    
    Args:
        remote_dataset_path: Path to the remote dataset
        stage_folder: Path to the staging folder
        enabled_scrapers: List of enabled scrapers
    """
    assert os.path.exists(remote_dataset_path)
    assert os.path.exists(os.path.join(remote_dataset_path, "index.json"))
    assert os.path.exists(os.path.join(remote_dataset_path, "parser-status.json"))
    assert os.path.exists(
        os.path.join(
            remote_dataset_path,
            f"{DumpFolderNames[enabled_scrapers[0]].value.lower()}.json",
        )
    )

    for csv_file in glob.glob(os.path.join(remote_dataset_path, "*.csv")):
        assert f"{enabled_scrapers[0].lower()}.csv" in csv_file

    assert not os.path.exists(stage_folder)


def validate_cleanup(app_folder, data_folder, outputs_folder, status_folder):
    """
    Validate that cleanup has been performed correctly.
    
    Args:
        app_folder: Base application folder
        data_folder: Folder containing the scraped data
        outputs_folder: Folder containing the converted output
        status_folder: Folder containing status information
    """
    assert not os.path.exists(data_folder)
    assert not os.path.exists(outputs_folder)
    assert not os.path.exists(status_folder)

    with CacheManager(app_folder) as cache:
        assert cache.is_empty()


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
    assert len(short_term_db_target._get_table_content(ScraperStatus.get_table_name())) == num_of_occasions*num_of_documents_in_scraper_status_per_chain*len(enabled_scrapers)
    assert len(short_term_db_target._get_table_content(ParserStatus.get_table_name())) == num_of_occasions * len(enabled_scrapers) * num_of_documents_in_parser_status_per_chain
    

def validate_api_scan(
    enabled_scrapers,
    short_term_database_connector,
    long_term_database_connector,
    num_of_expected_files,
    long_term_remote_dataset_path,
):
    """
    Validate the API scan results.
    
    Args:
        enabled_scrapers: List of enabled scrapers
        short_term_database_connector: Connector to the short-term database
        long_term_database_connector: Connector to the long-term database
        num_of_expected_files: Expected number of files
        long_term_remote_dataset_path: Path to the long-term remote dataset
    """
    #
    access_layer = AccessLayer(
        short_term_database_connector=short_term_database_connector,
        long_term_database_connector=long_term_database_connector,
    )
    #
    files = access_layer.list_files(chain=enabled_scrapers[0])
    assert len(files.processed_files) == num_of_expected_files

    entries_in_short_term_db = 0
    for file in files.processed_files:
        content = access_layer.get_file_content(
            chain=enabled_scrapers[0], file=file.file_name
        )
        entries_in_short_term_db += len(content.rows)

    entries_in_long_term_db = 0
    csv_file = glob.glob(os.path.join(long_term_remote_dataset_path, "*.csv"))
    for file in csv_file:
        df = pd.read_csv(file)
        entries_in_long_term_db += df.shape[0]

    assert entries_in_short_term_db == entries_in_long_term_db
