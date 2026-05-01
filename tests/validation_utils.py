"""
Utility functions for validating the state of the system during and after tests.
Provides validation helpers for scraper output, converter output, and database state.
"""

from il_supermarket_scarper import DumpFolderNames, FileTypesFilters, ScraperStatusOutput
from il_supermarket_parsers import ParserStatusOutput
from data_models.raw_schema import ScraperStatus, ParserStatus, file_name_to_table
from managers.cache_manager import CacheManager
from access.access_layer import AccessLayer
import os
import glob
import pandas as pd
from il_supermarket_scarper import ScraperFactory


def scrapers_to_test():
    """
    Since the system test is running externally to israel, some scrapers are not available.
    This function returns the list of scrapers to test that should be available.
    """
    return [
        ScraperFactory.BAREKET.name,
        ScraperFactory.WOLT.name,
        # ScraperFactory.COFIX.name,
    ]


def validate_scraper_output(
    data_folder, scraping_status_folder, enabled_scrapers, dump_files_deleted=False
):
    """
    Validate the output produced by the scraper.

    Args:
        data_folder: Folder containing the scraped data
        enabled_scrapers: List of enabled scrapers
    """
    assert os.path.exists(data_folder), f"Data folder {data_folder} does not exist"
    # status folder
    assert os.path.exists(
        scraping_status_folder
    ), f"Status folder does not exist in {data_folder}"

    assert len(os.listdir(scraping_status_folder)) == len(
        enabled_scrapers
    ), f"Expected scraper status file per chain, found {len(os.listdir(os.path.join(data_folder, 'status')))}"

    for scraper in enabled_scrapers:
        status_file = os.path.join(
            scraping_status_folder,
            f"{DumpFolderNames[scraper].value.lower()}.json",
        )
        assert os.path.exists(status_file), f"Status file {status_file} does not exist"

    # data folder
    if not dump_files_deleted:
        assert len(os.listdir(data_folder)) == len(
            enabled_scrapers
        ), f"Expected One folder per chain + status folder, found {len(os.listdir(data_folder))}"

        for scraper in enabled_scrapers:
            chain_folder = os.path.join(data_folder, DumpFolderNames[scraper].value)
            assert os.path.exists(
                chain_folder
            ), f"Chain folder {chain_folder} does not exist"
            assert (
                len(os.listdir(chain_folder)) == 1
            ), f"Expected 1 file in chain folder, found {len(os.listdir(chain_folder))}"
            assert os.listdir(chain_folder)[0].endswith(
                ".xml"
            ), f"Expected XML file, found {os.listdir(chain_folder)[0]}"
    else:
        assert (
            len(os.listdir(data_folder)) == 0
        ), f"Expected no folders in data folder, found {len(os.listdir(data_folder))}"


def validate_converting_output(
    data_folder,
    converting_status_folder,
    outputs_folder,
    enabled_scrapers,
    dump_files_deleted=False,
):
    """
    Validate the output produced by the converter.

    Args:
        data_folder: Folder containing the scraped data
        outputs_folder: Folder containing the converted output
        enabled_scrapers: List of enabled scrapers
    """

    assert os.path.exists(
        converting_status_folder
    ), f"Converting status folder {converting_status_folder} does not exist"

    assert len(os.listdir(converting_status_folder)) == len(enabled_scrapers) * len(
        FileTypesFilters
    ), f"Expected scraper status file per chain, found {len(os.listdir(converting_status_folder))}"

    assert os.path.exists(
        outputs_folder
    ), f"Outputs folder {outputs_folder} does not exist"

    assert len(os.listdir(outputs_folder)) == len(
        enabled_scrapers
    ), f"Expected csv files per chain + parser-status.json, found {len(os.listdir(outputs_folder))}"

    if not dump_files_deleted:
        for scraper in enabled_scrapers:
            # find the source file
            chain_folder = os.path.join(data_folder, DumpFolderNames[scraper].value)
            assert os.path.exists(
                chain_folder
            ), f"Chain folder {chain_folder} does not exist"
            assert (
                len(os.listdir(chain_folder)) == 1
            ), "We are expecting only one file per chain"

            downloaded_file = os.listdir(chain_folder)[0]
            # validate that a file was created
            detected_file_type = FileTypesFilters.get_type_from_file(
                downloaded_file.replace("NULL", "")
            )

            output_file = os.path.join(
                outputs_folder,
                f"{detected_file_type.name.lower()}_{scraper.lower()}.csv",
            )
            assert os.path.exists(
                output_file
            ), f"Expected output file {output_file} does not exist. {downloaded_file}. {chain_folder}"


def validate_state_after_api_update(
    app_folder, outputs_folder, enabled_scrapers, short_term_db_target
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
    # document_db folder
    scraper_status_table = ScraperStatus.get_table_name()
    scraper_status_count = len(
        short_term_db_target.get_destinations_content(scraper_status_table)
    )
    expected_scraper_count = 4 * len(enabled_scrapers)
    assert scraper_status_count == expected_scraper_count, (
        f"Expected {expected_scraper_count} documents in {scraper_status_table}, "
        f"found {scraper_status_count}"
    )

    parser_status_table = ParserStatus.get_table_name()
    parser_status_count = len(
        short_term_db_target.get_destinations_content(parser_status_table)
    )
    expected_parser_count = (
        1 * len(enabled_scrapers) * 2
    )  # start -> (failed or completed)
    assert (
        parser_status_count == expected_parser_count
    ), f"Expected {expected_parser_count} documents in {parser_status_table}, found {parser_status_count}"

    # # read the csv file
    csv_files = glob.glob(os.path.join(outputs_folder, "*.csv"))
    assert len(csv_files) == len(
        enabled_scrapers
    ), f"Expected {len(enabled_scrapers)} CSV files, found {len(csv_files)}"
    assert len(csv_files) > 0, f"No CSV files found in {outputs_folder}"
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)

        data_table = file_name_to_table(csv_file)
        table_docs = short_term_db_target.get_destinations_content(data_table)
        # LargeFilePushManager also inserts file_complete markers without row data
        data_count = len([d for d in table_docs if isinstance(d.get("content"), dict)])
        assert (
            data_count == df.shape[0]
        ), f"Expected {df.shape[0]} rows in {data_table}, found {data_count}"

        # cache
        with CacheManager(app_folder) as cache:
            last_processed = cache.get_last_processed_row(csv_file)
            expected_last_row = df.shape[0] - 1
            assert (
                last_processed == expected_last_row
            ), f"Expected last processed row to be {expected_last_row}, found {last_processed}"


def validate_long_term_structure(
    long_term_db_target, stage_folder, enabled_scrapers, in_app=True
):
    """
    Validate the structure of the long-term dataset.

    Args:
        remote_dataset_path: Path to the remote dataset
        stage_folder: Path to the staging folder
        enabled_scrapers: List of enabled scrapers
    """
    assert long_term_db_target.was_updated_in_last(
        seconds=60 * 60 * 24
    ), f"Long-term database was not updated in the last 24 hours"

    files = long_term_db_target.list_files()
    assert (
        "index.json" in files
    ), f"index.json not found in long-term database files: {files}"

    for scraper in enabled_scrapers:
        chain_status_file = f"{DumpFolderNames[scraper].value.lower()}.json"
        assert (
            chain_status_file in files
        ), f"{chain_status_file} not found in long-term database files: {files}"

    for scraper in enabled_scrapers:
        for file_type in FileTypesFilters:
            file_type_file = f"{scraper.lower()}_{file_type.name.lower()}.json"
            assert (
                file_type_file in files
            ), f"{file_type_file} not found in long-term database files: {files}"

    csv_files = long_term_db_target.list_files(extension="csv")
    assert len(csv_files) > 0, f"No CSV files found in long-term database"

    for scraper in enabled_scrapers:
        chain_pattern = f"{scraper.lower()}.csv"
        found_chain_file = False
        for csv_file in csv_files:
            if chain_pattern in csv_file:
                found_chain_file = True
                break
        assert (
            found_chain_file
        ), f"No CSV files for chain {scraper} found in {csv_files}"

    if in_app:
        assert not os.path.exists(
            stage_folder
        ), f"Stage folder {stage_folder} should not exist but does"


def validate_local_structure_deleted(
    app_folder, data_folder, outputs_folder, status_folder
):
    """
    Validate that cleanup has been performed correctly.

    Args:
        app_folder: Base application folder
        data_folder: Folder containing the scraped data
        outputs_folder: Folder containing the converted output
        status_folder: Folder containing status information
    """
    assert not os.path.exists(
        data_folder
    ), f"Data folder {data_folder} should not exist after cleanup"
    assert not os.path.exists(
        outputs_folder
    ), f"Outputs folder {outputs_folder} should not exist after cleanup"
    assert not os.path.exists(
        status_folder
    ), f"Status folder {status_folder} should not exist after cleanup"

    with CacheManager(app_folder) as cache:
        assert cache.is_empty(), f"Cache should be empty after cleanup"


def validate_short_term_structure(
    short_term_db_target,
    enabled_scrapers,
    num_of_occasions=None,
):
    """
    Validate the structure of the short-term database.

    Collects all published records from the four DB tables, reconstructs the
    canonical ScraperStatusOutput / ParserStatusOutput objects per task_id, and
    delegates lifecycle validation to each model's built-in method.

    Args:
        short_term_db_target: The short-term database target
        enabled_scrapers: List of enabled scrapers
        num_of_occasions: Expected number of scraping occasions (None = any)
    """
    # ------------------------------------------------------------------ #
    # Scraper                                                              #
    # ------------------------------------------------------------------ #
    scraper_events_docs = short_term_db_target.get_destinations_content(
        ScraperStatus.get_table_name()
    )
    scraper_global_docs = short_term_db_target.get_destinations_content(
        "GlobalScraperStatus"
    )

    assert len(scraper_events_docs) > 0, "Expected at least one ScraperStatus document."
    assert (
        len(scraper_global_docs) > 0
    ), "Expected at least one GlobalScraperStatus document."

    # group by task_id — one group per chain per occasion
    scraper_by_task: dict = {}
    for doc in scraper_global_docs:
        tid = doc["task_id"]
        scraper_by_task.setdefault(tid, {"events": [], "global_status": []})
        payload = {k: v for k, v in doc.items() if k != "index"}
        scraper_by_task[tid]["global_status"].append(payload)
        
    for doc in scraper_events_docs:
        tid = doc["task_id"]
        payload = {k: v for k, v in doc.items() if k != "index"}
        scraper_by_task[tid]["events"].append(payload)

    # all scrapers ran at least once
    assert len(scraper_by_task) >= len(
        enabled_scrapers
    ), f"Expected at least {len(enabled_scrapers)} scraper tasks, found {len(scraper_by_task)}"

    # each task forms a valid lifecycle
    for tid, parts in scraper_by_task.items():
        model = ScraperStatusOutput(
            events=parts["events"],
            global_status=parts["global_status"],
        )
        assert model.validate_file_status(), (
            f"ScraperStatusOutput invalid for task {tid}"
        )

    if num_of_occasions is not None:
        expected_tasks = len(enabled_scrapers) * num_of_occasions
        assert len(scraper_by_task) == expected_tasks, (
            f"Expected {expected_tasks} scraper tasks, found {len(scraper_by_task)}"
        )

    assert short_term_db_target._is_collection_updated(
        ScraperStatus.get_table_name(), seconds=60 * 60 * 3
    ), "ScraperStatus should be updated in the last 3 hours"
    assert short_term_db_target._is_collection_updated(
        "GlobalScraperStatus", seconds=60 * 60 * 3
    ), "GlobalScraperStatus should be updated in the last 3 hours"

    # ------------------------------------------------------------------ #
    # Parser                                                               #
    # ------------------------------------------------------------------ #
    parser_events_docs = short_term_db_target.get_destinations_content(
        ParserStatus.get_table_name()
    )
    parser_global_docs = short_term_db_target.get_destinations_content(
        "GlobalParserStatus"
    )

    assert len(parser_events_docs) > 0, "Expected at least one ParserStatus document."
    assert (
        len(parser_global_docs) > 0
    ), "Expected at least one GlobalParserStatus document."

    # group by task_id — one group per (chain, file_type) per occasion
    parser_by_task: dict = {}
    for doc in parser_global_docs :
        tid = doc["task_id"]
        parser_by_task.setdefault(tid, {"events": [], "global_status": []})
        payload = {k: v for k, v in doc.items() if k != "index"}
        parser_by_task[tid]["global_status"].append(payload)

    for doc in parser_events_docs:
        tid = doc["task_id"]
        payload = {k: v for k, v in doc.items() if k != "index"}
        parser_by_task[tid]["events"].append(payload)   
        
    # all enabled scrapers are represented in the started events
    scrapers_seen = {
        event["scraper"]
        for parts in parser_by_task.values()
        for event in parts["global_status"]
        if event.get("status") == "started" and "scraper" in event
    }
    assert scrapers_seen == set(enabled_scrapers), (
        f"Expected scrapers {set(enabled_scrapers)}, found {scrapers_seen}"
    )

    # each task forms a valid lifecycle
    for tid, parts in parser_by_task.items():
        model = ParserStatusOutput(
            events=parts["events"],
            global_status=parts["global_status"],
        )
        valid = model.validate_file_status()
        assert valid, f"ParserStatusOutput invalid for task {tid}"

    if num_of_occasions is not None:
        expected_tasks = len(enabled_scrapers) * len(FileTypesFilters) * num_of_occasions
        assert len(parser_by_task) == expected_tasks, (
            f"Expected {expected_tasks} parser tasks, found {len(parser_by_task)}"
        )

    assert short_term_db_target._is_collection_updated(
        ParserStatus.get_table_name(), seconds=60 * 60 * 3
    ), "ParserStatus should be updated in the last 3 hours"
    assert short_term_db_target._is_collection_updated(
        "GlobalParserStatus", seconds=60 * 60 * 3
    ), "GlobalParserStatus should be updated in the last 3 hours"


def validate_longterm_and_short_sync(
    enabled_scrapers,
    short_term_database_connector,
    long_term_database_connector,
    num_of_expected_files=None,
):
    """
    Validate that the API and the long-term database are in sync.

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
    assert (
        access_layer.is_short_term_updated()
    ), f"Short-term database should be updated in the last hour"
    #
    for chain in enabled_scrapers:
        files = access_layer.list_files(chain=chain)
        assert (
            num_of_expected_files is None
            or len(files.processed_files) == num_of_expected_files
        ), f"Expected {num_of_expected_files} processed files for chain {chain}, found {len(files.processed_files)}"

        entries_in_short_term_db = 0
        for file in files.processed_files:
            content = access_layer.get_file_content(chain=chain, file=file.file_name)
            entries_in_short_term_db += len(content.rows)

        entries_in_long_term_db = 0
        csv_file = long_term_database_connector.list_files(
            chain=chain.lower(), extension="csv"
        )
        assert (
            len(csv_file) > 0
        ), f"No CSV files found for chain {chain.lower()} in long-term database"

        for file in csv_file:
            df = long_term_database_connector.get_file_content(file)
            entries_in_long_term_db += df.shape[0]

        assert (
            entries_in_short_term_db == entries_in_long_term_db
        ), f"Number of entries in short-term DB ({entries_in_short_term_db}) does not match long-term DB ({entries_in_long_term_db}) for chain {chain}"
