from il_supermarket_scarper import ScarpingTask
from il_supermarket_parsers import ConvertingTask
from kaggle_database_manager import KaggleDatasetManager
import shutil
import datetime


if __name__ == "__main__":
    number_of_processes = 6
    data_folder = "dumps"
    outputs_folder = "outputs"
    status_folder = "status"
    enabled_scrapers = None

    ScarpingTask(
        enabled_scrapers=enabled_scrapers,  # download one from each
        dump_folder_name=data_folder,
        multiprocessing=number_of_processes,
        lookup_in_db=True,
        only_latest=True,
    ).start()
    scraper = ConvertingTask(
        enabled_parsers=enabled_scrapers,
        data_folder=data_folder,
        multiprocessing=number_of_processes,
        output_folder=outputs_folder,
    ).start()

    database = KaggleDatasetManager(dataset="israeli-supermarkets-2024")
    database.compose(outputs_folder=outputs_folder, status_folder=status_folder)
    database.upload_to_dataset(version_notes="first try")

    shutil.rmtree(data_folder)
