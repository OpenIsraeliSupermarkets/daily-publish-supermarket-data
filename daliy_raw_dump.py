from il_supermarket_scarper import ScarpingTask,ScraperFactory
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
    enabled_file_types = None

    ScarpingTask(
        enabled_scrapers=enabled_scrapers,  
        files_types=enabled_file_types,
        dump_folder_name=data_folder,
        multiprocessing=number_of_processes,
        lookup_in_db=True,
        only_latest=True,
    ).start()
    scraper = ConvertingTask(
        enabled_parsers=enabled_scrapers,
        files_types=enabled_file_types,
        data_folder=data_folder,
        multiprocessing=number_of_processes,
        output_folder=outputs_folder,
    ).start()

    database = KaggleDatasetManager(dataset="israeli-supermarkets-2024",enabled_scrapers=enabled_scrapers,enabled_file_types=enabled_file_types)
    database.compose(outputs_folder=outputs_folder, status_folder=status_folder)
    database.upload_to_dataset(version_notes=",".join(enabled_scrapers))
    database.clean(data_folder,status_folder,outputs_folder)
