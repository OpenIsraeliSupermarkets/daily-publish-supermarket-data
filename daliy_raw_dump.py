from il_supermarket_scarper import ScarpingTask,ScraperFactory
from il_supermarket_parsers import ConvertingTask
from kaggle_database_manager import KaggleDatasetManager
import os
import shutil
import json


if __name__ == "__main__":
    number_of_processes = 6
    data_folder = "dumps"
    outputs_folder = "outputs"
    status_folder = "status"
    enabled_scrapers=None

    ScarpingTask(
        enabled_scrapers=enabled_scrapers, #download one from each 
        dump_folder_name=data_folder,
        multiprocessing=number_of_processes,
        lookup_in_db=True,
        only_latest=True
    ).start()
    scraper = ConvertingTask(
        enabled_parsers=enabled_scrapers,
        data_folder=data_folder,
        multiprocessing=number_of_processes,
        output_folder=outputs_folder
    ).start()

    shutil.rmtree("israeli-supermarkets-2024",ignore_errors=True)
    os.makedirs("israeli-supermarkets-2024",exist_ok=True)
    with open("israeli-supermarkets-2024/dataset-metadata.json",'w') as file:
        json.dump({
        "title": "Israeli Supermarkets 2024", 
        "id": "erlichsefi/israeli-supermarkets-2024", 
        "licenses": [{"name": "CC0-1.0"}]
        },file)
    shutil.copytree(outputs_folder, "israeli-supermarkets-2024",dirs_exist_ok=True)
    shutil.rmtree(outputs_folder)
    shutil.copytree(status_folder, "israeli-supermarkets-2024",dirs_exist_ok=True)
    shutil.rmtree(status_folder)
    shutil.rmtree(data_folder)

    database = KaggleDatasetManager(dataset="israeli-supermarkets-2024")
    database.upload_to_dataset(version_notes="first try")

