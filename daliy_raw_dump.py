from il_supermarket_scarper import ScarpingTask,ScraperFactory
from il_supermarket_parsers import ConvertingTask
from kaggle import KaggleDatasetManager


if __name__ == "__main__":
    number_of_processes = 6
    data_folder = "dumps"
    outputs_folder = "outputs"
    enabled_scrapers=[ScraperFactory.BAREKET.name]

    ScarpingTask(
        enabled_scrapers=enabled_scrapers, #download one from each 
        dump_folder_name=data_folder,
        limit=1,
        multiprocessing=number_of_processes,
        lookup_in_db=True
    ).start()
    scraper = ConvertingTask(
        enabled_parsers=enabled_scrapers,
        data_folder=data_folder,
        multiprocessing=number_of_processes,
        output_folder=outputs_folder
    ).start()

    database =  KaggleDatasetManager()
    database.upload_to_dataset("israeli-supermarkets-2024", data_folder)

