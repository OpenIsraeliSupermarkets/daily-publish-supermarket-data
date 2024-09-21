from il_supermarket_scarper import ScarpingTask,ScraperFactory
from il_supermarket_parsers.main import ConvertingTask
from kaggle import KaggleDatasetManager


number_of_processes = 6
data_folder = "dumps"
enabled_scrapers=[ScraperFactory.BAREKET.name]

scraper = ScarpingTask(
    enabled_scrapers=enabled_scrapers,
    multiprocessing=number_of_processes,
    dump_folder_name=data_folder
)
scraper.start()

converter = ConvertingTask(
      data_folder=data_folder,
      number_of_processes=number_of_processes,
)
files = converter.start()

database =  KaggleDatasetManager()
database.upload_to_dataset("israeli-supermarkets-2024", data_folder)