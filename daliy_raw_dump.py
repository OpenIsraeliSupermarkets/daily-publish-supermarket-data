from il_supermarket_scarper import MainScrapperRunner
from il_supermarket_parsers import ParallelParser


number_of_processes = 6
data_folder = "dumps"

scraper = MainScrapperRunner()
scraper.run()

ParallelParser(
      data_folder,
      number_of_processes=number_of_processes,
).execute()
