from utils import get_long_term_database_connector, get_short_term_database_connector
from publishers.dag_publisher import SupermarketDataPublisherInterface
import os

if __name__ == "__main__":

    limit = os.environ.get("LIMIT", None)
    if limit:
        limit = int(limit)

    publisher = SupermarketDataPublisherInterface(
        app_folder="app_data",
        long_term_db_target=get_long_term_database_connector(),
        short_term_db_target=get_short_term_database_connector(),
        number_of_scraping_processes=min(os.cpu_count(), 3),
        number_of_parseing_processs=min(os.cpu_count(), 3),
        limit=limit,
    )
    publisher.run(operations=os.environ["OPREATION"])
