"""
Interface for executing DAG-like operations for supermarket data publishing.
This module provides a class that supports running a sequence of operations.
"""
import logging
from publishers.base_publisher import BaseSupermarketDataPublisher


logging.getLogger("Logger").setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class SupermarketDataPublisherInterface(BaseSupermarketDataPublisher):
    """
    Interface for executing DAG-like operations for supermarket data publishing.
    
    Extends BaseSupermarketDataPublisher to provide a method for running
    a sequence of operations specified as a comma-separated string.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run(self, operations):
        """
        Run the specified operations in sequence.
        
        Args:
            operations: Comma-separated string of operations to execute
        """
        logging.info("Starting executing DAG = %s", operations)
        self._check_tz()
        for operation in operations.split(","):

            logging.info("Starting the operation=%s", operation)
            if operation == "scraping":
                self._execute_scraping()
            elif operation == "converting":
                self._execute_converting()
            elif operation == "clean_dump_files":
                self._clean_all_dump_files()
            elif operation == "publishing":
                self._upload_and_clean()
            elif operation == "api_update":
                self._update_api_database()
            elif operation == "reload_complete_api":
                self._update_api_database(reset_cache=True)
            elif operation == "upload_compose":
                self._upload_and_clean(compose=True)
            elif operation == "upload_no_compose":
                self._upload_and_clean(compose=False)
            elif operation == "clean_all_source_data":
                self._clean_all_source_data()
            elif operation == "all":
                self._execute_scraping()
                self._execute_converting()
                self._upload_and_clean()
            else:
                raise ValueError(f"Invalid operation {operation}")
