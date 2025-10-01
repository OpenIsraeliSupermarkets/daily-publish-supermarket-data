"""
Interface for executing DAG-like operations for supermarket data publishing.
This module provides a class that supports running a sequence of operations.
"""

import os
from utils import Logger, HeartbeatManager

from publishers.base_publisher import BaseSupermarketDataPublisher


class SupermarketDataPublisherInterface(BaseSupermarketDataPublisher):
    """
    Interface for executing DAG-like operations for supermarket data publishing.

    Extends BaseSupermarketDataPublisher to provide a method for running
    a sequence of operations specified as a comma-separated string.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Initialize heartbeat manager
        heartbeat_path = os.path.join(self.app_folder, 'heartbeat.json')
        self.heartbeat = HeartbeatManager(heartbeat_path)

    def _execute_single_operation(self, operation):
        """
        Execute a single operation with heartbeat tracking.
        
        Args:
            operation: Name of the operation to execute
        """
        if operation == "scraping":
            self._execute_scraping()
        elif operation == "converting":
            self._execute_converting()
        elif operation == "download_from_long_term":
            self._download_from_long_term_database()
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
        else:
            raise ValueError(f"Invalid operation {operation}")

    def run(self, operations):
        """
        Run the specified operations in sequence.

        Args:
            operations: Comma-separated string of operations to execute
        """
        Logger.info("Starting executing DAG = %s", operations)
        self._check_tz()
        
        for operation in operations.split(","):
            Logger.info(f"DAG is {operations} starting the operation=%s", operation)
            
            # Mark operation as started in heartbeat
            self.heartbeat.start_operation(operation)
            
            try:
                # Execute the operation
                self._execute_single_operation(operation)
                
                # Mark operation as completed successfully
                self.heartbeat.complete_operation(operation, success=True)
                Logger.info("Done the operation=%s", operation)
                
            except Exception as e:
                # Mark operation as failed
                self.heartbeat.complete_operation(operation, success=False, error=str(e))
                Logger.error(f"Operation {operation} failed: {e}")
                raise  # Re-raise the exception to maintain original behavior
