"""
Module for simulating DAG-based execution of supermarket data publishing tasks.
Provides scheduling and execution of tasks at specified times.
"""

from utils import Logger
import time
import datetime
import schedule
from remotes import KaggleUploader, MongoDbUploader
from publishers.dag_publisher import SupermarketDataPublisherInterface


class SupermarketDataPublisher(SupermarketDataPublisherInterface):
    """
    Publisher that simulates DAG execution by scheduling tasks at specified intervals.

    Extends SupermarketDataPublisherInterface to provide functionality for scheduling
    and tracking execution of tasks.
    """

    def __init__(
        self,
        long_term_db_target=KaggleUploader,
        short_term_db_target=MongoDbUploader,
        number_of_scraping_processes=4,
        number_of_parseing_processs=None,
        app_folder="app_data",
        data_folder="dumps",
        outputs_folder="outputs",
        status_folder="status",
        enabled_scrapers=None,
        enabled_file_types=None,
        limit=None,
        when_date=None,
    ):
        super().__init__(
            number_of_scraping_processes=number_of_scraping_processes,
            number_of_parseing_processs=number_of_parseing_processs,
            long_term_db_target=long_term_db_target,
            short_term_db_target=short_term_db_target,
            app_folder=app_folder,
            data_folder=data_folder,
            outputs_folder=outputs_folder,
            status_folder=status_folder,
            enabled_scrapers=enabled_scrapers,
            enabled_file_types=enabled_file_types,
            limit=limit,
            when_date=when_date,
        )
        self.executed_jobs = 0
        self.last_execution_time = None

    def _now(self):
        return datetime.datetime.now()

    def _execute_operations(self, operations):
        try:
            self.last_execution_time = self._now()
            super().run(operations)
        finally:
            self.executed_jobs += 1
            Logger.info(f"Done {operations}")

    def _should_execute_final_operations(self, should_execute_final_operations):
        """Return True if the repeat condition is met"""
        if should_execute_final_operations == "EOD":
            return (
                self.last_execution_time
                and self.last_execution_time.date() < self._now().date()
            )
        elif should_execute_final_operations == "ONCE":
            return self.last_execution_time is not None
        elif isinstance(should_execute_final_operations, int):
            return self.executed_jobs >= should_execute_final_operations
        else:
            raise ValueError(
                f"Invalid repeat condition: {should_execute_final_operations}"
            )

    def _should_stop_dag(self, should_stop_dag):
        """Return True if the stop condition is met"""
        if should_stop_dag == "NEVER":
            return False
        elif should_stop_dag == "ONCE":
            return self.last_execution_time is not None
        else:
            raise ValueError(f"Invalid stop condition: {should_stop_dag}")

    def run(
        self,
        operations,
        final_operations=None,
        second_to_wait_between_opreation=60,
        second_to_wait_after_final_operations=0,        
        should_execute_final_operations="EOD",
        should_stop_dag="NEVER",
    ):
        """
        Run the scheduled operations and then the final operations.

        Args:
            operations: Operations to run on schedule
            final_operations: Operations to run after scheduled operations complete
            now: Whether to run the iterative operations immediately
            use_wait_time: Whether to use wait time between executions (True) or time-based scheduling (False)

        Note:
            This method overrides the parent class run method with different parameters.
        """
        Logger.info(
            f"Executing operations with second_to_wait_between_opreation={second_to_wait_between_opreation}s wait time between operations and second_to_wait_after_final_operations={second_to_wait_after_final_operations}s wait time after final operations"
        )

        while not self._should_stop_dag(should_stop_dag):
            # execute operations until final operations are executed
            while True:
                now = self._now()
                Logger.info(f"Setting when_date to {now}")
                self.when_date = now
                
                # execute operations
                Logger.info(f"Executing operations")
                self._execute_operations(operations)

                # check if final operations should be executed
                if self._should_execute_final_operations(
                    should_execute_final_operations
                ):
                    break

                # if not, wait for next run
                Logger.info(f"Waiting {second_to_wait_between_opreation} seconds before next run")
                time.sleep(second_to_wait_between_opreation)

            Logger.info(f"Executing final operations")
            if final_operations:
                self._execute_operations(final_operations)
                Logger.info(f"Waiting {second_to_wait_after_final_operations} seconds after final operations")
                time.sleep(second_to_wait_after_final_operations)
