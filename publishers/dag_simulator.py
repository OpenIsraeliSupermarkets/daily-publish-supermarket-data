"""
Module for simulating DAG-based execution of supermarket data publishing tasks.
Provides scheduling and execution of tasks at specified times.
"""
import logging
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
        start_at=None,
        completed_by=None,
        num_of_occasions=3,
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
        self.num_of_occasions = num_of_occasions
        self.completed_by = completed_by if completed_by else self._end_of_day()
        self.start_at = start_at if start_at else self._non()
        self.executed_jobs = 0
        self.occasions = self._compute_occasions()

    def _setup_schedule(self, operations):
        logging.info("Scheduling the scraping tasks at %s", self.occasions)
        for occasion in self.occasions:
            schedule.every().day.at(occasion).do(self._execute_operations, operations)

    def _execute_operations(self, operations):
        try:
            super().run(operations)
        finally:
            self.executed_jobs += 1
            logging.info("Scraping task is done")

    def _track_task(self):
        logging.info("Starting the tracking tasks")
        while self.executed_jobs < len(self.occasions):
            schedule.run_pending()
            time.sleep(1)
        logging.info("Scraping tasks are done, starting the converting task")

    def _compute_occasions(self):
        """Compute the occasions for the scraping tasks"""
        interval_start = max(self.start_at, self.today)
        interval = (
            self.completed_by - interval_start
        ).total_seconds() / self.num_of_occasions
        occasions = [
            (interval_start + datetime.timedelta(minutes=1)).strftime("%H:%M")
        ] + [
            (interval_start + datetime.timedelta(seconds=interval * (i + 1))).strftime(
                "%H:%M"
            )
            for i in range(1, self.num_of_occasions)
        ]
        return occasions

    def _get_time_to_execute(self):
        return datetime.timedelta(hours=1)

    def _end_of_day(self):
        """Return the end of the day"""
        return (
            datetime.datetime.combine(self.today, datetime.time(23, 59))
            - self._get_time_to_execute()
        )

    def _non(self):
        """Return the start of the day"""
        return datetime.datetime.combine(self.today, datetime.time(12, 0))

    def run(self, operations, final_operations=None, now=False):
        """
        Run the scheduled operations and then the final operations.

        Args:
            operations: Operations to run on schedule
            final_operations: Operations to run after scheduled operations complete
            now: Whether to run the iterative operations immediately

        Note:
            This method overrides the parent class run method with different parameters.
        """
        itreative_operations = operations

        if now:
            self._execute_operations(itreative_operations)

        self._check_tz()
        self._setup_schedule(itreative_operations)
        self._track_task()

        if final_operations:
            super().run(operations=final_operations)
