import os
import logging
import pandas as pd
from remotes import ShortTermDatabaseUploader
from managers.cache_manager import CacheState
from data_models.raw_schema import DataTable, file_name_to_table


class LargeFilePushManager:

    def __init__(
        self,
        outputs_folder: str,
        database_manager: ShortTermDatabaseUploader,
        chunk_size: int = 10000,
    ):
        """Initialize the LargeFilePushManager.
        The manager is responsible for pushing large files on a limited RAM machine.
        It does so by reading the file in chunks and uploading to the database.
        Args:
            outputs_folder (str): Path to the folder containing files to process
            database_manager (ShortTermDatabaseManager): Database manager for data insertion
            chunk_size (int): Number of rows to process in each chunk
        """
        self.outputs_folder = outputs_folder
        self.database_manager = database_manager
        self.chunk_size = chunk_size

    def _get_header(self, file: str):
        file_path = os.path.join(self.outputs_folder, file)
        return pd.read_csv(file_path, nrows=0).columns

    def process_file(self, file: str, local_cache: CacheState) -> None:
        """Process a large file in chunks and upload to database.

        Args:
            file (str): Name of the file to process
            local_cache (CacheState): Cache object to track processed rows
        """
        file_path = os.path.join(self.outputs_folder, file)
        target_table_name = file_name_to_table(file)
        logging.info(f"Pushing {file} to {target_table_name}")

        # Get last processed row from cache
        last_row = local_cache.get_last_processed_row(file, default=-1)
        logging.info(f"Last row: {last_row}")

        # Read header for column names
        header = self._get_header(file)
        logging.info(f"Header: {header}")

        last_row_saw = None
        # Process file in chunks
        for chunk in pd.read_csv(
            file_path,
            skiprows=lambda x: x == 0
            or x
            < last_row + 1,  # skip header since we provide it and the current batch
            names=header,
            chunksize=self.chunk_size,
        ):

            if chunk.empty:
                logging.warning(f"Chunk is empty,exiting... ")
                break

            # Set index releative to the 'last_row'
            stop_index = last_row + 1 + chunk.shape[0]
            chunk.index = range(last_row + 1, stop_index)
            # update for next itreation
            last_row = stop_index - 1
            # log the batch
            logging.info(
                f"Batch start: {chunk.iloc[0].name}, end: {chunk.iloc[-1].name}"
            )

            # Handle overlap with previous chunk if exists
            if last_row_saw is not None:
                chunk = pd.concat([last_row_saw, chunk])

            # Process and upload chunk
            items = [
                DataTable(
                    row_index=record["row_index"],
                    found_folder=record["found_folder"],
                    file_name=record["file_name"],
                    content={
                        k: v
                        for k, v in record.items()
                        if k not in ["row_index", "found_folder", "file_name"]
                    },
                ).to_dict()
                for record in chunk.reset_index(names=["row_index"])
                .ffill()
                .to_dict(orient="records")
            ]

            # remove the first item since it was used of ffill
            if last_row_saw is not None:
                items = items[1:]

            self.database_manager._insert_to_database(target_table_name, items)

            # Save last row for next iteration
            last_row_saw = chunk.tail(1)

        # Update cache with last processed row
        local_cache.update_last_processed_row(file, last_row)
        logging.info(f"Completed pushing {file}")
