import os
import logging
import pandas as pd
from managers.short_term_database_manager import ShortTermDatabaseUploader
from managers.cache_manager import CacheState
from data_models.raw import DataTable, file_name_to_table


class LargeFilePushManager:
    
    def __init__(self, outputs_folder: str, database_manager: ShortTermDatabaseUploader):
        """Initialize the LargeFilePushManager.
        
        Args:
            outputs_folder (str): Path to the folder containing files to process
            database_manager (ShortTermDatabaseManager): Database manager for data insertion
        """
        self.outputs_folder = outputs_folder
        self.database_manager = database_manager
        self.chunk_size = 10000

    def _get_header(self, file:str):
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
            skiprows=lambda x: x < last_row + 1,
            names=header,
            chunksize=self.chunk_size,
        ):
            
            if chunk.empty:
                logging.warning(f"Chunk is empty,exiting... ")
                break
                
            # Set index releative to the 'last_row' 
            stop_index = last_row + 1 + len(chunk)
            chunk.index = range(last_row + 1, stop_index)
            # update for next itreation
            last_row = stop_index
            # log the batch
            logging.info(
                f"Batch start: {chunk.iloc[0].name}, end: {chunk.iloc[-1].name}"
            )

            # Handle overlap with previous chunk if exists
            if last_row_saw is not None:
                chunk = pd.concat([last_row_saw, chunk])

            # Process and upload chunk
            items = chunk.reset_index(names=["row_index"]).ffill().to_dict(orient="records").apply(lambda x: DataTable(**x).to_dict())
            
            self.database_manager.uploader._insert_to_database(target_table_name, items[1:])

            # Save last row for next iteration
            last_row_saw = chunk.drop(columns=["row_index"]).tail(1)

        # Update cache with last processed row
        local_cache.update_last_processed_row(file, last_row)
        logging.info(f"Completed pushing {file}")
