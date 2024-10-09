import shutil
import  os
from il_supermarket_scarper import ScarpingTask
from il_supermarket_parsers import ConvertingTask
from kaggle_database_manager import KaggleDatasetManager


if __name__ == "__main__":
    number_of_processes = 4
    data_folder = "app_data/dumps"
    outputs_folder = "app_data/outputs"
    status_folder = "app_data/dumps/status"
    enabled_scrapers = None
    enabled_file_types = None

    try:
        ScarpingTask(
            enabled_scrapers=enabled_scrapers,  
            files_types=enabled_file_types,
            dump_folder_name=data_folder,
            multiprocessing=number_of_processes,
            lookup_in_db=True,
            only_latest=True,
        ).start()
        ConvertingTask(
            enabled_parsers=enabled_scrapers,
            files_types=enabled_file_types,
            data_folder=data_folder,
            multiprocessing=number_of_processes,
            output_folder=outputs_folder,
        ).start()

        database = KaggleDatasetManager(dataset="israeli-supermarkets-2024",enabled_scrapers=enabled_scrapers,enabled_file_types=enabled_file_types)
        database.compose(outputs_folder=outputs_folder, status_folder=status_folder)
        database.upload_to_dataset()
        database.clean(data_folder,status_folder,outputs_folder)
    
    except Exception as e:    
        # clean the folders in case of an error
        for folder in [data_folder,outputs_folder,status_folder]:
            
            if os.path.exists(folder):
                shutil.rmtree(folder)
        raise e