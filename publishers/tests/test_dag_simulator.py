
import datetime
from publishers.dag_simulator import SupermarketDataPublisher
from il_supermarket_scarper.scrappers_factory import ScraperFactory
from remotes import DummyFileStorage, DummyDocumentDbUploader
import os
from utils import now
import tempfile
from publishers.tests.validation_utils import validate_cleanup,validate_long_term_structure,validate_api_scan


def test_full_dag():
    # params
    expected_duration_in_minutes = 1
    num_of_occasions = 1
    file_per_run = 3
    app_folder = "app_data"
    data_folder = "dumps"
    outputs_folder = "outputs"
    status_folder = "status"
    when_date = None  # datetime.datetime(2025,1,10,0,0,0)

    with tempfile.TemporaryDirectory() as temp_dir:

        remote_dataset_path = os.path.join(temp_dir,"remote_test_dataset")
        stage_folder = os.path.join(temp_dir,"stage")
        
        app_folder = os.path.join(temp_dir, app_folder)
        outputs_folder = os.path.join(app_folder,outputs_folder)
        status_folder = os.path.join(app_folder,status_folder)
        data_folder = os.path.join(app_folder,data_folder)
        
        long_term_db_target = DummyFileStorage(
            dataset_remote_path=remote_dataset_path,
            dataset_path=stage_folder, 
            when=now()
        )
        short_term_db_target = DummyDocumentDbUploader(
            db_path=temp_dir
        )

        enabled_scrapers = ScraperFactory.sample(n=1)
        # run the process for couple of times
        publisher = SupermarketDataPublisher(
            long_term_db_target=long_term_db_target,
            short_term_db_target=short_term_db_target,
            app_folder=app_folder,
            data_folder=data_folder,
            enabled_scrapers=enabled_scrapers,
            enabled_file_types=None,
            limit=file_per_run,
            start_at=now(),
            completed_by=now() + datetime.timedelta(minutes=num_of_occasions * expected_duration_in_minutes),
            num_of_occasions=num_of_occasions,
            when_date=when_date,
        )
        publisher.run(
            now=True,
            itreative_operations="scraping,converting,api_update,clean_dump_files",
            final_operations="publishing,clean_all_source_data",
        )


        validate_api_scan(enabled_scrapers,short_term_db_target,long_term_db_target,num_of_occasions * file_per_run,remote_dataset_path)
        #
        validate_long_term_structure(remote_dataset_path,stage_folder,enabled_scrapers)
        
        # validate the output
        validate_cleanup(app_folder,data_folder,outputs_folder,status_folder)

