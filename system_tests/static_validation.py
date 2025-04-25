#!/usr/bin/env python3
"""
Script to download data from Kaggle and validate its structure.
This script connects to a Kaggle dataset, downloads the data,
and validates the structure of the downloaded data.
"""

import os
import tempfile
import logging
from datetime import datetime
import pytz


from remotes import KaggleUploader,MongoDbUploader
from tests.validation_utils import validate_long_term_structure, validate_short_term_structure


def download_and_validate_kaggle_data(dataset_remote_name, enabled_scrapers,file_per_run, mongodb_uri):
    """
    Download data from Kaggle and validate its structure.

    Args:
        dataset_name: Name of the Kaggle dataset
        enabled_scrapers: List of enabled scrapers
    Returns:
        bool: True if validation passes, False otherwise
    """

    # Create temporary directories
    temp_dir = tempfile.mkdtemp(prefix="kaggle_validation_")
    stage_folder = os.path.join(temp_dir, "stage")
    os.makedirs(stage_folder, exist_ok=True)

    logging.info(f"Created temporary directory: {temp_dir}")

    try:
        # Initialize KaggleUploader
        long_term_db_target = KaggleUploader(
            dataset_path=stage_folder,
            dataset_remote_name=dataset_remote_name,
            when=datetime.now(tz=pytz.utc),
        )
        
        short_term_db_target = MongoDbUploader(
            mongodb_uri=mongodb_uri
        )
        validate_short_term_structure(
            short_term_db_target,
            long_term_db_target,
            enabled_scrapers,
            1,
            file_per_run
        )
        validate_long_term_structure(
            long_term_db_target, stage_folder, enabled_scrapers
        )
        

    finally:
        # Clean up temporary files
        import shutil

        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logging.info(f"Cleaned up temporary directory: {temp_dir}")
