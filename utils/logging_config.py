"""
Module for centralized logging configuration.
"""

import logging
import os

def configure_logging():
    """
    Configure logging based on environment variables.
    
    The log level can be set using the LOG_LEVEL environment variable.
    Valid values are: DEBUG, INFO, WARNING, ERROR, CRITICAL
    If not set, defaults to WARNING.
    """
    log_level = os.getenv("LOG_LEVEL", "WARNING").upper()
    valid_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    
    # Default to WARNING if invalid level provided
    level = valid_levels.get(log_level, logging.WARNING)
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Set level for all existing loggers
    for logger_name in logging.root.manager.loggerDict:
        logging.getLogger(logger_name).setLevel(level) 