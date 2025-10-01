"""
Module for centralized logging configuration.
"""

import logging
import os
from sys import stdout
from discord_logging.handler import DiscordHandler




def build_logger():
    """create the logger instance"""
    # Define logger
    logger = logging.getLogger(os.getenv("LOG_LEVEL", "WARNING").upper())

    if not logger.handlers:
        logger.setLevel(logging.DEBUG)  # set logger level
        log_formatter = logging.Formatter(
            "%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s"
        )
        
        # Console handler
        console_handler = logging.StreamHandler(stdout)  # set streamhandler to stdout
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)

        # File handler
        file_handler = logging.FileHandler("logging.log")
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

        # Discord handler (optional)
        discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL", None)
        if discord_webhook_url:
            try:
                discord_formatter = logging.Formatter("%(message)s")
                discord_handler = DiscordHandler(
                    "Supermarket Scraping Bot",
                    discord_webhook_url,
                    avatar_url="https://cdn-icons-png.flaticon.com/512/3081/3081840.png"
                )
                discord_handler.setFormatter(discord_formatter)
                # Only log ERROR and CRITICAL messages to Discord to avoid spam
                discord_handler.setLevel(logging.ERROR)
                logger.addHandler(discord_handler)
                logger.info("Discord logging handler enabled")
            except Exception as e:
                logger.warning(f"Failed to initialize Discord handler: {e}")
    return logger


class Logger:
    """a static logger class to share will all components"""

    logger = build_logger()

    @classmethod
    def info(cls, msg, *args, **kwargs):
        """log info"""
        cls.logger.info(msg, *args, **kwargs)

    @classmethod
    def debug(cls, msg, *args, **kwargs):
        """log debug"""
        cls.logger.debug(msg, *args, **kwargs)

    @classmethod
    def error(cls, msg, *args, **kwargs):
        """log error"""
        cls.logger.error(msg, *args, **kwargs)

    @classmethod
    def warning(cls, msg, *args, **kwargs):
        """log warning"""
        cls.logger.warning(msg, *args, **kwargs)

    @classmethod
    def critical(cls, msg, *args, **kwargs):
        """log critical"""
        cls.logger.critical(msg, *args, **kwargs)
