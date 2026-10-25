"""Consistent console and rotating-file logging for the ETL pipeline."""

import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

LOG_FILE = os.getenv("ETL_LOG_FILE", "ETL.log")
FORMATTER = logging.Formatter(
    "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)


def get_logger(logger_name: str) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(os.getenv("ETL_LOG_LEVEL", "INFO").upper())
    logger.propagate = False

    # Avoid duplicate messages when the module is imported more than once.
    if logger.handlers:
        return logger

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)

    file_handler = TimedRotatingFileHandler(
        LOG_FILE, when="midnight", backupCount=14, encoding="utf-8"
    )
    file_handler.setFormatter(FORMATTER)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger
