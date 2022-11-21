import logging
from logging import Logger

from .settings import DEBUG

LOGGING_FORMAT_STR = "%(asctime)-10s [%(name)-40s] [%(levelname)-8s] %(message)s"


def get_logger(name: str) -> Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOGGING_FORMAT_STR)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    return logger
