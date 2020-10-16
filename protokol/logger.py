
from logging import Logger
import logging

from .settings import DEBUG

LOGGING_FORMAT_STR = '%(asctime)-10s [%(name)-40s] [%(levelname)-8s] %(message)s'


def get_logger(name: str) -> Logger:
    logging.basicConfig(format=LOGGING_FORMAT_STR, level=logging.DEBUG if DEBUG else logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    return logger
