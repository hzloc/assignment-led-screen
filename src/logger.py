import logging
import sys
from pathlib import Path


def create_logger(logger_name: str = 'default'):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level=logging.INFO)
    console_handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def create_file_logger(logger_name: str = 'default') -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(level=logging.INFO)
    console_handler = logging.FileHandler(filename=Path(f"./logs/{logger_name}.txt"))
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


log = create_logger("babysitter")
