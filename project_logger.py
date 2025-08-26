import logging
from pythonjsonlogger import jsonlogger
import warnings


def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel("DEBUG")
    if not logger.handlers:
        log_handler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s')
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
        warnings.filterwarnings('ignore')
    return logger
