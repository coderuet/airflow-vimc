import logging


def get_logger(name="airflow_vimc"):
    logger = logging.getLogger(name)
    if not logger.handlers:  # Avoid adding multiple handlers
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
