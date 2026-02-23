import logging
import sys

def setup_logger(name="my_etl_logger", log_file=None, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Optional file handler
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

# Example usage:
# logger = setup_logger()
# logger.info("ETL pipeline started.")
# logger.warning("Missing NAV value.")
# logger.error("Failed to parse holdings block.")
