import logging
from datetime import date, datetime
from pathlib import Path
from config import LOG_DIR

LOG_PATH = LOG_DIR / f"pipeline_{date.today().strftime('%Y%m%d')}.log"


class PeriodFormatter(logging.Formatter):
    """Use period instead of comma for milliseconds."""

    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime("%Y-%m-%d %H:%M:%S")
        return f"{s}.{int(record.msecs):04d}"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # file handler
        fh = logging.FileHandler(LOG_PATH)
        fh.setFormatter(PeriodFormatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        ))

        # console handler
        ch = logging.StreamHandler()
        ch.setFormatter(PeriodFormatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        ))

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
