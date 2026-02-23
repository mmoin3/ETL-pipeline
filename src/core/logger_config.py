import os
# src/config.py
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Logger settings
LOG_FILE = f"{PROJECT_ROOT}/logs/etl.log"
LOG_LEVEL = "INFO"

# Example project paths
DATA_DIR = f"{PROJECT_ROOT}/data/raw/"
REPORTS_DIR = f"{PROJECT_ROOT}/reports/"

# Metadata type map for NAV/INAV files
NAV_METADATA_TYPE_MAP = {
    "TRADE_DATE": "datetime64[ns]",
    "SS_LONG_CODE": str,
    "FULL_NAME": str,
    "TICKER": str,
    "BASE_CURRENCY": str,
    "CREATION_UNIT_SIZE": float,
    "DOMICILE": str,
    "ESTIMATED_DIVIDENDS": float,
    "PRODUCT_STRUCTURE": str,
    "EST_INC": float,
    "SETTLEMENT_CYCLE": str,
    "ASSET_CLASS": str,
    "ESTIMATED_EXPENSE": float,
    "CREATE_FEE": float,
    "ESTIMATED_CASH_COMPONENT": float,
    "NAV": float,
    "UNDISTRIBUTED_NET_INCOME_PER_SHARE": float,
    "BASKET_MARKET_VALUE": float,
    "REDEEM_FEE": float,
    "ACTUAL_CASH_COMPONENT": float,
    "NAV_PER_CREATION_UNIT": float,
    "UNDISTRIBUTED_NET_INCOME_PER_CREATION_UNIT": float,
    "BASKET_SHARES": float,
    "CREATE_VARIABLE_FEE": float,
    "NAV_LESS_UNDISTRIBUTED_NET_INCOME": float,
    "ACTUAL_CASH_IN_LIEU": float,
    "ESTIMATED_CASH_IN_LIEU": float,
    "REDEEM_VARIABLE_FEE": float,
    "ETF_SHARES_OUTSTANDING": float,
    "ACTUAL_INTEREST": float,
    "ESTIMATED_INTEREST": float,
    "EXPENSE_RATIO": float,
    "TOTAL_NET_ASSETS": float,
    "ACTUAL_TOTAL_CASH": float,
    "ESTIMATED_TOTAL_CASH": float,
    "THRESHOLD": str,
}

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
