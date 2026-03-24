# Configuration file for ETL pipeline. Contains constants, maps, file paths, and settings.
# Serves as a single source of truth for configuration values used across the project.

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import urllib

# Load environment variables from .env file
load_dotenv()

# Lazy import parsers to avoid circular imports
def get_parsers():
    from src.parsers.base_parser import BaseParser
    from src.parsers.inav_bskt import INAVBskt
    return {"BaseParser": BaseParser, "INAVBskt": INAVBskt}

# ===== Directory Paths =====
ROOT_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DATA_DIR = ROOT_DIR / "data" / "processed"
QUARANTINED_DATA_DIR = ROOT_DIR / "data" / "quarantined"
LOG_DIR = ROOT_DIR / "logs"

# ===== Database Configuration =====
DB_PARAMS = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)
DUCKDB_FILE = ROOT_DIR / "data" / "FundOperations.duckdb"
DB_CONN_STR_DUCKDB = f"duckdb:///{DUCKDB_FILE}"
# DB_CONN_STR_SQLSERVER = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(DB_PARAMS)}"

# Active connection string (toggle which one to use)
DB_CONN_STR = DB_CONN_STR_DUCKDB

# ===== Logging Configuration =====
LOG_FILE = LOG_DIR / "etl.log"
LOG_LEVEL = "INFO"

# ===== Email Configuration =====
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")

# ===== MFT Configuration =====
MFT_BASE_URL = os.getenv("MFT_BASE_URL")
MFT_USERNAME = os.getenv("MFT_USERNAME")
MFT_PASSWORD = os.getenv("MFT_PASSWORD")
MFT_CERT_PATH = os.getenv("MFT_CERT_PATH")
MFT_KEY_PATH = os.getenv("MFT_KEY_PATH")

# ===== Data Cleaning & Type Mapping =====
NULL_LIKE_VALUES = ["", " ", "NA", "NAN", "N/A", "NULL", "NONE", "-"]

# ===== Ingestion Mappings =====
# Each mapping defines: file pattern, parser class, parser method, and outputs
# Pattern is tested with: if re.search(pattern, filename)
# Parser is string name (resolved via get_parsers()) to avoid circular imports
# Each output defines target_table and schema for each returned dataframe
INGESTION_MAPPINGS = [
    {
        "pattern": r"All_Positions.*\.csv",
        "parser_class": "BaseParser",
        "parser_method": "parse",
        "outputs": [
            {
                "target_table": "alphadesk_holdings",
                "schema": {
                    "Basket Date": "datetime64[ns]",
                    "MATURITY DATE": "datetime64[ns]",
                    "Basket Shares": int,
                    "ROUND LOT": float,
                    "PAR": float,
                    "INTEREST RATE": float,
                    "CORPORATE ACTION FACTOR": float,
                    "INTEREST FACTOR": float,
                    "TIP FACTOR": float,
                },
            }
        ],
    },
    {
        "pattern": r"PLF_Positions.*\.csv",
        "parser_class": "BaseParser",
        "parser_method": "parse",
        "outputs": [
            {
                "target_table": "plf_holdings",
                "schema": {
                    "Basket Date": "datetime64[ns]",
                    "Basket Shares": int,
                },
            }
        ],
    },
    {
        "pattern": r"Daily_Net_Asset_Values.*\.csv",
        "parser_class": "BaseParser",
        "parser_method": "parse",
        "outputs": [
            {
                "target_table": "daily_NAVs",
                "schema": {
                    "Price Date": "datetime64[ns]",
                    "Shares/Units Outstanding": int,
                    "Net Asset Value Per Share": float,
                    "Total Net Asset Value": float,
                    "$ Change NAV": float,
                    "Class Ratio": float,
                    "Dual Pricing Basis": int,
                },
            }
        ],
    },
    {
        "pattern": r"Accounting_Cash_Statement.*\.csv",
        "parser_class": "BaseParser",
        "parser_method": "parse",
        "outputs": [
            {
                "target_table": "accounting_cash_transactions",
                "schema": {
                    "Report Date": "datetime64[ns]",
                    "Post Date": "datetime64[ns]",
                    "Amount Received": float,
                    "Disbursed Amount": float,
                    "Shares/Par Value": int,
                    "Report Date Starting Balance": float,
                    "Ending Ledger Balance": float,
                },
            }
        ],
    },
    {
        "pattern": r"Custody_Positions.*\.csv",
        "parser_class": "BaseParser",
        "parser_method": "parse",
        "outputs": [
            {
                "target_table": "custody_positions",
                "schema": {
                    "Maturity Date": "datetime64[ns]",
                    "Traded": int,
                    "Available": int,
                },
            }
        ],
    },
    {
        "pattern": r"Custody_Transactions.*\.csv",
        "parser_class": "BaseParser",
        "parser_method": "parse",
        "outputs": [
            {
                "target_table": "custody_transactions",
                "schema": {
                    "Trade Date": "datetime64[ns]",
                    "Pay/Settle Date": "datetime64[ns]",
                    "Share Quantity": int,
                    "Net Amount": float,
                    "Mainframe Time Stamp": "datetime64[ns]",
                },
            }
        ],
    },
    {
        "pattern": r"Cash_Forecast_Transactions.*\.csv",
        "parser_class": "BaseParser",
        "parser_method": "parse",
        "outputs": [
            {
                "target_table": "cash_forecast_transactions",
                "schema": {
                    "Trade Date": "datetime64[ns]",
                    "Pay/Settle Date": "datetime64[ns]",
                    "Share Quantity": int,
                    "Net Amount": float,
                    "Mainframe Time Stamp": "datetime64[ns]",
                },
            }
        ],
    },
    {
        "pattern": r"Harvest_BSKT.*\.CSV",
        "parser_class": "INAVBskt",
        "parser_method": "extract",
        "outputs": [
            {
                "target_table": "bskt_metrics",
                "schema": {},
            },
            {
                "target_table": "bskt_holdings",
                "schema": {},
            },
        ],
    },
]