import os
import sys
from sqlalchemy import create_engine, text

import pandas as pd

# Ensure project root is on sys.path regardless of the current working directory.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config.settings import BRONZE_DIR, RAW_DATA_DIR
from src.parsers.inav_bskt import INAVBskt

INAVdata_file = os.path.join(RAW_DATA_DIR, "Harvest_INAVBSKT_ALL.20260218.csv")

inst = INAVBskt(INAVdata_file).extract()

def load_into_bronze(df, table_name:str):
    """Example function to load DataFrame into a SQL database using SQLAlchemy."""
    try:
        # Create a SQLAlchemy engine (replace with your actual database URI)
        # Example using SQLite in the bronze layer data directory
        db_path = os.path.join(BRONZE_DIR, "inav_bskt.db")
        engine = create_engine(f"sqlite:///{db_path}")

        # Load DataFrame into the specified table, replacing it if it already exists
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        print(f"Data loaded into {table_name} table successfully.")    
    except Exception as e:
        print(f"Failed to load data into {table_name} table: {e}")

if __name__ == "__main__":
    if inst:
        fund_metrics_df, fund_holdings_df = inst
        load_into_bronze(fund_metrics_df, "fund_metrics")
        load_into_bronze(fund_holdings_df, "fund_holdings")