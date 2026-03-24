"""
Main entry point: Configure and run the ingestion pipeline.
Instantiates components and orchestrates execution.
"""

from config import RAW_DATA_DIR, DB_CONN_STR
from src.parsers.base_parser import BaseParser
from src.parsers.inav_bskt import INAVBskt
from src.cleaner import DataFrameCleaner
from src.validator import DataValidator
from pathlib import Path
import re
import pandas as pd


def main():
    """Run ingestion pipeline: discover → read → clean → validate → write → move."""
    for file_path in RAW_DATA_DIR.iterdir():
        # print(f"Processing file: {file_path.name}")

        if re.search(r"All_Positions*", file_path.stem, re.IGNORECASE):
            ans = BaseParser(file_path).parse()
            ans_cleaned = DataFrameCleaner().clean(ans)
            ans_cols_normalized = _normalize_columns(ans_cleaned)
            print(ans_cols_normalized.head(2))
            

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names: strip, lowercase, replace spaces/hyphens with underscores.
    
    Args:
        df: DataFrame with raw column names.
    """
    df.columns = (df.columns.astype(str)
                  .str.strip()
                  .str.replace(r'(.)([A-Z][a-z]+)', r'\1_\2', regex=True)  # BasketDate → Basket_Date
                  .str.replace(r'([a-z0-9])([A-Z])', r'\1_\2', regex=True)  # basketShares → basket_Shares
                  .str.lower()                                               # basket_date
                  .str.replace(r"[\-\s+]", "_", regex=True))                # replace spaces/hyphens
    return df

if __name__ == "__main__":
    main()
