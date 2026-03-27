from unittest import result

import pandas as pd
import csv
from pathlib import Path
from src.utils.logger import get_logger
from src.extractor import extract
logger = get_logger(__name__)


def parse_baskets(df: pd.DataFrame):
    """Custom parser for the basket files: Harvest_INAVBSKT_ALL.YYYYMMDD.CSV,
    and Harvest_BSKT_ALL.YYYYMMDD.CSV. These files are difficult to parse,
    so we read them line by line and split by a delimiter.

    Builds one unified table by enriching each holdings row with its corresponding metrics.

    Args:
        df: forcefully tabularized input data into pandas dataframe

    Returns:
        Single unified DataFrame with holdings + metrics columns
    """
    blocks = _split_into_blocks(df, markers="TRADE_DATE")

    enriched_holdings_list = []

    for block in blocks:
        # Extract metrics from first 8 rows
        metrics_dict = _extract_metrics(block.iloc[:8])

        # Extract holdings data starting from row 9 (row 8 contains headers)
        holdings_header = block.iloc[8].tolist()
        holdings_df = pd.DataFrame(
            block.iloc[9:].values, columns=holdings_header)

        # Add all metrics columns to holdings dataframe (broadcasts metrics to each row)
        for key, value in metrics_dict.items():
            holdings_df.insert(0, key, value)

        enriched_holdings_list.append(holdings_df)

    # Concatenate all blocks into one big table
    return pd.concat(enriched_holdings_list, ignore_index=True) if enriched_holdings_list else pd.DataFrame()


def _split_into_blocks(df: pd.DataFrame, markers: str = "TRADE_DATE") -> list[pd.DataFrame]:
    """Split DataFrame into blocks based on TRADE_DATE marker."""
    # Find all row indices where TRADE_DATE appears
    block_starts = df[df[0].str.upper().str.startswith(
        markers.upper(), na=False)].index.tolist()
    block_starts.append(len(df))  # Add end of file as last block end

    # Create blocks by slicing between consecutive TRADE_DATE markers
    blocks = []
    for i in range(len(block_starts) - 1):
        start_idx = block_starts[i]
        end_idx = block_starts[i + 1]
        # Its critical that indices are reset here.
        block = df.iloc[start_idx:end_idx].reset_index(drop=True)
        blocks.append(block)

    return blocks


def _extract_metrics(df: pd.DataFrame) -> dict:
    """Extract metrics from the first 8 lines of each block."""
    metrics_dict = {}
    # First line key-value pair (e.g., "TRADE_DATE", "20260224")
    metrics_dict[df.iloc[0, 0]] = df.iloc[0, 1]
    metrics_dict["SS_LONG_CODE"] = df.iloc[0, 2]
    metrics_dict["FULL_NAME"] = df.iloc[0, 4]
    metrics_dict["TICKER_1"] = df.iloc[0, 5]
    metrics_dict["TICKER_2"] = df.iloc[0, 6]
    metrics_dict["BASE_CURRENCY"] = df.iloc[0, 7]
    metrics_dict[df.iloc[0, 8]] = df.iloc[0, 9]

    for i in range(1, len(df)):
        row = df.iloc[i]
        row_size = len(row)
        for j in range(0, row_size - 1, 2):
            metrics_dict[row[j]] = row[j + 1]

    return metrics_dict


if __name__ == "__main__":
    # Example usage
    file_path = Path(
        "C:/Users/mmoin/PYTHON PROJECTS/data-pipeline/data/0_raw data/Harvest_INAVBSKT_ALL.20260226.CSV")
    df0 = extract(file_path)
    df = parse_baskets(df0)
    print(df.head(15))
    print(df.columns.tolist())
    print(df.shape)
    print(df.keys())
