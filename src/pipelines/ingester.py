import pandas as pd
import deltalake
from pathlib import Path
from config import INGESTION_MAPPINGS
from src.parsers.base_parser import BaseParser
from src.parsers.baskets_parser import BasketsParser
from src.utils.logger import get_logger

PARSERS_MAP = {
    "BaseParser": BaseParser,
    "BasketsParser": BasketsParser
}

logger = get_logger(__name__)


def ingest_into_bronze(df: pd.DataFrame, bronze_path: Path, load_type: str) -> None:
    """Write DataFrame to Bronze layer as Delta Lake table.

    Args:
        df: DataFrame to write.
        bronze_path: Target delta table path in bronze layer.
        load_type: delta lake write mode, either "append" or "replace".
    """
    # df["_source_file"] = file_path.name
    # df["_load_type"] = load_type
    # df["_ingested_at"] = pd.Timestamp.now(tz="US/Eastern")

    # Determine target path and write mode
    bronze_path.mkdir(parents=True, exist_ok=True)

    # Write using deltalake
    if load_type == "replace":
        deltalake.write_deltalake(str(bronze_path), df, mode="overwrite")
    else:  # append
        deltalake.write_deltalake(str(bronze_path), df, mode="append")


def ingest_into_bronze(file_path: Path, parsed_data: dict) -> None:
    """Load parsed data to Bronze layer."""
    mapping = _get_file_mapping(file_path)

    for df_key, target_table in mapping["outputs"].items():
        if df_key not in parsed_data:
            raise ValueError(
                f"Expected key '{df_key}' not found in parsed data for {file_path.name}")

        df = parsed_data[df_key]
        load_type = mapping["load_type"]
        destination_path = target_table
        ingest_into_bronze(df, destination_path, load_type)
