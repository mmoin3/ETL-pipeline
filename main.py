"""
Main entry point: Configure and run the ingestion pipeline.
Orchestrates file discovery, parsing, and loading through Bronze -> Silver -> Gold layers.
"""
from pathlib import Path
import pandas as pd
from datetime import datetime
import deltalake

from src.parsers.base_parser import BaseParser
from src.parsers.baskets_parser import BasketsParser
from src.pipelines.ingester import write_to_bronze
from config import INBOX_DIR, FAILED_DIR, PROCESSED_DIR, INGESTION_MAPPINGS
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Map parser class names to actual classes
PARSERS_MAP = {
    "BaseParser": BaseParser,
    "BasketsParser": BasketsParser
}


def main():
    """Run ingestion pipeline: discover files -> parse -> bronze -> silver -> gold."""
    bronze_failures = []
    silver_failures = []
    gold_failures = []
    processed_count = 0

    pipeline_start_time = datetime.now()

    for file_path in INBOX_DIR.iterdir():
        if not file_path.is_file():
            continue

        logger.info(f"Processing file: {file_path.name}")

        try:
            parsed = _parse_file(file_path)

            # BRONZE - Fail fast, move file to failed folder
            try:
                ingest_into_bronze(file_path, parsed)
            except Exception as e:
                _move_to_failed(file_path)
                bronze_failures.append((file_path.name, str(e)))
                logger.error(f"✗ BRONZE FAILED: {file_path.name} | {e}")
                continue  # Skip silver/gold for this file

            # SILVER - Log failure, keep file in place
            try:
                process_into_silver(file_path, parsed)
            except Exception as e:
                silver_failures.append((file_path.name, str(e)))
                logger.error(f"✗ SILVER FAILED: {file_path.name} | {e}")
                continue  # Skip gold for this file

            # GOLD - Log failure, keep file in place
            try:
                publish_into_gold(file_path, parsed)
            except Exception as e:
                gold_failures.append((file_path.name, str(e)))
                logger.error(f"✗ GOLD FAILED: {file_path.name} | {e}")
                continue

            # All layers succeeded
            _move_to_processed(file_path)
            processed_count += 1
            logger.info(f"✓ COMPLETED: {file_path.name}")

        except Exception as e:
            logger.error(f"Unexpected error for {file_path.name}: {e}")

    pipeline_end_time = datetime.now()

    # Send email only if there are failures
    if bronze_failures or silver_failures or gold_failures:
        _send_failure_email(
            processed_count,
            bronze_failures,
            silver_failures,
            gold_failures,
            pipeline_start_time,
            pipeline_end_time
        )


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
    # Create target directory if not exists
    bronze_path.mkdir(parents=True, exist_ok=True)

    # Write using deltalake
    if load_type == "replace":
        deltalake.write_deltalake(str(bronze_path), df, mode="overwrite")
    else:  # append
        deltalake.write_deltalake(str(bronze_path), df, mode="append")


def process_into_silver(file_path: Path, parsed_data: dict) -> None:
    pass


def publish_into_gold(file_path: Path, parsed_data: dict) -> None:
    pass


def _move_to_processed(file_path: Path) -> None:
    """Move file to processed folder after successful pipeline completion."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    new_path = PROCESSED_DIR / file_path.name
    file_path.rename(new_path)


def _move_to_failed(file_path: Path) -> None:
    """Move file to failed folder if bronze layer fails."""
    FAILED_DIR.mkdir(parents=True, exist_ok=True)
    new_path = FAILED_DIR / file_path.name
    file_path.rename(new_path)


if __name__ == "__main__":
    main()
