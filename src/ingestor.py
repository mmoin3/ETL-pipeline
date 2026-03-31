from src.utils.logger import get_logger
from pathlib import Path
from typing import Union

import deltalake
import pandas as pd

logger = get_logger(__name__)


def ingest(df: pd.DataFrame, source_name: str, current_time: pd.Timestamp,
           target_path: Union[str, Path], batch_id: str, write_mode: str = "append") -> pd.DataFrame:
    """
    Load a DataFrame to the bronze Delta table. Adds metadata columns
    and writes data in append mode by default.

    Args:
        df:           Input DataFrame.
        source_name:  Source file name.
        current_time: Timestamp for ingestion.
        target_path:  Destination Delta table path.
        batch_id:     Unique identifier for this ingestion batch.
        write_mode:   "append" or "overwrite" (default: "append").
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    if write_mode not in {"append", "overwrite"}:
        raise ValueError(
            f"Invalid write_mode '{write_mode}'. Use 'append' or 'overwrite'."
        )

    # Add metadata columns
    out = df.copy()
    out["_ingested_at"] = current_time  # convert to EST timezone if needed
    out["_source_name"] = source_name
    out["_batch_id"] = batch_id

    target_path = Path(target_path)
    target_path.mkdir(parents=True, exist_ok=True)

    deltalake.write_deltalake(target_path, out, mode=write_mode)

    logger.info(
        "Loaded into bronze | batch=%s | source=%s | mode=%s | rows=%s | target=%s",
        batch_id, source_name, write_mode, len(out), target_path
    )

    return out
