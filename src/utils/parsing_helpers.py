import csv
import io
from typing import Dict, Iterable, List, Optional

import pandas as pd


def split_blocks(lines: Iterable[str], start_token: str = "TRADE_DATE") -> List[List[str]]:
    """Split raw lines into blocks beginning with start_token."""
    normalized = list(lines)
    starts = [idx for idx, line in enumerate(normalized) if line.strip().upper().startswith(start_token)]
    if not starts:
        return []

    blocks: List[List[str]] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if (index + 1) < len(starts) else len(normalized)
        blocks.append(normalized[start:end])
    return blocks


def find_header_index(block_lines: List[str], markers: Optional[List[str]] = None) -> Optional[int]:
    """Return index of holdings header row inside one block."""
    search_markers = markers or ["CUSIP", "TICKER", "DESCRIPTION"]
    for index, line in enumerate(block_lines):
        upper_line = line.strip().upper()
        if any(marker in upper_line for marker in search_markers):
            return index
    return None


def parse_metadata_lines(metadata_lines: List[str]) -> Dict[str, str]:
    """Parse BSKT metadata lines into a key-value dict."""
    if not metadata_lines:
        return {}

    metadata: Dict[str, str] = {}

    first_fields = next(csv.reader([metadata_lines[0]]))
    if len(first_fields) >= 2:
        metadata[first_fields[0].strip().upper()] = first_fields[1]
    if len(first_fields) >= 3:
        metadata["SS_LONG_CODE"] = first_fields[2]
    if len(first_fields) >= 5:
        metadata["FULL_NAME"] = first_fields[4]
    if len(first_fields) >= 6:
        metadata["TICKER"] = first_fields[5]
    if len(first_fields) >= 8:
        metadata["BASE_CURRENCY"] = first_fields[7]

    for line in metadata_lines[1:]:
        fields = next(csv.reader([line]))
        for offset in range(0, len(fields) - 1, 2):
            key = fields[offset].strip().upper()
            value = fields[offset + 1]
            if key:
                metadata[key] = value

    return metadata


def convert_metadata_types(meta: Dict[str, str], type_map: Dict[str, object]) -> Dict[str, object]:
    """Normalize nulls and cast metadata values based on a type map."""
    cleaned: Dict[str, object] = {}

    for key, value in meta.items():
        target_type = type_map.get(key, str)
        try:
            if value is None or str(value).strip() == "":
                cleaned[key] = None
            elif target_type == "datetime64[ns]":
                cleaned[key] = pd.to_datetime(value, errors="coerce")
            elif target_type == float:
                cleaned[key] = float(value)
            elif target_type == int:
                cleaned[key] = int(float(value))
            elif target_type == str:
                cleaned[key] = str(value).strip().lstrip("'").upper()
            else:
                cleaned[key] = value
        except Exception:
            cleaned[key] = None

    return cleaned


def parse_holdings_csv(holdings_lines: List[str]) -> pd.DataFrame:
    """Parse holdings lines into a DataFrame using pandas CSV parser."""
    if not holdings_lines:
        return pd.DataFrame()
    return pd.read_csv(io.StringIO("".join(holdings_lines)))


def clean_holdings_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean object columns: trim, strip leading apostrophe, uppercase."""
    output = df.copy()
    object_columns = output.select_dtypes(include="object").columns
    if object_columns.empty:
        return output

    output[object_columns] = output[object_columns].apply(
        lambda series: series.astype(str).str.strip().str.lstrip("'").str.upper()
    )
    return output
