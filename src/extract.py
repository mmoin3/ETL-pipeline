import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
import logging
from config import setup_logger
from config import LOG_FILE, LOG_LEVEL, NAV_METADATA_TYPE_MAP

logger = logging.getLogger(__name__)

def get_inav_data()

class FileReader:
    """Simple file reader that detects type by extension and reads into a DataFrame.
    Can be subclassed to add custom logic for non-standard formats.
    """
    def __init__(self, path: str):
        self.path = path
        def __init__(self, path: str, logger=None):
            super().__init__(path)
            self.logger = logger or setup_logger(log_file=LOG_FILE, level=LOG_LEVEL)

    def load_into_dataframe(self, **kwargs) -> pd.DataFrame:
        """Read file and return DataFrame. Detect type by extension and dispatch."""
        ext = os.path.splitext(self.path)[1].lower()
        if ext in {".csv",".ndm01"}:
            return pd.read_csv(self.path, **kwargs)
        if ext in {".xls", ".xlsx", ".xlsm", ".xlsb"}:
            return pd.read_excel(self.path, **kwargs)
        if ext == ".json":
            lines = kwargs.pop("lines", True)
            return pd.read_json(self.path, lines=lines, **kwargs)
        if ext == ".txt":
            return pd.read_table(self.path, **kwargs)
        else:
            raise ValueError(f"Unsupported file type: {ext}")
        
class BSKTFile(FileReader):
    """Readable, minimal parser for BSKT-style files (multiple fund blocks).

    Each block starts with a line beginning with 'TRADE_DATE'. Metadata
    occupies the first few lines of the block (pairs of key,value fields).
    The holdings table begins on the line that contains the header (usually
    includes 'CUSIP' or 'TICKER') and continues until the next TRADE_DATE
    or end of file.
    """

    def extract(self, lines=None):
        """
        Return a list of fund dicts: {'metadata': {...}, 'holdings': DataFrame}.
        Accepts a list of lines (from CSV, Excel, etc.).
        If lines is None, reads from self.path as text lines (default CSV/txt).
        """
        if lines is None:
            with open(self.path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        return self._parse_blocks(lines)

    def _parse_blocks(self, lines):
        """Split file into blocks, then parse each block separately.

        Holdings parsing uses io.StringIO and pandas.read_csv to robustly handle CSV quirks:
        - Handles embedded commas, quoted fields, and inconsistent delimiters.
        - Avoids manual parsing errors (e.g., split failures on numbers like 40,000).
        """
        # Find indices where a block starts
        starts = [idx for idx, ln in enumerate(lines) if ln.strip().upper().startswith("TRADE_DATE")]
        
        # Split into blocks
        blocks = []
        for i, start in enumerate(starts):
            if (i + 1) < len(starts):
                end = starts[i + 1]
            else:
                end = len(lines)
            blocks.append(lines[start:end])

        # Parse each block into fund metadata and holdings
        funds = []
        for block_lines in blocks:
            # Find holdings header in this block
            header_idx = None
            for j, ln in enumerate(block_lines):
                l = ln.strip().upper()
                if "CUSIP" in l or "TICKER" in l or "DESCRIPTION" in l:
                    header_idx = j
                    break
            if header_idx is None:
                continue
            metadata_lines = block_lines[:header_idx]
            holdings_lines = block_lines[header_idx:]

            # Parse metadata
            metadata = {}
            if metadata_lines:
                # Special handling for the first line
                first_fields = next(csv.reader([metadata_lines[0]]))
                metadata[first_fields[0]] = first_fields[1]
                metadata["SS_LONG_CODE"] = first_fields[2]
                metadata["FULL_NAME"]= first_fields[4]
                metadata["TICKER"] = first_fields[5]
                metadata["BASE_CURRENCY"] = first_fields[7]
                # Parse remaining lines as key-value pairs using csv.reader
                for line in metadata_lines[1:]:
                    fields = next(csv.reader([line]))
                    for i in range(0, len(fields) - 1, 2):
                        key = fields[i]
                        val = fields[i + 1]
                        metadata[key] = val
            # Clean and convert metadata types
            metadata = self._clean_and_convert_metadata(metadata)

            # Parse Holdings data
            try:
                holdings_df = pd.read_csv(io.StringIO("".join(holdings_lines)))
                holdings_df = self._clean_holdings_df(holdings_df)
            except Exception as e:
                self.logger.error(f"Failed to parse holdings block: {e}")
                holdings_df = pd.DataFrame()  # fallback to empty DataFrame

            funds.append({"metadata": metadata, "holdings": holdings_df})
        return funds

    def _clean_and_convert_metadata(self, meta):
        def is_null(val):
            return val is None or str(val).strip() in ('', ' ')
        cleaned = {}
        for k, v in meta.items():
            typ = NAV_METADATA_TYPE_MAP.get(k, str)
            try:
                if is_null(v):
                    cleaned[k] = None
                elif typ == "datetime64[ns]":
                    cleaned[k] = pd.to_datetime(v, errors="coerce")
                elif typ == float:
                    cleaned[k] = float(v)
                elif typ == int:
                    cleaned[k] = int(float(v))
                elif typ == str:
                    cleaned[k] = str(v).strip().lstrip("'").upper()
                else:
                    cleaned[k] = v
            except Exception:
                cleaned[k] = None
        return cleaned

    def _clean_holdings_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Target only 'object' (string-like) columns
        str_cols = df.select_dtypes(include='object').columns
        if not str_cols.empty:
            # Vectorized cleaning: strip whitespace, remove leading ', and uppercase
            df[str_cols] = df[str_cols].apply(lambda x: x.astype(str).str.strip().str.lstrip("'").str.upper())
            
        return df

__all__ = ["FileReader", "BSKTFile"]