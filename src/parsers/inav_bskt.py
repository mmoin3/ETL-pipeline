"""
INAVBskt Parser: Parse non-tabular INAV and BSKT files.

These files are non-tabular vertical reports separated into blocks,
where each block represents a single fund. Each block begins with a 'TRADE_DATE' line.

Structure:
- Primary metadata: Key-value pairs in the first few lines
- Holdings table: Starts at the header row (contains 'CUSIP', 'TICKER', or 'DESCRIPTION')
  and continues until the next TRADE_DATE or end of file
"""

import logging
import csv
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


class INAVBskt:
    """Parser for INAV and BSKT non-tabular files. Returns two DataFrames: metrics and holdings."""
    
    def __init__(self, file_path: str):
        """Initialize parser with file path.
        
        Args:
            file_path: Path to INAV/BSKT file.
        """
        self.path = Path(file_path)
    
    def parse(self, **kwargs) -> tuple:
        """Parse file and return tuple of (metrics_df, holdings_df).
        """
        try:
            parsed_rows = self._read_rows()
            if not parsed_rows:
                return (pd.DataFrame(), pd.DataFrame())
            
            fund_blocks = self._split_blocks(parsed_rows, start_marker="TRADE_DATE")
            
            metrics_list = []
            holdings_list = []
            
            for block in fund_blocks:
                header_idx = self._find_header_idx(block, markers={"CUSIP", "TICKER", "DESCRIPTION"})
                if header_idx == -1:
                    continue
                
                # Extract metadata and holdings from this block
                metrics_df = self._extract_metadata(block[:header_idx])
                holdings_df = self._rows_to_df(block[header_idx:])
                
                # Add fund identifiers to holdings
                if not metrics_df.empty and not holdings_df.empty:
                    holdings_df["TRADE_DATE"] = metrics_df["TRADE_DATE"].iloc[0]
                    holdings_df["SS Long Code"] = metrics_df["SS Long Code"].iloc[0]
                    metrics_list.append(metrics_df)
                    holdings_list.append(holdings_df)
            
            metrics = pd.concat(metrics_list, ignore_index=True) if metrics_list else pd.DataFrame()
            holdings = pd.concat(holdings_list, ignore_index=True) if holdings_list else pd.DataFrame()
            
            return (metrics, holdings)
        
        except Exception as e:
            logger.error(f"Failed to parse {self.path.name}: {e}")
            return (pd.DataFrame(), pd.DataFrame())
    
    def _read_rows(self) -> list[list[str]]:
        """Read file line-by-line and parse each line."""
        rows = []
        with open(self.path, "r", encoding="utf-8", errors="replace") as f:
            for raw_line in f:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                rows.append(self._parse_line(line))
        return rows
    
    def _parse_line(self, line: str) -> list[str]:
        """Parse a line using CSV reader, fallback to tab split."""
        try:
            fields = next(csv.reader([line], skipinitialspace=True))
        except Exception:
            fields = [line]
        
        if len(fields) <= 1 and "\t" in line:
            fields = line.split("\t")
        
        return [field.strip() for field in fields]
    
    def _split_blocks(self, rows: list[list[str]], start_marker: str) -> list[list[list[str]]]:
        """Split parsed rows into blocks where first column starts with marker."""
        blocks = []
        current_block = []
        normalized_marker = start_marker.upper()
        
        for row in rows:
            first_cell = row[0].upper() if row and row[0] else ""
            if first_cell.startswith(normalized_marker) and current_block:
                blocks.append(current_block)
                current_block = []
            current_block.append(row)
        
        if current_block:
            blocks.append(current_block)
        
        return blocks
    
    def _find_header_idx(self, rows: list[list[str]], markers: set[str]) -> int:
        """Find first row where any marker is present as a field."""
        normalized_markers = {m.upper() for m in markers}
        for idx, row in enumerate(rows):
            row_values = {v.upper() for v in row if v}
            if normalized_markers.intersection(row_values):
                return idx
        return -1
    
    def _rows_to_df(self, rows: list[list[str]]) -> pd.DataFrame:
        """Convert parsed rows to DataFrame, handling uneven row lengths."""
        if not rows:
            return pd.DataFrame()
        
        header = rows[0]
        data_rows = rows[1:]
        
        max_len = max([len(header)] + [len(r) for r in data_rows]) if data_rows else len(header)
        
        # Pad header if needed
        column_names = header + [f"col_{i}" for i in range(len(header), max_len)]
        
        # Pad data rows
        padded_data = []
        for row in data_rows:
            padded_data.append(row + [""] * (max_len - len(row)))
        
        df = pd.DataFrame(padded_data, columns=column_names)
        return df.dropna(how="all")
    
    def _extract_metadata(self, chunk: list[list[str]]) -> pd.DataFrame:
        """Extract metadata from top lines of block."""
        if not chunk:
            return pd.DataFrame()
        
        metadata = {}
        first_row = chunk[0]
        
        # Extract key metadata fields from first row
        metadata.update({
            "TRADE_DATE": self._safe_get(first_row, 1),
            "SS Long Code": self._safe_get(first_row, 2),
            "Full Name": self._safe_get(first_row, 4),
            "Ticker 1": self._safe_get(first_row, 5),
            "Ticker 2": self._safe_get(first_row, 6),
            "Base Currency": self._safe_get(first_row, 7),
            "Instruction Asset": self._safe_get(first_row, 9)
        })
        
        # Extract key-value pairs from remaining lines
        metadata.update(self._pairs_to_dict(chunk[1:]))
        
        return pd.DataFrame([metadata])
    
    def _pairs_to_dict(self, rows: list[list[str]], step: int = 2) -> dict:
        """Flatten rows into key-value dict."""
        result = {}
        for row in rows:
            for i in range(0, len(row) - 1, step):
                key = row[i] if row[i] else None
                value = row[i + 1] if i + 1 < len(row) else None
                if key and value:
                    result[key] = value
        return result
    
    def _safe_get(self, row: list[str], idx: int) -> str:
        """Safely get row value by index."""
        return row[idx] if idx < len(row) else ""