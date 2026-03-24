"""
BaseParser: Simple CSV/tabular file reader.

Reads tabular files (CSV, Excel, JSON, Parquet, etc.) and returns them as a tuple
containing a single DataFrame with normalized column names.
"""

import pandas as pd
from pathlib import Path


class BaseParser:
    """Read tabular files and return as normalized DataFrame."""
    
    def __init__(self, file_path: str):
        """Initialize parser with file path.
        
        Args:
            file_path: Path to file to parse.
        """
        self.path = Path(file_path)
    
    def parse(self, ext_override: str = None, **kwargs) -> pd.DataFrame:
        """Read file based on extension and return DataFrame.
        
        Args:
            ext_override: Override file extension detection.
            **kwargs: Passed to pandas read function.
        """
        if ext_override:
            ext = ext_override.lstrip(".").lower()
        else:
            ext = self.path.suffix.lstrip(".").lower()
        
        if ext in {"csv", "ndm01"}:
            return pd.read_csv(self.path, **kwargs)
        elif ext in {"xls", "xlsx", "xlsm", "xlsb"}:
            return pd.read_excel(self.path, **kwargs)
        elif ext == "json":
            return pd.read_json(self.path, lines=kwargs.pop("lines", True), **kwargs)
        elif ext == "txt":
            return pd.read_table(self.path, **kwargs)
        elif ext == "parquet":
            return pd.read_parquet(self.path, **kwargs)
        else:
            raise ValueError(f"Unsupported file type: .{ext}")
    


    