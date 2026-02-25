import pandas as pd
import os
import logging

class BaseParser:
    """Simple file reader that detects type by extension and reads into a DataFrame.
    Can be subclassed to add custom logic for non-standard formats.
    """
    def __init__(self, path: str, logger=logging.getLogger(__name__)):
        self.path = path
        self.logger = logger

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

        self.logger.error(f"Unsupported file type: {ext}")
        raise ValueError(f"Unsupported file type: {ext}")