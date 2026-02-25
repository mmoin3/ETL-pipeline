from logger_config import setup_logger, LOG_FILE, LOG_LEVEL
import pandas as pd
import os

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