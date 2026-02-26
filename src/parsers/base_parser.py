import io, os, logging, csv
import pandas as pd

class BaseParser:
    """Simple file reader that detects type by extension and reads into a DataFrame.
    Can be subclassed to add custom logic for non-standard formats.
    """
    def __init__(self, path: str, logger=logging.getLogger(__name__)):
        self.path = path
        self.logger = logger

    def read_into_dataframe(self, **kwargs) -> pd.DataFrame:
        """Read file and return DataFrame. Detect type by extension and dispatch.
        Data must already be in tabular for this method to work in isolation
        """
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
            self.logger.error(f"Unsupported file type: {ext}")

  
    def list_to_dataframe(self, lines: list[str], **kwargs) -> pd.DataFrame:
        """Convert a list of lines to a pandas DataFrame. Log errors if logger is provided."""
        try:
            if not lines:
                return pd.DataFrame()
            csv_text = "\n".join(line.rstrip("\r\n") for line in lines)
            df = pd.read_csv(io.StringIO(csv_text), **kwargs)
            return df
        except Exception as e:
            self.logger.error(f"Failed to parse lines to DataFrame: {e}")
            return pd.DataFrame()
        
    def list_to_blocks(self, lines: list[str], start_marker="TRADE_DATE") -> list[list[str]]:
        """Split file into blocks based on lines that start with a marker. Logs errors if logger is provided."""
        try:
            starts = [idx for idx, ln in enumerate(lines) if
                      ln.strip().lstrip("'").upper().startswith(
                          start_marker.strip().lstrip("'").upper())]
            blocks = []
            for idx, start in enumerate(starts):
                if (idx + 1) < len(starts):
                    end = starts[idx + 1]
                else:
                    end = len(lines)
                blocks.append(lines[start:end])
            return blocks
        except Exception as e:
            self.logger.error(f"Failed to get blocks: {e}")
            return []
        
    def get_header_idx(self, lines, header_marker) -> int:
        """Find the first line that contains the header marker and return it as a list of column names.line could also be a pandas dataaframe header row"""
        for idx, line in enumerate(lines):
            if header_marker in line:
                return idx
    