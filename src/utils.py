import pandas as pd
import os
import io

def log_error(logger, msg):
    """Log an error message if logger is provided."""
    if logger:
        logger.error(msg)

def load_into_dataframe(path, *args, logger=None, **kwargs) -> pd.DataFrame:
    """Read file and return DataFrame. Detect type by extension and dispatch. Logs errors if logger is provided."""
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext in {".csv", ".ndm01"}:
            return pd.read_csv(path, **kwargs)
        if ext in {".xls", ".xlsx", ".xlsm", ".xlsb"}:
            return pd.read_excel(path, **kwargs)
        if ext == ".json":
            lines = kwargs.pop("lines", True)
            return pd.read_json(path, lines=lines, **kwargs)
        if ext == ".txt":
            return pd.read_table(path, **kwargs)
        else:
            raise ValueError(f"Unsupported file type: {ext}")
    except Exception as e:
        log_error(logger, f"Failed to load file {path}: {e}")
        return pd.DataFrame()

def read_lines(path, logger=None) -> list:
    """Read a text file and return a list of lines. Logs errors if logger is provided."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.readlines()
    except Exception as e:
        log_error(logger, f"Failed to read lines from {path}: {e}")
        return []

def lines_to_df(lines, logger=None, **kwargs) -> pd.DataFrame:
    """
    Convert a list of lines to a pandas DataFrame. Log errors if logger is provided.
    """
    try:
        df = pd.read_csv(io.StringIO("".join(lines)), **kwargs)
        return df
    except Exception as e:
        log_error(logger, f"Failed to parse lines to DataFrame: {e}")
        return pd.DataFrame()

def get_blocks(lines: list, start_marker="TRADE_DATE", logger=None) -> list[list[str]]:
    """Split file into blocks based on lines that start with a marker. Logs errors if logger is provided."""
    try:
        starts = [idx for idx, ln in enumerate(lines) if ln.strip().upper().startswith(start_marker)]
        blocks = []
        for idx, start in enumerate(starts):
            if (idx + 1) < len(starts):
                end = starts[idx + 1]
            else:
                end = len(lines)
            blocks.append(lines[start:end])
        return blocks
    except Exception as e:
        log_error(logger, f"Failed to get blocks: {e}")
        return []