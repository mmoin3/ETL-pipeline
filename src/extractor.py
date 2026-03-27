from duckdb import df
import pandas as pd
import csv
from pathlib import Path
import logging
from src.utils.logger import get_logger

logger = get_logger(__name__)


def extract(file_path: Path, ext_override: str = None, **kwargs) -> pd.DataFrame:
    """
    Simple extracter function that can handle 80% of files by just reading them into a dataframe.

    Args:
        file_path: Path to the input file.
        ext_override: Optional extension to override the file's extension.
        **kwargs: Additional arguments for specific parsing functions.
    """
    if ext_override:
        ext = ext_override.lower().lstrip(".")
    else:
        ext = file_path.suffix.lower().lstrip(".")

    try:
        if ext in {"csv", "ndm01"}:
            return pd.read_csv(file_path, **kwargs)
        elif ext in {"xls", "xlsx", "xlsm", "xlsb"}:
            return pd.read_excel(
                file_path,
                sheet_name=kwargs.pop("sheet_name", 0),
                engine=kwargs.pop("engine", "openpyxl"),
                **kwargs,
            )
    except Exception as e:
        try:
            return _extract_complex(file_path, **kwargs)
        except Exception as e2:
            logger.error(f"Failed to extract {file_path.name}\n"
                         f"with both simple and complex methods: {e}, {e2}")
            raise e2
    raise ValueError(
        f"Unsupported file type '{ext}' for file {file_path.name}")


def _extract_complex(file_path: Path, **csv_kwargs) -> pd.DataFrame:
    """Extracter function for irregular files that can't be read by pandas.
    Reads the file line by line and splits by a delimiter.

    Args:
        file_path: Path to the input file.
        delimiter: Delimiter to use for splitting the lines.
        skip_empty: Whether to skip empty lines.
        **csv_kwargs: Additional arguments for the CSV reader.
    """
    records = []
    with open(file_path, "r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=csv_kwargs.pop(
            "delimiter", ","), **csv_kwargs)
        for row in reader:
            if csv_kwargs.pop("skip_empty", False) and not any(row):
                continue
            records.append(row)
    return pd.DataFrame(records)


if __name__ == "__main__":
    # Example usage
    file_path = Path(
        r"C:\Users\mmoin\PYTHON PROJECTS\data-pipeline\data\0_raw data\Harvest_INAVBSKT_ALL.20260226.CSV")
    df = extract(file_path)
    print(df.head())
