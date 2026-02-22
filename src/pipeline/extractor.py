import os
import csv
from typing import Optional
import pandas as pd


class FileReader:
    """Simple file reader that detects type by extension and reads into a DataFrame.

    Can be subclassed to add custom logic for non-standard formats.
    """

    def __init__(self, path: str, probe_header: bool = True):
        self.path = path
        self.file_type = self._detect_type()
        self.probe_header = probe_header

    def _detect_type(self) -> str:
        """Detect file type by extension."""
        ext = os.path.splitext(self.path)[1].lower()
        if ext == ".csv":
            return "csv"
        if ext in {".xls", ".xlsx"}:
            return "excel"
        if ext == ".json":
            return "json"
        if ext == ".txt":
            return "txt"
        return "unknown"

    def _sniff_csv(self) -> dict:
        """Return dict with detected CSV header and delimiter."""
        info = {"has_header": None, "delimiter": None}
        try:
            with open(self.path, "r", encoding="utf-8", errors="ignore") as f:
                sample = f.read(8192)
            sniffer = csv.Sniffer()
            info["has_header"] = bool(sniffer.has_header(sample))
            info["delimiter"] = sniffer.sniff(sample).delimiter
        except Exception:
            pass
        return info

    def read(self, **kwargs) -> pd.DataFrame:
        """Read file and return DataFrame. Dispatch by detected file type."""
        if self.file_type == "csv":
            return self._read_csv(**kwargs)
        if self.file_type == "excel":
            return self._read_excel(**kwargs)
        if self.file_type == "json":
            return self._read_json(**kwargs)
        if self.file_type == "txt":
            return self._read_txt(**kwargs)
        raise ValueError(f"Unsupported file type: {self.file_type}")

    def _read_csv(self, **kwargs) -> pd.DataFrame:
        """Read CSV, optionally probing header/delimiter."""
        reader_kwargs = dict(kwargs)
        if self.probe_header and "header" not in reader_kwargs and "sep" not in reader_kwargs:
            hints = self._sniff_csv()
            if hints.get("delimiter"):
                reader_kwargs.setdefault("sep", hints["delimiter"])
            if hints.get("has_header") is False:
                reader_kwargs.setdefault("header", None)
        return pd.read_csv(self.path, **reader_kwargs)

    def _read_excel(self, **kwargs) -> pd.DataFrame:
        """Read Excel (first sheet by default)."""
        return pd.read_excel(self.path, **kwargs)

    def _read_json(self, **kwargs) -> pd.DataFrame:
        """Read JSON (newline-delimited by default)."""
        lines = kwargs.pop("lines", True)
        return pd.read_json(self.path, lines=lines, **kwargs)

    def _read_txt(self, **kwargs) -> pd.DataFrame:
        """Read TXT as tab-delimited table."""
        return pd.read_table(self.path, **kwargs)


__all__ = ["FileReader"]
