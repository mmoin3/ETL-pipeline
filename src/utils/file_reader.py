import os

import pandas as pd


class FileReader:
	"""Reusable file reader with extension-based dispatch to pandas readers."""

	def __init__(self, path: str):
		self.path = path

	def load_into_dataframe(self, **kwargs) -> pd.DataFrame:
		ext = os.path.splitext(self.path)[1].lower()
		if ext in {".csv", ".ndm01"}:
			return pd.read_csv(self.path, **kwargs)
		if ext in {".xls", ".xlsx", ".xlsm", ".xlsb"}:
			return pd.read_excel(self.path, **kwargs)
		if ext == ".json":
			lines = kwargs.pop("lines", True)
			return pd.read_json(self.path, lines=lines, **kwargs)
		if ext == ".txt":
			return pd.read_table(self.path, **kwargs)
		raise ValueError(f"Unsupported file type: {ext}")


__all__ = ["FileReader"]