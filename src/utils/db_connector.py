import sqlite3
from typing import Optional
import pandas as pd


class SQLiteUploader:
	"""Simple SQLite uploader for development and testing.

	- Creates a table based on DataFrame dtypes (simple mapping)
	- Inserts rows using executemany
	"""

	def __init__(self, db_path: str = ":memory:"):
		self.db_path = db_path
		self.conn = sqlite3.connect(self.db_path)

	def close(self) -> None:
		try:
			self.conn.close()
		except Exception:
			pass

	def create_table_from_df(self, table_name: str, df: pd.DataFrame) -> None:
		cols_def = []
		for c, dtype in zip(df.columns, df.dtypes):
			if pd.api.types.is_integer_dtype(dtype):
				t = "INTEGER"
			elif pd.api.types.is_float_dtype(dtype):
				t = "REAL"
			else:
				# text for strings, datetimes, objects
				t = "TEXT"
			cols_def.append(f'"{c}" {t}')

		ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols_def)});'
		cur = self.conn.cursor()
		cur.execute(ddl)
		self.conn.commit()

	def upload_df(self, df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> None:
		cur = self.conn.cursor()
		if if_exists == "replace":
			cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
			self.conn.commit()
			self.create_table_from_df(table_name, df)
		else:
			# if append and table does not exist, create it
			self.create_table_from_df(table_name, df)

		cols = list(df.columns)
		placeholders = ",".join(["?"] * len(cols))
		col_list = ",".join([f'"{c}"' for c in cols])
		insert_sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})'

		rows = []
		for row in df.itertuples(index=False, name=None):
			# convert numpy/nan to None for sqlite
			converted = tuple(None if pd.isna(x) else x for x in row)
			rows.append(converted)

		if rows:
			cur.executemany(insert_sql, rows)
			self.conn.commit()

	def query(self, sql: str):
		cur = self.conn.cursor()
		cur.execute(sql)
		return cur.fetchall()


__all__ = ["SQLiteUploader"]
