import pandas as pd
from config import *

class DataFrameCleaner:
    """Clean and typecast DataFrames with optional schema."""

    def clean(self, df: pd.DataFrame, schema: dict = None) -> pd.DataFrame:
        """Clean DataFrame: cast schema columns, clean string/object columns, leave others as-is."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected pd.DataFrame, got {type(df)}")
        
        schema = schema or {}
        for col in df.columns:
            if col in schema:
                df[col] = self.cast(df[col], schema[col])
            else:
                df[col] = self._clean_str(df[col])
        return df

    def _clean_str(self, s: pd.Series) -> pd.Series:
        """Clean strings: strip, remove leading apostrophes, replace null-like values with pd.NA."""
        return (s.astype(str).str.strip().str.lstrip("'")
                .replace(NULL_LIKE_VALUES, pd.NA).astype("string"))

    def _strip_numeric_chars(self, series: pd.Series) -> pd.Series:
        """Extract numeric values, strip non-numeric chars."""
        if pd.api.types.is_numeric_dtype(series):
            return series
        s = series.astype(str).str.strip()
        cleaned = s.str.extract(r"(-?)[\d.,]+", expand=False)
        return pd.to_numeric(cleaned, errors="coerce")

    def _parse_percent(self, series: pd.Series) -> pd.Series:
        """Extract numeric values from percentage strings and divide by 100."""
        s = self._strip_numeric_chars(series)
        return s / 100.0

    def cast(self, s: pd.Series, target_type) -> pd.Series:
        """Cast series to target type using dispatch dict.
        Args:
            s: Series that will be casted
            target_type: Target type (e.g., float, int, str, "pct", datetime types)
        """
        type_handlers = {
            float: self._strip_numeric_chars,
            int: lambda x: self._strip_numeric_chars(x).round().astype("Int64"),
            str: self._clean_str,
            "pct": self._parse_percent,
        }
        
        handler = type_handlers.get(target_type)
        if handler:
            return handler(s)
        
        # Handle date types: datetime64[ns], datetime64, datetime, timestamp, date
        if str(target_type).lower() in ["datetime64[ns]", "datetime64", "datetime", "timestamp", "date"]:
            return self.parse_dates(self._clean_str(s))
        
        return s

    def parse_dates(self, s: pd.Series) -> pd.Series:
        """Parse dates using pandas auto-parsing."""
        return pd.to_datetime(s, errors="coerce")