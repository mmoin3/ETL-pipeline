import pandas as pd
from typing import List, Dict, Optional


def impute_numeric_median(df: pd.DataFrame, cols: Optional[List[str]] = None) -> pd.DataFrame:
    df = df.copy()
    cols = cols or df.select_dtypes(include=["number"]).columns.tolist()
    for c in cols:
        median = df[c].median()
        df[c] = df[c].fillna(median)
    return df


def impute_categorical_mode(df: pd.DataFrame, cols: Optional[List[str]] = None) -> pd.DataFrame:
    df = df.copy()
    cols = cols or df.select_dtypes(exclude=["number"]).columns.tolist()
    for c in cols:
        try:
            mode = df[c].mode(dropna=True)
            if not mode.empty:
                df[c] = df[c].fillna(mode.iloc[0])
        except Exception:
            # fallback: fill with empty string
            df[c] = df[c].fillna("")
    return df


def fill_missing(df: pd.DataFrame, strategy: Dict[str, Dict]) -> pd.DataFrame:
    """Fill missing values according to a provided strategy.

    strategy example:
      { 'col_a': {'method': 'median'}, 'col_b': {'method': 'constant', 'value': 0} }
    """
    df = df.copy()
    for col, cfg in strategy.items():
        method = cfg.get("method")
        if method == "median":
            df[col] = df[col].fillna(df[col].median())
        elif method == "mean":
            df[col] = df[col].fillna(df[col].mean())
        elif method == "mode":
            mode = df[col].mode(dropna=True)
            if not mode.empty:
                df[col] = df[col].fillna(mode.iloc[0])
        elif method == "constant":
            df[col] = df[col].fillna(cfg.get("value"))
        else:
            # unknown method: no-op
            pass
    return df


def parse_dates(df: pd.DataFrame, cols: List[str], format: Optional[str] = None) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        df[c] = pd.to_datetime(df[c], format=format, errors="coerce")
    return df


__all__ = ["impute_numeric_median", "impute_categorical_mode", "fill_missing", "parse_dates"]
