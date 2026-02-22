import pandas as pd
from typing import Dict, Any


class Transformer:
    """Transformer applies business logic to cleaned, validated DataFrames.

    Keep transforms deterministic and testable. No decorators are used.
    """

    def __init__(self):
        pass

    def transform(self, df: pd.DataFrame, rules: Dict[str, Any] = None) -> pd.DataFrame:
        df = df.copy()
        rules = rules or {}

        # example: rename mapping
        rename = rules.get("rename")
        if rename:
            df = df.rename(columns=rename)

        # example: computed columns
        computed = rules.get("computed")
        if computed:
            for col, fn in computed.items():
                # fn is a callable that accepts df and returns a Series
                try:
                    df[col] = fn(df)
                except Exception:
                    # leave column absent on failure
                    pass

        # example: drop columns
        drop = rules.get("drop")
        if drop:
            df = df.drop(columns=[c for c in drop if c in df.columns])

        return df


__all__ = ["Transformer"]
