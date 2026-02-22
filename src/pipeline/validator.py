import pandas as pd
from typing import Dict, List


class Validator:
    """Validator runs schema and simple business-rule checks on DataFrames.

    Usage: v = Validator(); result = v.validate(df, schema)
    schema example: { 'col_a': {'required': True, 'type': 'numeric'}, 'col_b': {'required': False} }
    """

    def __init__(self):
        pass

    def validate(self, df: pd.DataFrame, schema: Dict) -> Dict:
        errors: List[str] = []
        # required columns
        for col, cfg in schema.items():
            if cfg.get("required") and col not in df.columns:
                errors.append(f"missing required column: {col}")

        # simple type checks
        for col, cfg in schema.items():
            if col not in df.columns:
                continue
            expected = cfg.get("type")
            if expected == "numeric":
                if not pd.api.types.is_numeric_dtype(df[col]):
                    errors.append(f"column {col} expected numeric")
            elif expected == "string":
                if not pd.api.types.is_string_dtype(df[col]):
                    errors.append(f"column {col} expected string")
            elif expected == "datetime":
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    errors.append(f"column {col} expected datetime")

        valid = len(errors) == 0
        return {"valid": valid, "errors": errors}


__all__ = ["Validator"]
