from numpy import dtype
import polars as pl
import re
from pathlib import Path
import duckdb


def clean_basic(df: pl.LazyFrame, config: dict) -> pl.LazyFrame:
    """Consolidated rename and vectorized cast."""

    # Get current names from the LazyFrame schema
    current_cols = df.collect_schema().names()
    custom_map = config.get("columns_map", {})

    # 1. BUILD A SINGLE MAPPING
    # Priority: 1. Custom Map, 2. Snake_case conversion
    final_mapping = {}
    for col in current_cols:
        if col in custom_map:
            final_mapping[col] = custom_map[col]
        else:
            # Fallback to snake_case
            new_name = col.strip().lower().replace(" ", "_")
            if new_name != col:
                final_mapping[col] = new_name

    if final_mapping:
        df = df.rename(final_mapping)

    # 2. APPLY SCHEMA-BASED TYPE CASTING
    schema = config.get("schema", {})
    expressions = []

    # Get the names AFTER the rename to ensure Step 3 finds the right targets
    post_rename_cols = df.collect_schema().names()

    for orig_col, dtype in schema.items():
        # Determine what the column is named NOW
        target_col = custom_map.get(
            orig_col, orig_col.strip().lower().replace(" ", "_"))

        if target_col not in post_rename_cols:
            print(
                f"Warning: Expected column '{target_col}' not found after rename.")
            continue

        # Regex for numbers (handling commas and signs)
        num_expr = pl.col(target_col).str.replace_all(
            ",", "").str.extract(r"(-?[\d.]+)")

        if dtype in [float, "float"]:
            expressions.append(num_expr.cast(
                pl.Float64, strict=False).alias(target_col))

        elif dtype in [int, "int"]:
            expressions.append(num_expr.cast(
                pl.Float64, strict=False).cast(pl.Int64, strict=False).alias(target_col))

        elif dtype in ["pct", "percent", "percentage"]:
            expressions.append(
                (num_expr.cast(pl.Float64, strict=False) / 100).alias(target_col))

        elif isinstance(dtype, dict) and dtype.get("type") == "datetime":
            fmt = dtype.get("format", "%Y-%m-%d")
            expressions.append(pl.col(target_col).str.to_datetime(
                format=fmt, strict=False).alias(target_col))

        elif dtype in [str, "str"]:
            # Standard string scrub
            expressions.append(
                pl.col(target_col).str.strip_chars().str.replace(
                    r"^'", "").alias(target_col)
            )

    if expressions:
        df = df.with_columns(expressions)

    return df


if __name__ == "__main__":
    # example usage
    config = {
        "columns_map": {
            "Shares": "share_count",
            "CUID": "cuid_id"
        },
        "schema": {
            "Shares": int,
            "Date": {"type": "datetime", "format": "%m/%d/%Y"},
        },
    }
    con = duckdb.connect()
    arrow_stream = con.execute(
        "SELECT * FROM delta_scan('C:/Users/mmoin/PYTHON PROJECTS/DataWareHouse/bronze/cds_monthly_participant_reports')").arrow()
    # Polars can initialize a LazyFrame directly from an Arrow stream
    lf = pl.from_arrow(arrow_stream).lazy()
    lf = lf.drop_nulls(subset=["Date", "ISIN", "CUID"])

    # Apply your cleaning and collect at the very end
    result = clean_basic(lf, config).collect()
    print("Transformed DataFrame:")
    print(result)
