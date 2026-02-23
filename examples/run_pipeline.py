import os
import sys

# add src to path so we can import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline.extractor import FileReader
from pipeline.validator import Validator
from pipeline.cleaning import fill_missing, parse_dates
from pipeline.transformer import Transformer
from utils.db_connector import SQLiteUploader


def main():
    print("=" * 70)
    print("ETL Pipeline Example: Read → Validate → Clean → Transform → Upload")
    print("=" * 70)

    # ========== STEP 1: EXTRACT ==========
    print("\n[STEP 1] EXTRACT: Reading two CSV files...")
    
    report_a_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "report_a.csv")
    report_b_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "report_b.csv")

    reader_a = FileReader(report_a_path)
    df_a = reader_a.read()
    print(f"  ✓ Read {report_a_path}: shape {df_a.shape}")
    print(f"    Columns: {list(df_a.columns)}")
    print(f"    Data:\n{df_a}\n")

    reader_b = FileReader(report_b_path)
    df_b = reader_b.read()
    print(f"  ✓ Read {report_b_path}: shape {df_b.shape}")
    print(f"    Columns: {list(df_b.columns)}")
    print(f"    Data:\n{df_b}\n")

    # ========== STEP 2: VALIDATE ==========
    print("[STEP 2] VALIDATE: Running schema checks...")
    
    validator = Validator()
    schema_a = {
        "id": {"required": True, "type": "numeric"},
        "name": {"required": True, "type": "string"},
        "amount": {"required": False, "type": "numeric"},
        "date": {"required": False, "type": "string"},
    }
    result_a = validator.validate(df_a, schema_a)
    print(f"  Report A validation: valid={result_a['valid']}")
    if result_a["errors"]:
        print(f"    Errors: {result_a['errors']}")

    schema_b = {
        "transaction_id": {"required": True, "type": "string"},
        "customer": {"required": True, "type": "string"},
        "value": {"required": False, "type": "numeric"},
        "status": {"required": False, "type": "string"},
    }
    result_b = validator.validate(df_b, schema_b)
    print(f"  Report B validation: valid={result_b['valid']}")
    if result_b["errors"]:
        print(f"    Errors: {result_b['errors']}\n")

    # ========== STEP 3: CLEAN ==========
    print("[STEP 3] CLEAN: Filling missing values...")
    
    strategy_a = {
        "amount": {"method": "median"},
        "date": {"method": "constant", "value": "2024-01-01"},
    }
    df_a_cleaned = fill_missing(df_a, strategy_a)
    print(f"  Report A after cleaning (missing filled):\n{df_a_cleaned}\n")

    strategy_b = {
        "value": {"method": "median"},
        "status": {"method": "constant", "value": "unknown"},
    }
    df_b_cleaned = fill_missing(df_b, strategy_b)
    print(f"  Report B after cleaning (missing filled):\n{df_b_cleaned}\n")

    # ========== STEP 4: TRANSFORM ==========
    print("[STEP 4] TRANSFORM: Applying business logic...")
    
    transformer = Transformer()
    
    # Example: rename columns and add a computed column for Report A
    rules_a = {
        "rename": {"id": "record_id", "amount": "transaction_amount"},
        "computed": {
            "amount_category": lambda df: df["transaction_amount"].apply(
                lambda x: "high" if x > 1500 else ("medium" if x > 1000 else "low") if x else "unknown"
            )
        },
    }
    df_a_transformed = transformer.transform(df_a_cleaned, rules_a)
    print(f"  Report A after transform:\n{df_a_transformed}\n")

    # Example: rename columns for Report B
    rules_b = {
        "rename": {"transaction_id": "trans_id", "value": "trans_amount"},
    }
    df_b_transformed = transformer.transform(df_b_cleaned, rules_b)
    print(f"  Report B after transform:\n{df_b_transformed}\n")

    # ========== STEP 5: LOAD ==========
    print("[STEP 5] LOAD: Uploading to SQLite database...")
    
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "etl.db")
    uploader = SQLiteUploader(db_path)
    
    uploader.upload_df(df_a_transformed, "report_a_processed", if_exists="replace")
    print(f"  ✓ Uploaded Report A to 'report_a_processed' table")
    
    uploader.upload_df(df_b_transformed, "report_b_processed", if_exists="replace")
    print(f"  ✓ Uploaded Report B to 'report_b_processed' table")

    # Verify by querying
    rows_a = uploader.query("SELECT COUNT(*) FROM report_a_processed")
    rows_b = uploader.query("SELECT COUNT(*) FROM report_b_processed")
    print(f"  ✓ Report A table has {rows_a[0][0]} rows")
    print(f"  ✓ Report B table has {rows_b[0][0]} rows\n")

    # ========== SUMMARY ==========
    print("=" * 70)
    print("SUCCESS: Full pipeline executed (extract → validate → clean → transform → load)")
    print(f"Database saved to: {db_path}")
    print("=" * 70)

    uploader.close()


if __name__ == "__main__":
    main()
