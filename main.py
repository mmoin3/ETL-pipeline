"""
Main entry point: Configure and run the ingestion pipeline.
Instantiates components and orchestrates execution.
"""

from config import RAW_DATA_DIR, DB_CONN_STR
from src.loader import Loader
from src.cleaner import DataFrameCleaner
from src.writer import get_writer
from src.validator import DataValidator
from src.ingestor import Ingestor


def main():
    """Run ingestion pipeline: discover → read → clean → validate → write → move."""
    
    # Instantiate specialized components
    loader = Loader()
    cleaner = DataFrameCleaner()
    validator = DataValidator()
    writer = get_writer(DB_CONN_STR)
    
    # Instantiate orchestrator
    ingestor = Ingestor(loader, cleaner, writer, validator)
    
    try:
        # Execute pipeline
        results = ingestor.run(RAW_DATA_DIR)
        
        # Print summary
        print(f"\nIngestion complete:")
        print(f"  Total files processed: {len(results)}")
        
        success_count = sum(1 for r in results if r["status"] == "success")
        skipped_count = sum(1 for r in results if r["status"] == "skipped")
        error_count = sum(1 for r in results if r["status"] == "error")
        
        print(f"  [OK] Success: {success_count}")
        print(f"  [SKIP] Skipped: {skipped_count}")
        print(f"  [ERR] Errors: {error_count}")
    
    finally:
        writer.close()


if __name__ == "__main__":
    main()
