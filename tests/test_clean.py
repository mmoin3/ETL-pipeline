import unittest
import pandas as pd

from src.cleaner import DataFrameCleaner

class TestDataFrameCleaner(unittest.TestCase):
    """
    Unit tests for DataFrameCleaner. Tests cover cleaning with and without schema,
    handling of NULL-like values, quoted values, and partial schemas. New tests can
    be added to cover additional edge cases or specific data types. To run tests,
    type in root directory: python -m unittest tests/test_clean.py
    """
    def setUp(self):
        self.cleaner = DataFrameCleaner()

    def test_clean_with_schema_casts_datetime_and_numeric(self):
        """
        Test cleaning with a provided schema that includes datetime, float, and int types.
        To test, we create a DataFrame with string representations of dates and numbers,
        then clean it using the schema from our cleaner function. 
        We assert that the resulting columns have the correct types and values.
        If the test passes, it confirms that the cleaner correctly casts columns
        according to the provided schema, handling various formats and characters in the process.
        """
        schema = {"TRADE_DATE": "datetime64[ns]", "NAV": float, "COUNT": int}
        dataframe = pd.DataFrame([
            {"TRADE_DATE": "20260220", "NAV": "1,234.56", "COUNT": "10"},
            {"TRADE_DATE": "2026-02-21", "NAV": "$789.00", "COUNT": "20"},
        ])
        cleaned = self.cleaner.clean(dataframe, schema=schema)

        assert pd.api.types.is_datetime64_any_dtype(cleaned["TRADE_DATE"])
        assert cleaned["NAV"].dtype == float
        assert cleaned["COUNT"].dtype == "Int64"
        assert cleaned["NAV"].tolist() == [1234.56, 789.0]
        assert cleaned["COUNT"].tolist() == [10, 20]

    def test_clean_without_schema_infers_types_and_normalizes_text(self):
        dataframe = pd.DataFrame([
            {"date_col": "20260220", "num_col": "1,000", "name": "  'abc  "},
            {"date_col": "2026-02-21", "num_col": "2000", "name": "def"},
        ])
        cleaned = self.cleaner.clean(dataframe)

        assert pd.api.types.is_datetime64_any_dtype(cleaned["date_col"])
        assert cleaned["num_col"].dtype == "Int64"
        assert cleaned["num_col"].tolist() == [1000, 2000]
        assert cleaned["name"].dtype == "string"
        assert cleaned["name"].tolist() == ["abc", "def"]

    def test_clean_with_null_like_values(self):
        """Test handling of NULL-like values (N/A, NULL, etc)"""
        dataframe = pd.DataFrame([
            {"value": "100", "text": "valid"},
            {"value": "N/A", "text": "NULL"},
            {"value": "null", "text": "N/A"},
        ])
        cleaned = self.cleaner.clean(dataframe)

        assert pd.isna(cleaned["value"].iloc[1])
        assert pd.isna(cleaned["text"].iloc[1])

    def test_clean_with_quoted_values(self):
        """Test handling of quoted values like 'abc or '5"""
        dataframe = pd.DataFrame([
            {"text": "'quoted", "num": "'123"},
            {"text": "normal", "num": "456"},
        ])
        cleaned = self.cleaner.clean(dataframe)

        assert cleaned["text"].tolist() == ["quoted", "normal"]
        assert cleaned["num"].dtype == "Int64"
        assert cleaned["num"].tolist() == [123, 456]

    def test_clean_with_partial_schema_still_infers_unmapped_columns(self):
        schema = {"TRADE_DATE": "datetime64[ns]"}
        dataframe = pd.DataFrame([
            {"TRADE_DATE": "2026-02-20", "UNMAPPED_NUM": "5", "UNMAPPED_TEXT": " x "},
            {"TRADE_DATE": "30/30/2026", "UNMAPPED_NUM": "10", "UNMAPPED_TEXT": "'y"},
        ])
        cleaned = self.cleaner.clean(dataframe, schema=schema)

        assert pd.api.types.is_datetime64_any_dtype(cleaned["TRADE_DATE"])
        assert cleaned["UNMAPPED_NUM"].dtype == "Int64"
        assert cleaned["UNMAPPED_NUM"].tolist() == [5, 10]
        assert cleaned["UNMAPPED_TEXT"].tolist() == ["x", "y"]
        assert pd.notna(cleaned["TRADE_DATE"].iloc[0])
        assert pd.isna(cleaned["TRADE_DATE"].iloc[1])

if __name__ == "__main__":
    unittest.main()
