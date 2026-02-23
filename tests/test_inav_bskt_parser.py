import os
import sys
import tempfile
import unittest

import pandas as pd


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_ROOT = os.path.join(PROJECT_ROOT, "src")
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

from config import NAV_METADATA_TYPE_MAP
from parsers.inav_bskt import BSKTFile
from utils.parsing_helpers import (
    clean_holdings_df,
    convert_metadata_types,
    find_header_index,
    parse_holdings_csv,
    parse_metadata_lines,
    split_blocks,
)


class TestParsingHelpers(unittest.TestCase):
    def setUp(self):
        self.sample_lines = [
            "TRADE_DATE,2026-02-18,LCODE123,,My Fund,TST,,USD\n",
            "CREATION_UNIT_SIZE,50000,DOMICILE,US\n",
            "CUSIP,TICKER,DESCRIPTION,SHARES\n",
            "123456789,abc,first asset,10\n",
            "TRADE_DATE,2026-02-19,LCODE456,,Other Fund,XYZ,,USD\n",
            "CREATION_UNIT_SIZE,100000,DOMICILE,UK\n",
            "CUSIP,TICKER,DESCRIPTION,SHARES\n",
            "987654321,def,second asset,20\n",
        ]

    def test_split_blocks(self):
        blocks = split_blocks(self.sample_lines, start_token="TRADE_DATE")
        self.assertEqual(len(blocks), 2)
        self.assertTrue(blocks[0][0].startswith("TRADE_DATE"))
        self.assertTrue(blocks[1][0].startswith("TRADE_DATE"))

    def test_find_header_index(self):
        block = self.sample_lines[:4]
        header_index = find_header_index(block)
        self.assertEqual(header_index, 2)

    def test_parse_metadata_lines(self):
        metadata = parse_metadata_lines(self.sample_lines[:2])
        self.assertEqual(metadata["TRADE_DATE"], "2026-02-18")
        self.assertEqual(metadata["SS_LONG_CODE"], "LCODE123")
        self.assertEqual(metadata["FULL_NAME"], "My Fund")
        self.assertEqual(metadata["TICKER"], "TST")
        self.assertEqual(metadata["BASE_CURRENCY"], "USD")
        self.assertEqual(metadata["CREATION_UNIT_SIZE"], "50000")
        self.assertEqual(metadata["DOMICILE"], "US")

    def test_convert_metadata_types(self):
        parsed = parse_metadata_lines(self.sample_lines[:2])
        converted = convert_metadata_types(parsed, NAV_METADATA_TYPE_MAP)
        self.assertIsInstance(converted["TRADE_DATE"], pd.Timestamp)
        self.assertIsInstance(converted["CREATION_UNIT_SIZE"], float)
        self.assertEqual(converted["FULL_NAME"], "MY FUND")
        self.assertEqual(converted["TICKER"], "TST")

    def test_parse_and_clean_holdings(self):
        holdings_lines = self.sample_lines[2:4]
        df = parse_holdings_csv(holdings_lines)
        self.assertEqual(df.shape, (1, 4))

        cleaned = clean_holdings_df(df)
        self.assertEqual(cleaned.loc[0, "TICKER"], "ABC")
        self.assertEqual(cleaned.loc[0, "DESCRIPTION"], "FIRST ASSET")


class TestBSKTFileIntegration(unittest.TestCase):
    def test_extract_parses_block_end_to_end(self):
        content = """TRADE_DATE,2026-02-20,LCODE789,,Integration Fund,IVT,,USD
CREATION_UNIT_SIZE,75000,DOMICILE,US
CUSIP,TICKER,DESCRIPTION,SHARES
111111111,ivt,integration asset,25
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as handle:
            handle.write(content)
            temp_path = handle.name

        try:
            parser = BSKTFile(temp_path)
            result = parser.extract()

            self.assertEqual(len(result), 1)
            metadata = result[0]["metadata"]
            holdings = result[0]["holdings"]

            self.assertEqual(metadata["TICKER"], "IVT")
            self.assertEqual(metadata["FULL_NAME"], "INTEGRATION FUND")
            self.assertIsInstance(metadata["TRADE_DATE"], pd.Timestamp)
            self.assertEqual(holdings.shape, (1, 4))
            self.assertEqual(holdings.loc[0, "DESCRIPTION"], "INTEGRATION ASSET")
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


if __name__ == "__main__":
    unittest.main()
