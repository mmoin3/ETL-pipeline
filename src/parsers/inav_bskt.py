import pandas as pd
import logging
from config import LOG_FILE, LOG_LEVEL, NAV_METADATA_TYPE_MAP
from core.logger_config import setup_logger
from utils.file_reader import FileReader
from utils.parsing_helpers import (
    clean_holdings_df,
    convert_metadata_types,
    find_header_index,
    parse_holdings_csv,
    parse_metadata_lines,
    split_blocks,
)

logger = logging.getLogger(__name__)


class BSKTFile(FileReader):
    """Readable, minimal parser for BSKT-style files (multiple fund blocks).

    Each block starts with a line beginning with 'TRADE_DATE'. Metadata
    occupies the first few lines of the block (pairs of key,value fields).
    The holdings table begins on the line that contains the header (usually
    includes 'CUSIP' or 'TICKER') and continues until the next TRADE_DATE
    or end of file.
    """
    def __init__(self, path: str, logger_instance=None):
        super().__init__(path)
        self.logger = logger_instance or setup_logger(
            name=__name__,
            log_file=LOG_FILE,
            level=getattr(logging, str(LOG_LEVEL).upper(), logging.INFO),
        )

    def extract(self, lines=None):
        """
        Return a list of fund dicts: {'metadata': {...}, 'holdings': DataFrame}.
        Accepts a list of lines (from CSV, Excel, etc.).
        If lines is None, reads from self.path as text lines (default CSV/txt).
        """
        if lines is None:
            with open(self.path, "r", encoding="utf-8") as handle:
                lines = handle.readlines()

        blocks = split_blocks(lines, start_token="TRADE_DATE")
        funds = []

        for block_lines in blocks:
            header_idx = find_header_index(block_lines)
            if header_idx is None:
                continue

            metadata_lines = block_lines[:header_idx]
            holdings_lines = block_lines[header_idx:]

            metadata = parse_metadata_lines(metadata_lines)
            metadata = self._clean_and_convert_metadata(metadata)

            try:
                holdings_df = parse_holdings_csv(holdings_lines)
                holdings_df = self._clean_holdings_df(holdings_df)
            except Exception as e:
                self.logger.error(f"Failed to parse holdings block: {e}")
                holdings_df = pd.DataFrame()

            funds.append({"metadata": metadata, "holdings": holdings_df})

        return funds

    def _clean_and_convert_metadata(self, meta):
        return convert_metadata_types(meta, NAV_METADATA_TYPE_MAP)

    def _clean_holdings_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return clean_holdings_df(df)

__all__ = ["FileReader", "BSKTFile"]