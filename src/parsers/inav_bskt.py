import pandas as pd
from base_parser import BaseParser

class INAVBskt(BaseParser):
    """Readable, minimal parser for BSKT-style files (single fund blocks).

    Each block starts with a line beginning with 'TRADE_DATE'. Metadata
    occupies the first few lines of the block (pairs of key,value fields).
    The holdings table begins on the line that contains the header (usually
    includes 'CUSIP' or 'TICKER') and continues until the next TRADE_DATE
    or end of file.
    """
    def extract(self) -> list[dict[str, pd.DataFrame]]:
        """
        Return a list of fund dicts: {'metadata': {...}, 'holdings': DataFrame}.
        Accepts a list of lines (from CSV, Excel, etc.).
        If lines is None, reads from self.path as text lines (default CSV/txt).
        """
        try:
            df0 = self.read_into_dataframe(header=None, index_col=False)
            fund_blocks = self._get_blocks(df0, start_marker="TRADE_DATE")

            funds_data:list[dict[str,pd.DataFrame]] = []
            for block in fund_blocks:
                holdings_df_header_idx = 8  # Default header row index for holdings table
                metadata_df = self._extract_metadata(block.iloc[:holdings_df_header_idx])

                holdings_df = pd.DataFrame(
                    block.values[holdings_df_header_idx + 1:],
                    columns=block.values[holdings_df_header_idx]).convert_dtypes()
                
                funds_data.append({"fund_metadata": metadata_df, "fund_holdings": holdings_df})

            return funds_data
        except Exception as e:
            self.logger.error(f"Failed to extract data: {e}")
    
    def _get_blocks(self, data: pd.DataFrame, start_marker: str) -> list[pd.DataFrame]:
        """Split DataFrame into blocks where col0 equals start_marker."""
        try:
            marker = start_marker.strip().lstrip("'").upper()
            col0 = data.iloc[:, 0].astype(str).str.strip().str.lstrip("'").str.upper()

            starts = data.index[col0 == marker].tolist()
            if not starts:
                return []

            blocks = []
            for i, s in enumerate(starts):
                e = starts[i + 1] if i + 1 < len(starts) else len(data)
                blocks.append(data.iloc[s:e].reset_index(drop=True))
            return blocks

        except Exception as e:
            self.logger.error(f"Failed to get blocks: {e}")
            return []

    def _extract_metadata(self, data_chunk: pd.DataFrame) -> pd.DataFrame:
        """Extract metadata from fixed-format top lines (no cleaning)."""
        metadata = {}
        first_fields = data_chunk.iloc[0].tolist()
        metadata[first_fields[0]] = first_fields[1]
        metadata["SS_LONG_CODE"] = first_fields[2]
        metadata["FULL_NAME"] = first_fields[4]
        metadata["TICKER"] = first_fields[5]
        metadata["BASE_CURRENCY"] = first_fields[7]

        # Remaining rows are key/value pairs across columns: [k1,v1,k2,v2,...]
        for row in data_chunk.iloc[1:].values:
            for offset in range(0, len(row) - 1, 2):
                key = row[offset]
                value = row[offset + 1]
                if key != "":
                    metadata[key] = value

        return pd.DataFrame([metadata])
    