import pandas as pd
from .base_parser import BaseParser

class INAVBskt(BaseParser):
    """Readable, minimal parser for BSKT-style files (single fund blocks).

    Each block starts with a line beginning with 'TRADE_DATE'. Metadata
    occupies the first few lines of the block (pairs of key,value fields).
    The holdings table begins on the line that contains the header (usually
    includes 'CUSIP' or 'TICKER') and continues until the next TRADE_DATE
    or end of file.
    """
    def extract(self):
        """
        Return a list of fund dicts: {'metadata': {...}, 'holdings': DataFrame}.
        Accepts a list of lines (from CSV, Excel, etc.).
        If lines is None, reads from self.path as text lines (default CSV/txt).
        """
        try:
            table0 = pd.read_table(self.path, header=None)
            fund_blocks =  self._get_blocks(table0, start_marker="TRADE_DATE")

            funds_data = []
            for block in fund_blocks:
                holdings_df_header_idx = 8  # Default header row index for holdings table
                metadata_df = self._extract_metadata(block.iloc[:holdings_df_header_idx])

                holdings_df = pd.DataFrame(
                    block.values[holdings_df_header_idx + 1:],
                    columns=block.values[holdings_df_header_idx]).convert_dtypes()
                
                funds_data.append({"fund_metadata": metadata_df, "fund_holdings": holdings_df})

            return funds_data
        except Exception as e:
            self.logger.error(f"Failed to extract data haha: {e}")
    
    def _get_blocks(self, table:pd.DataFrame, start_marker:str="TRADE_DATE") -> list[pd.DataFrame]:
        """Split DataFrame into blocks based on rows that start with a marker."""
        try:
            starts = [i for i,v in enumerate(table.iloc[:,0]) if v.startswith(start_marker)]
            blocks = []
            for idx, start in enumerate(starts):
                if (idx + 1) < len(starts):
                    end = starts[idx + 1]
                else:
                    end = len(table)
                blocks.append(table[start:end].reset_index(drop=True))
            return blocks
        except Exception as e:
            self.logger.error(f"Error splitting table into blocks: {e}")
            return []
        
    def _extract_metadata(self, data_chunk:list[list[str]]) -> pd.DataFrame:
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
    