from base_parser import BaseParser
import pandas as pd
import csv

class INAVBskt(BaseParser):
    """Readable, minimal parser for BSKT-style files (multiple fund blocks).

    Each block starts with a line beginning with 'TRADE_DATE'. Metadata
    occupies the first few lines of the block (pairs of key,value fields).
    The holdings table begins on the line that contains the header (usually
    includes 'CUSIP' or 'TICKER') and continues until the next TRADE_DATE
    or end of file.
    """
    def extract(self) -> list[dict[str, pd.DataFrame]]:
        """
        Return a list of fund dicts: {'metadata': {...}, 'holdings': DataFrame}..
        """
        parsed_file = self.read_into_list() #Read file as lines
        fund_blocks = self.get_blocks(parsed_file, start_marker="TRADE_DATE") #Split into blocks based on TRADE_DATE

        all_funds_nav_data = []
        for block in fund_blocks:

            header_idx = self._get_header_index(block)
            metadata = self._parse_metadata(block[:header_idx])
             #Assumes metadata occupies first 8 lines, holdings start at line 9 with headerr
            holdings_data = self.list_to_dataframe(block[header_idx:], header=True, index_col=False)

            all_funds_nav_data.append({"metadata": metadata, "holdings": holdings_data})
        
        return all_funds_nav_data
    
    def _parse_metadata(self, metadata_lines: list[str]) -> dict:
        """Parse metadata lines into a dictionary. Handles key-value pairs and special first line."""
        metadata = {}
        if not metadata_lines:
              self.logger.warning("No metadata lines found in block")
              return metadata
        # Special handling for the first line
        first_fields = next(csv.reader([metadata_lines[0]]))
        metadata[first_fields[0]] = first_fields[1]
        metadata["SS_LONG_CODE"] = first_fields[2]
        metadata["FULL_NAME"]= first_fields[4]
        metadata["TICKER"] = first_fields[5]
        metadata["BASE_CURRENCY"] = first_fields[7]
        # Parse remaining lines as key-value pairs using csv.reader
        for line in metadata_lines[1:]:
            fields = next(csv.reader([line]))
            for i in range(0, len(fields) - 1, 2):
                key = fields[i]
                val = fields[i + 1]
                metadata[key] = val
        
        return pd.DataFrame([metadata]) #Convert to DataFrame for consistency with other parsers
    
    def _get_header_index(self, block_lines: list[str]) -> int | None:
        """Find the index of the holdings header line in the block."""
        for idx, ln in enumerate(block_lines):
            l = ln.strip().upper()
            if "CUSIP" in l or "TICKER" in l or "DESCRIPTION" in l:
                return idx
        self.logger.warning("No holdings header found in block")
        return None  # Not found

__all__ = ["INAVBskt"]