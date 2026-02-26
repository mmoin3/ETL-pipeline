import pandas as pd
import numpy as np
from blpapi import Session, SessionOptions, Name, Event
from typing import List, Optional, Sequence, Any
import logging

logger = logging.getLogger(__name__)

class BloombergClient:
    """
    Bloomberg API client for data retrieval.
    Example:
        client = BloombergClient()
        df = client.bdh(["AAPL US Equity"], ["PX_LAST"], "20240101", "20240131")
        client.close()
    """
    def __init__(self, host: str = "localhost", port: int = 8194):
        """
        Initialize and start Bloomberg session.
        Args:
            host: Bloomberg server host
            port: Bloomberg server port
        """
        self.session = None
        self.host = host
        self.port = port
        self._connect()
    
    def _connect(self) -> None:
        """Start Bloomberg session."""
        options = SessionOptions()
        options.setServerHost(self.host)
        options.setServerPort(self.port)
        
        self.session = Session(options)
        if not self.session.start():
            raise ConnectionError(f"Failed to connect to Bloomberg at {self.host}:{self.port}")
        
        logger.info(f"Connected to Bloomberg at {self.host}:{self.port}")
    
    def close(self) -> None:
        """Close Bloomberg session."""
        if self.session:
            try:
                self.session.stop()
                logger.info("Bloomberg session closed")
            except Exception as e:
                logger.warning(f"Error closing session: {e}")
            finally:
                self.session = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def BDH(
        self,
        tickers: Sequence[str],
        fields: Sequence[str],
        start_date: str,
        end_date: str,
        frequency: str = "DAILY",
    ) -> pd.DataFrame:
        """
        Bloomberg Historical Data Request.
        
        Args:
            tickers: List of tickers (e.g., ["AAPL US Equity", "MSFT US Equity"])
            fields: List of fields (e.g., ["PX_LAST", "VOLUME"])
            start_date: YYYYMMDD
            end_date: YYYYMMDD
            frequency: DAILY, WEEKLY, MONTHLY, QUARTERLY, YEARLY
        
        Returns:
            DataFrame with columns: Ticker, Date, [fields]
        """
        if not self.session:
            raise RuntimeError("Bloomberg session not connected")
        
        service = self.session.getService("//blp/refdata")
        request = service.createRequest("HistoricalDataRequest")
        
        # Add tickers
        for ticker in tickers:
            request.getElement("securities").appendValue(ticker)
        
        # Add fields
        for field in fields:
            request.getElement("fields").appendValue(field)
        
        # Set parameters
        request.set("startDate", start_date)
        request.set("endDate", end_date)
        request.set("periodicitySelection", frequency)
        
        self.session.sendRequest(request)
        
        # Process response
        rows = []
        while True:
            event = self.session.nextEvent()
            for msg in event:
                if msg.messageType() != Name("HistoricalDataResponse"):
                    continue
                
                if msg.hasElement("responseError"):
                    logger.error(f"Response error: {msg.getElementAsString('responseError')}")
                    continue
                
                security_data = msg.getElement("securityData")
                if security_data.hasElement("securityError"):
                    logger.warning(f"Security error: {security_data.getElementAsString('securityError')}")
                    continue
                
                ticker = security_data.getElementAsString("security")
                field_data = security_data.getElement("fieldData")
                
                for i in range(field_data.numValues()):
                    entry = field_data.getValueAsElement(i)
                    row = {
                        "Ticker": ticker,
                        "Date": entry.getElementAsDatetime("date")
                    }
                    for field in fields:
                        if entry.hasElement(field):
                            row[field] = entry.getElementAsFloat(field)
                        else:
                            row[field] = np.nan
                    rows.append(row)
            
            if event.eventType() == Event.RESPONSE:
                break
        
        return pd.DataFrame(rows)
    
    def BDP(
        self,
        tickers: Sequence[str],
        fields: Sequence[str],
    ) -> pd.DataFrame:
        """
        Bloomberg Reference Data Request.
        
        Args:
            tickers: List of tickers
            fields: List of fields (e.g., ["NAME", "SECTOR", "MARKET_CAP"])
        
        Returns:
            DataFrame with columns: Ticker, [fields]
        """
        if not self.session:
            raise RuntimeError("Bloomberg session not connected")
        
        service = self.session.getService("//blp/refdata")
        request = service.createRequest("ReferenceDataRequest")
        
        # Add tickers
        for ticker in tickers:
            request.getElement("securities").appendValue(ticker)
        
        # Add fields
        for field in fields:
            request.getElement("fields").appendValue(field)
        
        self.session.sendRequest(request)
        
        # Process response
        rows = []
        while True:
            event = self.session.nextEvent()
            for msg in event:
                if msg.messageType() != Name("ReferenceDataResponse"):
                    continue
                
                if msg.hasElement("responseError"):
                    logger.error(f"Response error: {msg.getElementAsString('responseError')}")
                    continue
                
                security_data_array = msg.getElement("securityData")
                for i in range(security_data_array.numValues()):
                    security_data = security_data_array.getValueAsElement(i)
                    
                    if security_data.hasElement("securityError"):
                        logger.warning(f"Security error: {security_data.getElementAsString('securityError')}")
                        continue
                    
                    ticker = security_data.getElementAsString("security")
                    field_data = security_data.getElement("fieldData")
                    
                    row = {"Ticker": ticker}
                    for field in fields:
                        if not field_data.hasElement(field):
                            row[field] = np.nan
                            continue
                        
                        try:
                            row[field] = field_data.getElementAsFloat(field)
                        except Exception:
                            row[field] = str(field_data.getElement(field))
                    
                    rows.append(row)
            
            if event.eventType() == Event.RESPONSE:
                break
        
        return pd.DataFrame(rows)
    

# Convenience functions for simple use cases
def get_historical_prices(
    tickers: List[str],
    start_date: str,
    end_date: str,
    periodicity: str = "DAILY",
    field: list[str] = ["PX_LAST"],
    host: str = "localhost",
    port: int = 8194,
) -> pd.DataFrame:
    """
    Quick helper to get historical prices using BDH.
    
    Example:
        df = get_historical_prices(["AAPL US Equity"], "20240101", "20240131", "DAILY")
    """
    client = BloombergClient(host, port)
    try:
        return client.BDH(tickers, field, start_date, end_date)
    finally:
        client.close()


def get_security_info(
    tickers: List[str],
    fields: List[str] = ["NAME", "SECTOR", "GICS_SECTOR_NAME"],
    host: str = "localhost",
    port: int = 8194,
) -> pd.DataFrame:
    """
    Quick helper to get security reference information using BDP.
    """
    client = BloombergClient(host, port)
    try:
        return client.BDP(tickers, fields)
    finally:
        client.close()

__all__ = ["BloombergClient", "get_historical_prices", "get_security_info"]
