from typing import Any, Sequence

import numpy as np
import pandas as pd
from blpapi import Event, Name, Session, SessionOptions


def create_session(host: str = "localhost", port: int = 8194) -> Any:
	options = SessionOptions()
	options.setServerHost(host)
	options.setServerPort(port)
	session = Session(options)
	if not session.start():
		raise RuntimeError("Failed to start Bloomberg session.")
	if not session.openService("//blp/refdata"):
		raise RuntimeError("Failed to open //blp/refdata")
	return session


def close_session(session: Any) -> None:
	if session is None:
		return
	try:
		session.stop()
	except Exception:
		pass


def bdh(
	session: Any,
	tickers: Sequence[str],
	fields: Sequence[str],
	start_date: str,
	end_date: str,
	frequency: str = "DAILY",
) -> pd.DataFrame:
	service = session.getService("//blp/refdata")
	request = service.createRequest("HistoricalDataRequest")
	for ticker in tickers:
		request.getElement("securities").appendValue(ticker)
	for field in fields:
		request.getElement("fields").appendValue(field)
	request.set("startDate", start_date)
	request.set("endDate", end_date)
	request.set("periodicitySelection", frequency)
	session.sendRequest(request)

	rows = []
	while True:
		event = session.nextEvent()
		for msg in event:
			if msg.messageType() != Name("HistoricalDataResponse") or msg.hasElement("responseError"):
				continue
			security_data = msg.getElement("securityData")
			if security_data.hasElement("securityError"):
				continue
			ticker = security_data.getElementAsString("security")
			field_data = security_data.getElement("fieldData")
			for i in range(field_data.numValues()):
				entry = field_data.getValueAsElement(i)
				row = {"Ticker": ticker, "Date": entry.getElementAsDatetime("date")}
				for field in fields:
					row[field] = entry.getElementAsFloat(field) if entry.hasElement(field) else np.nan
				rows.append(row)
		if event.eventType() == Event.RESPONSE:
			break
	return pd.DataFrame(rows)


def bdp(session: Any, tickers: Sequence[str], fields: Sequence[str]) -> pd.DataFrame:
	service = session.getService("//blp/refdata")
	request = service.createRequest("ReferenceDataRequest")
	for ticker in tickers:
		request.getElement("securities").appendValue(ticker)
	for field in fields:
		request.getElement("fields").appendValue(field)
	session.sendRequest(request)

	rows = []
	while True:
		event = session.nextEvent()
		for msg in event:
			if msg.messageType() != Name("ReferenceDataResponse") or msg.hasElement("responseError"):
				continue
			security_data_array = msg.getElement("securityData")
			for i in range(security_data_array.numValues()):
				security_data = security_data_array.getValueAsElement(i)
				if security_data.hasElement("securityError"):
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


__all__ = ["create_session", "close_session", "bdh", "bdp"]
