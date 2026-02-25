# ETL Pipeline

## Quick Start

### Windows (PowerShell / CMD)
- Initial setup: `setup.bat`
- Run parser tests: `setup.bat test`

### Unix / macOS (bash)
- Initial setup: `./setup.sh`
- Run parser tests: `./setup.sh test`

## Direct Test Command
If you already have your environment ready, you can run:

`python -m unittest discover -s tests -v`

## Bloomberg Tools (Simple)

```python
from services.bloomberg_tools import create_session, close_session, bdh, bdp

session = create_session()
try:
	hist = BDH(session, ["XIU CN Equity"], ["PX_LAST"], "20260101", "20260220")
finally:
	close_session(session)

#Simpler function call:
hist = get_historical_prices(["AAPL US Equity"], "20240101", "20240131", "DAILY")
```
