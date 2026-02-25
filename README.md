# Daily Extract Transform Load (ETL) Pipeline

## Overview
This pipeline automates the daily data workflow: extracting data from 10+ source files (`INAV_BSKT_ALL`, etc.), cleaning, transforming, and loading into PostgreSQL for analysis.

## Quick Start

### Prerequisites
* Python 3.9+
* PostgreSQL 15+
* 8GB RAM recommended
* Code editor (VSCode recommended)
* Basic git knowledge

### Installation
In the Command prompt or Terminal type or copy the following instructions
```bash
# 1. Clone the repository
git clone [https://github.com/mmoin3/ETL-pipeline.git](https://github.com/mmoin3/ETL-pipeline.git)
cd ETL-pipeline

# 2. Set up virtual environment
python -m venv venv
source venv/bin/activate  # On MacOS
venv\Scripts\activate     # On Windows

# 3. Install dependencies
# Note: Bloomberg API requires special installation - see Bloomberg Tools section below
pip install --upgrade pip
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your credentials:
# - Database password
# - Email App Password (for 2FA)
# - Any other secrets
```

# 5. Bloomberg Integration Tools
* Note: Bloomberg Anywhere License required; Bloomberg Terminal must be on and running in Background for data pulls
```bash
python -m pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple/ blpapi
```

```python
from services.bloomberg_tools import create_session, close_session, BDH, BDP
session = create_session()
try:
    # Historical Data Request
    hist = BDH(session, ["XIU CN Equity"], ["PX_LAST"], "20260101", "20260220")
    # Snapshot Data Request
    snap = BDP(session, ["XIU CN Equity"], ["PX_LAST", "VOLUME"])
finally:
    close_session(session)

# Alternative simplified call:
snap = get_security_info(["XIU CN Equity"], ["PX_LAST"])
```