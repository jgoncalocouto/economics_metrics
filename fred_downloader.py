"""Minimal script to pull key U.S. series from the FRED API.

This mirrors the output style of ``ecb_suite.py``: run the script and it will
download the requested datasets, print a quick preview, and save CSV files
under ``./data``.

The script intentionally keeps everything in the global scope so it can be run
interactively (e.g. from Spyder) and the resulting ``DataFrame`` objects remain
available in the namespace for further inspection.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

import pandas as pd
from fredapi import Fred

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Set ``FRED_API_KEY`` in your environment before running the script.
FRED_API_KEY = os.getenv("FRED_API_KEY")
if not FRED_API_KEY:
    raise EnvironmentError("FRED_API_KEY environment variable is required")

# Date range for the downloads. Adjust to taste.
OBSERVATION_START = "1990-01-01"
OBSERVATION_END = None  # e.g. "2024-12-31"

# FRED series identifiers. Feel free to add more.
INFLATION_SERIES_ID = "CPIAUCSL"  # CPI for All Urban Consumers: All Items (Index 1982-84=100)
INTEREST_RATE_SERIES: Dict[str, str] = {
    "fed_funds": "FEDFUNDS",    # Effective Federal Funds Rate (monthly, %)
    "t_bill_3m": "DTB3",        # 3-Month Treasury Bill: Secondary Market Rate (daily, %)
    "t_bill_6m": "DTB6",        # 6-Month Treasury Bill: Secondary Market Rate (daily, %)
}

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ``fredapi`` handles the HTTP layer and returns pandas ``Series`` out of the box.
fred = Fred(api_key=FRED_API_KEY)


# ---------------------------------------------------------------------------
# Inflation: CPI level + YoY percentage change
# ---------------------------------------------------------------------------
print(f"Fetching inflation series ({INFLATION_SERIES_ID})")
_cpi = fred.get_series(
    INFLATION_SERIES_ID,
    observation_start=OBSERVATION_START,
    observation_end=OBSERVATION_END,
)

df_inflation = pd.DataFrame({
    "cpi_index": _cpi,
})
df_inflation["cpi_yoy_pct"] = df_inflation["cpi_index"].pct_change(12) * 100
df_inflation = df_inflation.dropna(how="all")

print(df_inflation.tail())
inflation_out = DATA_DIR / "us_inflation.csv"
df_inflation.to_csv(inflation_out, float_format="%.6f", date_format="%Y-%m-%d")
print(f"Saved -> {inflation_out}")


# ---------------------------------------------------------------------------
# Interest rates: basic U.S. money-market proxies
# ---------------------------------------------------------------------------
interest_cols = []
for name, series_id in INTEREST_RATE_SERIES.items():
    print(f"Fetching interest rate series {name} ({series_id})")
    s = fred.get_series(
        series_id,
        observation_start=OBSERVATION_START,
        observation_end=OBSERVATION_END,
    ).rename(name)
    interest_cols.append(s)

if interest_cols:
    df_interest_rates = pd.concat(interest_cols, axis=1).sort_index()
else:
    df_interest_rates = pd.DataFrame()

print(df_interest_rates.tail())
rates_out = DATA_DIR / "us_interest_rates.csv"
df_interest_rates.to_csv(rates_out, float_format="%.6f", date_format="%Y-%m-%d")
print(f"Saved -> {rates_out}")

