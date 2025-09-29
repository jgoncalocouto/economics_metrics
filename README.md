# economics_metrics

This repository provides small utilities for downloading publicly available
macroeconomic datasets. The unified ``ecb_suite.py`` script now covers both
United States indicators from FRED and Euro area indicators from the European
Central Bank (ECB).


# Everything (ECB suite + U.S. CPI + U.S. rates)
python ecb_suite.py --start 2015-01 --end 2025-09

# Only Euribor, custom output
python ecb_suite.py --run euribor --start 2010-01 --out-euribor euribor_2010_2025.csv

# Only FX with restricted set of currencies
python ecb_suite.py --run fx --fx-currencies USD,GBP,JPY,CHF --start 2020-01-01

# Only U.S. CPI (requires ``FRED_API_KEY`` env var or ``--fred-api-key``)
python ecb_suite.py --run fred_inflation --start 2015-01

# Only U.S. rates, custom output paths
python ecb_suite.py --run fred_rates --start 2015-01 --out-fred-rates data/us_rates.csv

# Only HICP aggregates (ANR)
python ecb_suite.py --run hicp_agg --measure ANR --start 2015-01

# Only HICP by sector (INX), keep your two exports
python ecb_suite.py --run hicp_sector --measure INX --start 2015-01

