# economics_metrics

This repository provides small utilities for downloading publicly available
macroeconomic datasets. It now supports both United States indicators from
FRED (Consumer Price Index and benchmark interest rates) and Euro area
indicators from the European Central Bank (ECB).

## Quick examples

```bash
# Everything (ECB Euribor + FX + HICP aggregates/pivots + US CPI + US rates)
python ecb_suite.py --start 2015-01 --end 2025-09

# Only Euribor, custom output name
python ecb_suite.py --run euribor --start 2010-01 --out-euribor euribor_2010_2025.csv

# Only FX with restricted set of currencies
python ecb_suite.py --run fx --fx-currencies USD,GBP,JPY,CHF --start 2020-01-01

# Only HICP aggregates (ANR)
python ecb_suite.py --run hicp_agg --measure ANR --start 2015-01

# Only HICP by sector (INX), keep your two exports
python ecb_suite.py --run hicp_sector --measure INX --start 2015-01

# Only US CPI (index + YoY inflation)
python ecb_suite.py --run us_inflation --start 2000-01 --out-us-inflation us_cpi.csv

# Only US benchmark interest rates
python ecb_suite.py --run us_rates --start 2000-01 --out-us-rates us_rates.csv

# Run both US downloads without ECB data
python ecb_suite.py --run us --start 2000-01
```

