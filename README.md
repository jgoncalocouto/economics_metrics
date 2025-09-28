# economics_metrics

This repository provides small utilities for downloading publicly available
macroeconomic datasets. The initial focus is on United States indicators.

## Downloading US CPI and federal funds rate data

The script at `scripts/download_us_macro.py` downloads the following data series
from the Federal Reserve Economic Data (FRED) service:

- `CPIAUCSL` – Consumer Price Index for All Urban Consumers (CPI)
- `FEDFUNDS` – Effective federal funds rate, a common reference interest rate in
  the US

The command below downloads both series into `data/raw/us/`:

```bash
python scripts/download_us_macro.py
```

To choose a different output directory or download a subset of the available
series:

```bash
python scripts/download_us_macro.py --output-dir path/to/save --series cpi
```

Use `--no-overwrite` to skip downloading files that already exist.

## Interactive dashboard

Launch the Streamlit application to explore the available series with an
interactive Plotly chart:

```bash
streamlit run streamlit_app.py
```

The dashboard lets you select the series to display, adjust the date range, and
download the filtered data as a CSV file directly from the interface.
