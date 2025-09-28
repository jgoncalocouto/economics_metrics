# economics_metrics

This repository provides small utilities for downloading publicly available
macroeconomic datasets. It now supports both United States indicators from
FRED and Euro area indicators from the European Central Bank (ECB).

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

Use the sidebar to switch between US (FRED) and Euro area (ECB) data sources.
When the ECB option is selected, the dashboard displays the Harmonised Index of
Consumer Prices (HICP) annual rate and the 3-month Euribor reference rate.

If the app cannot reach the ECB due to a networking problem, it automatically
falls back to bundled sample data so you can continue exploring the interface.
The warning banner at the top of the page lists any series that are being
displayed from the offline samples instead of live data.
