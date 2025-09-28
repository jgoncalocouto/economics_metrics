"""economics_metrics package."""

from . import ecb_data, us_data
from .ecb_data import (
    DEFAULT_SERIES as ECB_DEFAULT_SERIES,
    DownloadError as ECBDownloadError,
    SeriesConfig as ECBSeriesConfig,
    download_series_collection as download_ecb_series,
    fetch_series_dataframe as fetch_ecb_series,
)
from .us_data import (
    DEFAULT_SERIES as US_DEFAULT_SERIES,
    DownloadError as USDownloadError,
    SeriesConfig as USSeriesConfig,
    download_fred_series,
    download_series_collection as download_us_series,
    fetch_series_dataframe as fetch_us_series,
)

# Backwards compatibility with earlier versions that exposed the US helpers
# directly at the package root.
DEFAULT_SERIES = US_DEFAULT_SERIES
DownloadError = USDownloadError
SeriesConfig = USSeriesConfig
download_series_collection = download_us_series
fetch_series_dataframe = fetch_us_series

__all__ = [
    "DEFAULT_SERIES",
    "DownloadError",
    "SeriesConfig",
    "download_fred_series",
    "download_series_collection",
    "fetch_series_dataframe",
    "download_ecb_series",
    "fetch_ecb_series",
    "ECB_DEFAULT_SERIES",
    "ECBDownloadError",
    "ECBSeriesConfig",
    "US_DEFAULT_SERIES",
    "USDownloadError",
    "USSeriesConfig",
    "us_data",
    "ecb_data",
]
