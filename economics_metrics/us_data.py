"""Utilities for downloading US macroeconomic data series."""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pandas as pd
import requests

FRED_GRAPH_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


@dataclass(frozen=True)
class SeriesConfig:
    """Configuration for a FRED time series download."""

    slug: str
    series_id: str
    description: str
    filename: str | None = None

    def output_name(self) -> str:
        """Return the filename to use for this series."""
        if self.filename:
            return self.filename
        return f"{self.slug}.csv"


DEFAULT_SERIES: Mapping[str, SeriesConfig] = {
    "cpi": SeriesConfig(
        slug="us_cpi_all_urban",
        series_id="CPIAUCSL",
        description="Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
    ),
    "reference_rate": SeriesConfig(
        slug="us_federal_funds_rate",
        series_id="FEDFUNDS",
        description="Effective Federal Funds Rate",
    ),
}


class DownloadError(RuntimeError):
    """Raised when a dataset cannot be downloaded."""


def _retrieve_series_csv(
    series: SeriesConfig, session: requests.Session | None = None
) -> bytes:
    """Fetch the raw CSV bytes for a FRED series."""

    http = session or requests.Session()
    try:
        response = http.get(
            FRED_GRAPH_URL,
            params={"id": series.series_id},
            headers={
                "Accept": "text/csv",
                "User-Agent": "economics-metrics-bot/1.0 (+https://openai.com)",
            },
            timeout=30,
        )
    except requests.RequestException as exc:
        raise DownloadError(f"Failed to download {series.series_id}: {exc}") from exc

    if response.status_code != 200:
        raise DownloadError(
            f"Unexpected status code {response.status_code} for {series.series_id}"
        )

    return response.content


def download_fred_series(
    series: SeriesConfig,
    output_dir: Path,
    session: requests.Session | None = None,
    *,
    overwrite: bool = True,
) -> Path:
    """Download a single FRED series to ``output_dir``.

    Parameters
    ----------
    series:
        The :class:`SeriesConfig` that identifies the series to fetch.
    output_dir:
        Directory where the CSV should be saved. The directory will be created
        if it does not already exist.
    session:
        Optional :class:`requests.Session` to use for the HTTP request.
    overwrite:
        When ``True`` (default) the file will be overwritten if it already
        exists. When ``False`` existing files are left untouched and the path is
        simply returned.

    Returns
    -------
    pathlib.Path
        The path of the downloaded file.
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    target_path = output_dir / series.output_name()
    if not overwrite and target_path.exists():
        return target_path

    content = _retrieve_series_csv(series, session=session)

    try:
        target_path.write_bytes(content)
    except OSError as exc:
        raise DownloadError(f"Unable to save {series.series_id}: {exc}") from exc

    return target_path


def fetch_series_dataframe(
    series: SeriesConfig, session: requests.Session | None = None
) -> pd.DataFrame:
    """Return a FRED series as a :class:`pandas.DataFrame`.

    The resulting dataframe uses the ``DATE`` column as a ``DatetimeIndex`` and
    preserves the original series identifier column name.
    """

    raw_csv = _retrieve_series_csv(series, session=session)

    try:
        frame = pd.read_csv(
            BytesIO(raw_csv),
            na_values={series.series_id: ["."]},
            encoding="utf-8-sig",
        )
    except (ValueError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        raise DownloadError(f"Unable to parse data for {series.series_id}: {exc}") from exc

    date_column: str | None = None
    for column in frame.columns:
        if column.strip().upper() == "DATE":
            date_column = column
            break

    if not date_column:
        raise DownloadError(f"Malformed data received for {series.series_id}: missing DATE column")

    if date_column != "DATE":
        frame = frame.rename(columns={date_column: "DATE"})

    frame["DATE"] = pd.to_datetime(frame["DATE"], errors="coerce")
    if frame["DATE"].isna().all():
        raise DownloadError(f"Malformed data received for {series.series_id}: invalid DATE values")

    series_column: str | None = None
    for column in frame.columns:
        if column == series.series_id:
            series_column = column
            break
        if column.strip().upper() == series.series_id.upper():
            series_column = column
            break

    if series_column and series_column != series.series_id:
        frame = frame.rename(columns={series_column: series.series_id})

    if series.series_id not in frame:
        raise DownloadError(
            f"Malformed data received for {series.series_id}: missing series column"
        )

    frame[series.series_id] = pd.to_numeric(frame[series.series_id], errors="coerce")

    frame = frame.dropna(subset=["DATE"]).set_index("DATE").sort_index()
    return frame


def download_series_collection(
    series_names: Sequence[str] | None,
    output_dir: Path,
    *,
    available: Mapping[str, SeriesConfig] | None = None,
    session: requests.Session | None = None,
    overwrite: bool = True,
) -> list[Path]:
    """Download multiple series by name.

    Parameters
    ----------
    series_names:
        Iterable of keys from ``available`` to download. When ``None`` or empty
        all available series will be downloaded.
    output_dir:
        Destination directory for CSV files.
    available:
        Mapping of series names to :class:`SeriesConfig` definitions.
    session:
        Optional :class:`requests.Session` for HTTP requests.
    overwrite:
        Forwarded to :func:`download_fred_series`.
    """

    if available is None:
        available = DEFAULT_SERIES

    names: Iterable[str]
    if series_names:
        names = series_names
    else:
        names = available.keys()

    downloaded: list[Path] = []
    for name in names:
        if name not in available:
            raise KeyError(f"Unknown series '{name}'. Valid options: {sorted(available)}")
        downloaded.append(
            download_fred_series(available[name], output_dir, session=session, overwrite=overwrite)
        )
    return downloaded
