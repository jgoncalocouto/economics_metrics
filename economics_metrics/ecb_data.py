"""Utilities for downloading Euro area macroeconomic data from the ECB."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd
import requests

ECB_QUICKVIEW_URL = "https://sdw.ecb.europa.eu/quickviewexport.do"


@dataclass(frozen=True)
class SeriesConfig:
    """Configuration for an ECB time series download."""

    slug: str
    series_key: str
    description: str
    filename: str | None = None
    value_column: str = "OBS_VALUE"

    def output_name(self) -> str:
        """Return the filename to use for this series."""

        if self.filename:
            return self.filename
        return f"{self.slug}.csv"


DEFAULT_SERIES: Mapping[str, SeriesConfig] = {
    "cpi": SeriesConfig(
        slug="ea_hicp_all_items",
        series_key="ICP.M.U2.N.000000.4.ANR",
        description="Euro area HICP (annual rate of change)",
    ),
    "reference_rate": SeriesConfig(
        slug="ea_euribor_3m",
        series_key="FM.M.U2.EUR.RT.MM.EUR.IBOR.3M",
        description="Euro Interbank Offered Rate (3 months)",
    ),
}


class DownloadError(RuntimeError):
    """Raised when a dataset cannot be downloaded."""


def _retrieve_series_csv(
    series: SeriesConfig, session: requests.Session | None = None
) -> bytes:
    """Fetch the raw CSV bytes for an ECB series."""

    http = session or requests.Session()
    params = {
        "SERIES_KEY": series.series_key,
        "type": "csv",
    }

    try:
        response = http.get(
            ECB_QUICKVIEW_URL,
            params=params,
            headers={
                "Accept": "text/csv",
                "User-Agent": "economics-metrics-bot/1.0 (+https://openai.com)",
            },
            timeout=30,
        )
    except requests.RequestException as exc:
        raise DownloadError(f"Failed to download {series.series_key}: {exc}") from exc

    if response.status_code != 200:
        raise DownloadError(
            f"Unexpected status code {response.status_code} for {series.series_key}"
        )

    return response.content


def _strip_metadata_lines(raw_csv: bytes) -> bytes:
    """Remove descriptive header lines before the tabular data."""

    text = raw_csv.decode("utf-8-sig")
    lines = text.splitlines()

    start_index: int | None = None
    for idx, line in enumerate(lines):
        # ECB quickview exports contain a line with the TIME_PERIOD header before the
        # actual observations. Once we find that line we can use pandas normally.
        if "TIME_PERIOD" in line.upper().split(","):
            start_index = idx
            break

    if start_index is None:
        raise DownloadError("Malformed data received: missing TIME_PERIOD header")

    data = "\n".join(lines[start_index:]).strip()
    if not data:
        raise DownloadError("Malformed data received: no observations returned")

    return data.encode("utf-8")


def fetch_series_dataframe(
    series: SeriesConfig, session: requests.Session | None = None
) -> pd.DataFrame:
    """Return an ECB series as a :class:`pandas.DataFrame`."""

    raw_csv = _retrieve_series_csv(series, session=session)
    cleaned_csv = _strip_metadata_lines(raw_csv)

    try:
        frame = pd.read_csv(
            BytesIO(cleaned_csv),
            dtype=str,
        )
    except (ValueError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        raise DownloadError(f"Unable to parse data for {series.series_key}: {exc}") from exc

    time_column: str | None = None
    for column in frame.columns:
        if column.strip().upper() == "TIME_PERIOD":
            time_column = column
            break

    if not time_column:
        raise DownloadError(
            f"Malformed data received for {series.series_key}: missing TIME_PERIOD column"
        )

    if time_column != "TIME_PERIOD":
        frame = frame.rename(columns={time_column: "TIME_PERIOD"})

    value_column: str | None = None
    for column in frame.columns:
        if column.strip().upper() == series.value_column.upper():
            value_column = column
            break

    if not value_column:
        raise DownloadError(
            f"Malformed data received for {series.series_key}: missing {series.value_column} column"
        )

    if value_column != series.value_column:
        frame = frame.rename(columns={value_column: series.value_column})

    frame["TIME_PERIOD"] = pd.to_datetime(frame["TIME_PERIOD"], errors="coerce")
    if frame["TIME_PERIOD"].isna().all():
        raise DownloadError(
            f"Malformed data received for {series.series_key}: invalid TIME_PERIOD values"
        )

    frame[series.value_column] = pd.to_numeric(
        frame[series.value_column], errors="coerce"
    )

    frame = frame.dropna(subset=["TIME_PERIOD"]).set_index("TIME_PERIOD").sort_index()
    return frame[[series.value_column]]


def download_series_collection(
    series_names: Sequence[str] | None,
    output_dir: Path,
    *,
    available: Mapping[str, SeriesConfig] | None = None,
    session: requests.Session | None = None,
    overwrite: bool = True,
) -> list[Path]:
    """Download multiple ECB series by name."""

    if available is None:
        available = DEFAULT_SERIES

    if series_names:
        names: Sequence[str] = series_names
    else:
        names = list(available.keys())

    output_dir.mkdir(parents=True, exist_ok=True)

    downloaded: list[Path] = []
    for name in names:
        if name not in available:
            raise KeyError(f"Unknown series '{name}'. Valid options: {sorted(available)}")

        series = available[name]
        target_path = output_dir / series.output_name()
        if not overwrite and target_path.exists():
            downloaded.append(target_path)
            continue

        content = _retrieve_series_csv(series, session=session)
        cleaned = _strip_metadata_lines(content)

        try:
            target_path.write_bytes(cleaned)
        except OSError as exc:
            raise DownloadError(f"Unable to save {series.series_key}: {exc}") from exc

        downloaded.append(target_path)

    return downloaded

