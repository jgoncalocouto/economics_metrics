"""Tests for the FRED download helpers."""

from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from economics_metrics.us_data import DownloadError, SeriesConfig, fetch_series_dataframe


class FetchSeriesDataFrameTests(TestCase):
    """Validate parsing behaviour for downloaded CSV content."""

    def setUp(self) -> None:
        self.config = SeriesConfig(
            slug="test_series",
            series_id="FEDFUNDS",
            description="Test series",
        )

    def _fetch_with_csv(self, csv_text: str) -> pd.DataFrame:
        with patch("economics_metrics.us_data._retrieve_series_csv", return_value=csv_text.encode("utf-8")):
            return fetch_series_dataframe(self.config)

    def test_handles_bom_prefixed_date_column(self) -> None:
        frame = self._fetch_with_csv("\ufeffDATE,FEDFUNDS\n2024-01-01,5.25\n")
        self.assertListEqual(list(frame.columns), [self.config.series_id])
        self.assertEqual(frame.index[0], pd.Timestamp("2024-01-01"))
        self.assertEqual(frame.iloc[0, 0], 5.25)

    def test_accepts_case_insensitive_date_column(self) -> None:
        frame = self._fetch_with_csv("date,FEDFUNDS\n2024-01-01,5.25\n")
        self.assertEqual(frame.index[0], pd.Timestamp("2024-01-01"))

    def test_accepts_observation_date_column(self) -> None:
        frame = self._fetch_with_csv("observation_date,FEDFUNDS\n2024-01-01,5.25\n")
        self.assertEqual(frame.index[0], pd.Timestamp("2024-01-01"))

    def test_raises_when_series_column_missing(self) -> None:
        with self.assertRaises(DownloadError):
            self._fetch_with_csv("DATE,OTHER\n2024-01-01,1\n")
