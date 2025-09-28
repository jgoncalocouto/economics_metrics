"""Tests for the ECB download helpers."""

from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from economics_metrics.ecb_data import (
    DownloadError,
    SeriesConfig,
    fetch_series_dataframe,
)


class FetchSeriesDataFrameTests(TestCase):
    """Validate parsing behaviour for ECB CSV responses."""

    def setUp(self) -> None:
        self.config = SeriesConfig(
            slug="ea_euribor_3m",
            series_key="FM.M.U2.EUR.RT.MM.EUR.IBOR.3M",
            description="Euro Interbank Offered Rate (3 months)",
        )

    def _fetch_with_csv(self, csv_text: str) -> pd.DataFrame:
        with patch(
            "economics_metrics.ecb_data._retrieve_series_csv",
            return_value=csv_text.encode("utf-8"),
        ):
            return fetch_series_dataframe(self.config)

    def test_strips_metadata_and_parses_time_period(self) -> None:
        csv_text = """Data Source in SDW: FM.M.U2.EUR.RT.MM.EUR.IBOR.3M
Frequency: Monthly
TIME_PERIOD,OBS_VALUE,OBS_STATUS
2024-01,-0.4,
2024-02,-0.41,
"""
        frame = self._fetch_with_csv(csv_text)
        self.assertListEqual(list(frame.columns), [self.config.value_column])
        self.assertEqual(frame.index.name, "TIME_PERIOD")
        self.assertEqual(frame.index[0], pd.Timestamp("2024-01-01"))
        self.assertAlmostEqual(frame.iloc[0, 0], -0.4)

    def test_raises_when_time_period_missing(self) -> None:
        with self.assertRaises(DownloadError):
            self._fetch_with_csv("OBS_VALUE\n2024-01,2\n")

    def test_fallback_used_on_network_failure(self) -> None:
        def _raise_download_error(*args, **kwargs):
            from requests import RequestException

            exc = RequestException("network down")
            raise DownloadError("failed") from exc

        with patch(
            "economics_metrics.ecb_data._retrieve_series_csv",
            side_effect=_raise_download_error,
        ):
            frame = fetch_series_dataframe(self.config)

        self.assertEqual(frame.attrs.get("source"), "fallback")
        self.assertFalse(frame.empty)
