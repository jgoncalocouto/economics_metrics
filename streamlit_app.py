"""Streamlit app for exploring US macroeconomic time series."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from economics_metrics import DEFAULT_SERIES, DownloadError, fetch_series_dataframe


st.set_page_config(page_title="US Macro Dashboard", layout="wide")


@st.cache_data(show_spinner=False)
def _load_series(name: str) -> pd.DataFrame:
    """Load a single series and return a dataframe indexed by date."""

    series = DEFAULT_SERIES[name]
    frame = fetch_series_dataframe(series)
    return frame.rename(columns={series.series_id: series.description or series.slug})


def _combine_series(selected: list[str]) -> pd.DataFrame:
    combined: pd.DataFrame | None = None
    for name in selected:
        frame = _load_series(name)
        combined = frame if combined is None else combined.join(frame, how="outer")
    if combined is None:
        return pd.DataFrame()
    return combined.sort_index()


def _series_options() -> dict[str, str]:
    return {
        (config.description or config.slug): name for name, config in DEFAULT_SERIES.items()
    }


st.title("US Macroeconomic Dashboard")
st.markdown(
    """
    Explore consumer prices and reference interest rates sourced from
    [FRED](https://fred.stlouisfed.org/). Choose the series and time range to
    visualize. Use the download button below the chart to export the selected
    data as a CSV file.
    """
)

options = _series_options()
labels = list(options.keys())
default_selection = labels
selected_labels = st.multiselect(
    "Select series to display", labels, default=default_selection, help="Choose one or more time series to visualize."
)

selected_series = [options[label] for label in selected_labels]

error_placeholder = st.empty()

if not selected_series:
    st.info("Select at least one series to display the chart.")
    st.stop()

try:
    data = _combine_series(selected_series)
except DownloadError as exc:
    error_placeholder.error(f"Failed to download data: {exc}")
    st.stop()

if data.empty:
    st.warning("No data available for the selected series.")
    st.stop()

min_date = data.index.min().to_pydatetime()
max_date = data.index.max().to_pydatetime()

start_date, end_date = st.slider(
    "Date range",
    min_value=min_date,
    max_value=max_date,
    value=(min_date, max_date),
)

filtered = data.loc[start_date:end_date]

if filtered.empty:
    st.warning("No observations within the selected range.")
    st.stop()

chart_df = filtered.reset_index().rename(columns={"index": "DATE"})
chart = px.line(chart_df, x="DATE", y=chart_df.columns[1:], markers=False)
chart.update_layout(margin=dict(l=0, r=0, t=40, b=0), legend_title="Series")

st.plotly_chart(chart, use_container_width=True)

csv_data = filtered.reset_index().to_csv(index=False).encode("utf-8")
st.download_button(
    "Download filtered data",
    data=csv_data,
    file_name="us_macro_filtered.csv",
    mime="text/csv",
    help="Download the currently displayed data as a CSV file.",
)
