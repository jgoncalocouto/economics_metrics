"""Streamlit app for exploring macroeconomic time series."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from economics_metrics import ecb_data, us_data


DATASETS: dict[str, dict[str, Any]] = {
    "Euro area (ECB)": {
        "series": ecb_data.DEFAULT_SERIES,
        "fetch": ecb_data.fetch_series_dataframe,
        "column_name": lambda config: config.value_column,
        "error": ecb_data.DownloadError,
        "download_stub": "euro_area",
    },
    "United States (FRED)": {
        "series": us_data.DEFAULT_SERIES,
        "fetch": us_data.fetch_series_dataframe,
        "column_name": lambda config: config.series_id,
        "error": us_data.DownloadError,
        "download_stub": "united_states",
    },
}


st.set_page_config(page_title="Macro Dashboard", layout="wide")


@st.cache_data(show_spinner=False)
def _load_series(dataset: str, name: str) -> pd.DataFrame:
    """Load a single series and return a dataframe indexed by date."""

    provider = DATASETS[dataset]
    series_mapping = provider["series"]
    series = series_mapping[name]
    fetch = provider["fetch"]
    column_name_getter = provider["column_name"]

    frame = fetch(series)
    frame_attrs = dict(frame.attrs)
    column_name = column_name_getter(series)

    renamed = frame.rename(
        columns={column_name: series.description or series.slug}
    ).copy()
    renamed.attrs.update(frame_attrs)
    renamed.index.name = "DATE"
    return renamed


def _combine_series(dataset: str, selected: list[str]) -> tuple[pd.DataFrame, list[str]]:
    combined: pd.DataFrame | None = None
    fallback_used: list[str] = []
    for name in selected:
        frame = _load_series(dataset, name)
        if frame.attrs.get("source") == "fallback":
            fallback_used.append(name)
        combined = frame if combined is None else combined.join(frame, how="outer")
    if combined is None:
        return pd.DataFrame(), []
    return combined.sort_index(), fallback_used


def _series_options(dataset: str) -> dict[str, str]:
    series_mapping = DATASETS[dataset]["series"]
    return {
        (config.description or config.slug): name
        for name, config in series_mapping.items()
    }


st.sidebar.title("Configuration")
dataset_names = list(DATASETS.keys())
selected_dataset = st.sidebar.selectbox(
    "Data source",
    dataset_names,
    index=0,
    help="Choose whether to load Euro area data from the ECB or US data from FRED.",
)

st.title("Macroeconomic Dashboard")
if selected_dataset == "Euro area (ECB)":
    st.markdown(
        """
        Explore Euro area consumer prices and reference interest rates sourced from
        the European Central Bank's Statistical Data Warehouse.
        """
    )
else:
    st.markdown(
        """
        Explore consumer prices and reference interest rates sourced from the Federal
        Reserve Economic Data (FRED) service.
        """
    )

options = _series_options(selected_dataset)
labels = list(options.keys())
name_to_label = {name: label for label, name in options.items()}
default_selection = labels
selected_labels = st.multiselect(
    "Select series to display",
    labels,
    default=default_selection,
    help="Choose one or more time series to visualize.",
)

selected_series = [options[label] for label in selected_labels]

error_placeholder = st.empty()

if not selected_series:
    st.info("Select at least one series to display the chart.")
    st.stop()

error_classes = (
    DATASETS[selected_dataset]["error"],
    us_data.DownloadError,
    ecb_data.DownloadError,
)

try:
    data, fallback_series = _combine_series(selected_dataset, selected_series)
except error_classes as exc:
    error_placeholder.error(f"Failed to download data: {exc}")
    st.stop()

if data.empty:
    st.warning("No data available for the selected series.")
    st.stop()

if fallback_series:
    friendly_names = ", ".join(name_to_label.get(name, name) for name in fallback_series)
    st.warning(
        "Using bundled sample data for the following series due to a network issue: "
        f"{friendly_names}."
    )

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
    file_name=f"{DATASETS[selected_dataset]['download_stub']}_macro_filtered.csv",
    mime="text/csv",
    help="Download the currently displayed data as a CSV file.",
)
