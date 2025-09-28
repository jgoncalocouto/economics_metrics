# ecb_suite.py
from __future__ import annotations
import argparse
import os
import time
from typing import Dict, List, Optional

import pandas as pd
from ecbdata import ecbdata

# =========================
# Config
# =========================
EURO_AREA_CODES: List[str] = [
    "U2",  # Euro area aggregate
    "AT","BE","CY","DE","EE","ES","FI","FR","GR","IE",
    "IT","LT","LU","LV","MT","NL","PT","SI","SK"
]

# HICP "all items" (ECOICOP)
ALL_ITEMS = "000000"

# Broad sector aggregates (ECOICOP / ICP aggregates)
SECTORS: Dict[str, str] = {
    "ALL_ITEMS": "000000",
    "ENERGY":    "NRGY00",
    "FOOD":      "FOOD00",       # Food incl. alcohol & tobacco (ECB aggregate)
    "SERVICES":  "SERV00",
    "GOODS_X_ENERGY": "IGXE00",  # Industrial goods excl. energy
    # Add more if you want: "HOUSING": "CP04", "TRANSPORT": "CP07", ...
}

# Measures:
#  - "ANR" (annual rate of change, % YoY)
#  - "INX" (index, 2015=100)
MEASURE_CHOICES = {"ANR", "INX"}

# ECB Euribor series keys (monthly, historical close/avg through period)
EURIBOR_SERIES: Dict[str, str] = {
    "euribor_3m":  "FM.M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "euribor_6m":  "FM.M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
    "euribor_12m": "FM.M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA",
}

# Common FX currencies (Daily reference rates vs EUR)
CURRENCIES = [
    "USD","GBP","JPY","CHF","CNY","AUD","CAD","NOK","SEK","DKK",
    "PLN","CZK","HUF","TRY","ZAR","BRL","INR","KRW","MXN","NZD"
]

# Optional: force proxy via ecbdata if needed (requests also respects HTTPS_PROXY/HTTP_PROXY)
# ecbdata.connect(proxies={"https": os.getenv("HTTPS_PROXY", ""), "http": os.getenv("HTTP_PROXY", "")})


# =========================
# Low-level helpers
# =========================
def _normalize_ecb_df(df: pd.DataFrame, date_key="TIME_PERIOD", value_key="OBS_VALUE") -> pd.Series:
    """Normalize ECB SDMX dataframe to a float Series indexed by datetime."""
    cols = {c.upper(): c for c in df.columns}
    date_col = cols.get(date_key.upper(), date_key)
    val_col  = cols.get(value_key.upper(), value_key)

    df = df.rename(columns={date_col: "date", val_col: "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    s = (df[["date", "value"]]
         .dropna(subset=["date", "value"])
         .set_index("date")
         .sort_index()["value"])

    # Coerce to float (handles decimal commas just in case)
    s = pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False), errors="coerce").dropna()
    s.index = pd.to_datetime(s.index)
    return s


def fetch_series_retry(key: str,
                       start: Optional[str] = None,
                       end: Optional[str] = None,
                       retries: int = 3,
                       pause_s: float = 1.0) -> pd.Series:
    """Retry wrapper around ecbdata.get_series -> normalized monthly/daily Series."""
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            raw = ecbdata.get_series(key, start=start, end=end)
            return _normalize_ecb_df(raw)
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(pause_s)
            else:
                raise
    raise RuntimeError(f"Failed to fetch series {key}: {last_err}")


# =========================
# Euribor (monthly)
# =========================
def download_euribor_all(start: Optional[str], end: Optional[str],
                         series_map: Dict[str, str]) -> pd.DataFrame:
    """Return wide DataFrame with columns for 3M/6M/12M Euribor (monthly)."""
    cols = []
    for name, key in series_map.items():
        print(f"Fetching {name} …")
        s = fetch_series_retry(key, start=start, end=end).rename(name)
        cols.append(s)
    out = pd.concat(cols, axis=1).sort_index().astype("float64")
    return out


# =========================
# FX (daily)
# =========================
def fetch_exr_daily(codes=CURRENCIES, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """
    ECB EXR dataset: daily euro reference rates (foreign currency per 1 EUR).
    Key pattern: EXR.D.<CUR>.EUR.SP00.A
    """
    series = {}
    for cur in codes:
        key = f"EXR.D.{cur}.EUR.SP00.A"
        print(f"Fetching FX {cur}/EUR …")
        s = fetch_series_retry(key, start=start, end=end).rename(cur)
        series[cur] = s
    out = pd.concat(series, axis=1).sort_index().astype(float)
    return out


# =========================
# HICP (monthly)
# =========================
def _hicp_key(geo: str, coicop: str, measure: str) -> str:
    """ICP.M.<GEO>.N.<COICOP>.4.<MEASURE>"""
    if measure not in MEASURE_CHOICES:
        raise ValueError(f"measure must be one of {MEASURE_CHOICES}")
    return f"ICP.M.{geo}.N.{coicop}.4.{measure}"


def hicp_all_items_by_country(geos: List[str], measure: str,
                              start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    """
    Wide DataFrame (UNCHANGED): one column per country (U2 + members) for HICP all-items.
    """
    cols = []
    for geo in geos:
        key = _hicp_key(geo, ALL_ITEMS, measure)
        print(f"Fetching HICP all-items for {geo} [{measure}] …")
        s = fetch_series_retry(key, start=start, end=end).rename(geo)
        cols.append(s)
    out = pd.concat(cols, axis=1).sort_index().astype(float)
    return out


def hicp_by_sector_long(geos: List[str], sectors: Dict[str, str], measure: str,
                        start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    """
    Build a tidy base: date | sector | geo | value
    """
    rows = []
    for sector_name, coicop in sectors.items():
        for geo in geos:
            key = _hicp_key(geo, coicop, measure)
            print(f"Fetching {sector_name} for {geo} [{measure}] …")
            s = fetch_series_retry(key, start=start, end=end)
            if s.empty:
                continue
            df = s.reset_index().rename(columns={0: "value"})
            df["sector"] = sector_name
            df["geo"] = geo
            rows.append(df[["date", "sector", "geo", "value"]])
    if not rows:
        return pd.DataFrame(columns=["date", "sector", "geo", "value"])
    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["date", "sector", "geo"]).reset_index(drop=True)
    return out


def pivots_from_hicp_long(df_long: pd.DataFrame,
                          sectors: Dict[str, str],
                          geos: List[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    From tidy (date|sector|geo|value), build:
      - filterbyCountry: date | country | <one column per sector>
      - filterbySector:  date | sector  | <one column per country>
    """
    # filterbyCountry
    df_country = (df_long.rename(columns={"geo": "country"})
                  .pivot_table(index=["date", "country"],
                               columns="sector",
                               values="value",
                               aggfunc="first")
                  .reset_index()
                  .sort_values(["date", "country"]))
    # Stable sector order (as defined in SECTORS)
    sector_order = list(sectors.keys())
    existing = [c for c in sector_order if c in df_country.columns]
    others = [c for c in df_country.columns if c not in existing + ["date", "country"]]
    df_country = df_country[["date", "country"] + existing + others]

    # filterbySector
    df_sector = (df_long
                 .pivot_table(index=["date", "sector"],
                              columns="geo",
                              values="value",
                              aggfunc="first")
                 .reset_index()
                 .sort_values(["date", "sector"]))
    geo_order = [g for g in geos if g in df_sector.columns]
    others_geo = [c for c in df_sector.columns if c not in geo_order + ["date", "sector"]]
    df_sector = df_sector[["date", "sector"] + geo_order + others_geo]

    return df_country, df_sector


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ECB data suite: Euribor (monthly), FX (daily), and HICP (monthly).")
    p.add_argument("--start", default=None, help="Start (YYYY-MM for monthly; YYYY-MM-DD ok for FX).")
    p.add_argument("--end",   default=None, help="End (YYYY-MM for monthly; YYYY-MM-DD ok for FX).")
    p.add_argument("--run",   default="all",
                   choices=["all", "euribor", "fx", "hicp_agg", "hicp_sector"],
                   help="Which part to run (default: all).")

    # Euribor / FX outputs
    p.add_argument("--out-euribor", default="euribor_3m_6m_12m_ecb.csv",
                   help="Output CSV for Euribor (monthly).")
    p.add_argument("--out-fx",      default="fx_daily_ecb.csv",
                   help="Output CSV for FX daily reference rates.")

    # HICP outputs
    p.add_argument("--measure", choices=list(MEASURE_CHOICES), default="ANR",
                   help="HICP measure: ANR (YoY %) or INX (index 2015=100). Default: ANR")
    p.add_argument("--out-hicp-agg", default="hicp_all_items_by_country.csv",
                   help="HICP all-items by country (wide).")
    p.add_argument("--out-hicp-sector-country", default="hicp_sectors_filterbyCountry.csv",
                   help="By-sector: date|country + sector columns.")
    p.add_argument("--out-hicp-sector-sector",  default="hicp_sectors_filterbySector.csv",
                   help="By-sector: date|sector + country columns.")

    # FX currency selection (optional)
    p.add_argument("--fx-currencies", default=",".join(CURRENCIES),
                   help="Comma-separated list of FX currencies (e.g., USD,GBP,JPY).")

    return p.parse_args()


# =========================
# Main
# =========================
if __name__ == "__main__":
    args = parse_args()

    # Parse FX currencies
    fx_codes = [c.strip().upper() for c in args.fx_currencies.split(",") if c.strip()]

    if args.run in ("all", "euribor"):
        df_eur = download_euribor_all(start=args.start, end=args.end, series_map=EURIBOR_SERIES)
        print(df_eur.tail())
        df_eur.to_csv(args.out_euribor, float_format="%.6f", date_format="%Y-%m-%d")
        print(f"Saved -> {args.out_euribor}")

    if args.run in ("all", "fx"):
        df_fx = fetch_exr_daily(codes=fx_codes, start=args.start, end=args.end)
        print(df_fx.tail())
        df_fx.to_csv(args.out_fx, float_format="%.6f", date_format="%Y-%m-%d")
        print(f"Saved -> {args.out_fx}")

    if args.run in ("all", "hicp_agg"):
        df_agg = hicp_all_items_by_country(EURO_AREA_CODES, measure=args.measure, start=args.start, end=args.end)
        print(df_agg.head())
        df_agg.to_csv(args.out_hicp_agg, float_format="%.4f", date_format="%Y-%m-%d")
        print(f"Saved -> {args.out_hicp_agg}")

    if args.run in ("all", "hicp_sector"):
        df_long = hicp_by_sector_long(EURO_AREA_CODES, SECTORS, measure=args.measure, start=args.start, end=args.end)
        print(df_long.head())

        df_by_country, df_by_sector = pivots_from_hicp_long(df_long, SECTORS, EURO_AREA_CODES)
        df_by_country.to_csv(args.out_hicp_sector_country, index=False, float_format="%.4f", date_format="%Y-%m-%d")
        print(f"Saved -> {args.out_hicp_sector_country}")

        df_by_sector.to_csv(args.out_hicp_sector_sector, index=False, float_format="%.4f", date_format="%Y-%m-%d")
        print(f"Saved -> {args.out_hicp_sector_sector}")
