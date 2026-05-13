"""Input loading utilities for client master data."""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd


BASE_REQUIRED_COLUMNS = {"client_id", "client_name", "sales_rep", "visit_frequency"}
GPS_COLUMNS = ("gps", "gps_te", "gps_coordinates", "coordinates", "lat_lon", "latlon")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with normalized snake_case column names."""
    out = df.copy()
    out.columns = [re.sub(r"[^0-9a-zA-Z]+", "_", str(c).strip().lower()).strip("_") for c in out.columns]
    return out


def _gps_column(df: pd.DataFrame) -> str | None:
    for col in GPS_COLUMNS:
        if col in df.columns:
            return col
    return None


def _parse_gps_value(value: object) -> tuple[float | None, float | None]:
    if pd.isna(value):
        return None, None
    text = str(value).strip().replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    parts = [part.strip() for part in re.split(r"[,;]", text) if part.strip()]
    if len(parts) != 2:
        return None, None
    lat = pd.to_numeric(parts[0], errors="coerce")
    lon = pd.to_numeric(parts[1], errors="coerce")
    if pd.isna(lat) or pd.isna(lon):
        return None, None
    return float(lat), float(lon)


def _ensure_lat_lon_from_gps(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    gps_col = _gps_column(out)
    has_lat_lon = {"lat", "lon"}.issubset(out.columns)
    if not has_lat_lon and gps_col is None:
        raise ValueError("Missing required coordinate columns: provide either gps or both lat and lon.")

    if gps_col is not None:
        if "gps" not in out.columns:
            out["gps"] = out[gps_col]
        parsed = out[gps_col].map(_parse_gps_value)
        gps_lat = pd.Series([item[0] for item in parsed], index=out.index, dtype="float64")
        gps_lon = pd.Series([item[1] for item in parsed], index=out.index, dtype="float64")
        if "lat" not in out.columns:
            out["lat"] = gps_lat
        else:
            out["lat"] = pd.to_numeric(out["lat"], errors="coerce").fillna(gps_lat)
        if "lon" not in out.columns:
            out["lon"] = gps_lon
        else:
            out["lon"] = pd.to_numeric(out["lon"], errors="coerce").fillna(gps_lon)

    return out


def load_clients(path: str) -> pd.DataFrame:
    """Load clients from Excel or CSV and coerce required column types."""
    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if input_path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        df = pd.read_excel(input_path)
    elif input_path.suffix.lower() == ".csv":
        df = pd.read_csv(input_path)
    else:
        raise ValueError(f"Unsupported input file type: {input_path.suffix}")

    df = normalize_columns(df)
    missing = sorted(BASE_REQUIRED_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    df = _ensure_lat_lon_from_gps(df)

    df["client_id"] = df["client_id"].astype("string").str.strip()
    df["client_name"] = df["client_name"].astype("string").str.strip()
    df["sales_rep"] = df["sales_rep"].astype("string").str.strip()
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["visit_frequency"] = pd.to_numeric(df["visit_frequency"], errors="coerce").astype("Int64")

    for col in ["fixed_weekday", "forbidden_weekdays", "preferred_weekdays", "cluster_manual", "notes"]:
        if col not in df.columns:
            df[col] = pd.NA
    return df
