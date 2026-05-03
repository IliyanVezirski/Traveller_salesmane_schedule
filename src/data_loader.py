"""Input loading utilities for client master data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {"client_id", "client_name", "sales_rep", "lat", "lon", "visit_frequency"}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with normalized snake_case column names."""
    out = df.copy()
    out.columns = [str(c).strip().lower().replace(" ", "_") for c in out.columns]
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
    missing = sorted(REQUIRED_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

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
