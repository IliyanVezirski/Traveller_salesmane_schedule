"""Distance matrix creation with OSRM Table API and haversine fallback."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests


def _cache_key(df_rep: pd.DataFrame) -> str:
    payload = "|".join(f"{r.client_id}:{float(r.lat):.7f}:{float(r.lon):.7f}" for r in df_rep.sort_values("client_id").itertuples())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def _haversine_matrix(coords: np.ndarray) -> np.ndarray:
    lat = np.radians(coords[:, 0])
    lon = np.radians(coords[:, 1])
    dlat = lat[:, None] - lat[None, :]
    dlon = lon[:, None] - lon[None, :]
    a = np.sin(dlat / 2) ** 2 + np.cos(lat[:, None]) * np.cos(lat[None, :]) * np.sin(dlon / 2) ** 2
    km = 6371.0088 * 2 * np.arcsin(np.sqrt(a))
    return km * 1000.0


def _try_osrm(df_rep: pd.DataFrame, config: dict) -> tuple[np.ndarray, np.ndarray | None] | None:
    coords = ";".join(f"{float(r.lon)},{float(r.lat)}" for r in df_rep.itertuples())
    url = f"{config['osrm']['url'].rstrip('/')}/table/v1/driving/{coords}"
    params = {"annotations": "distance,duration"}
    response = requests.get(url, params=params, timeout=int(config["osrm"].get("request_timeout_seconds", 30)))
    response.raise_for_status()
    data = response.json()
    if data.get("code") != "Ok":
        raise RuntimeError(f"OSRM returned {data.get('code')}: {data.get('message')}")
    distances = np.asarray(data["distances"], dtype=float)
    durations = np.asarray(data.get("durations"), dtype=float) if data.get("durations") is not None else None
    return distances, durations


def build_distance_matrix_for_rep(df_rep: pd.DataFrame, config: dict, cache_dir: str) -> dict[str, Any]:
    """Build or load a square distance matrix for one sales representative."""
    df_rep = df_rep.sort_values("client_id").reset_index(drop=True)
    rep_name = str(df_rep["sales_rep"].iloc[0]).replace(" ", "_")
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    cache_file = cache_path / f"{rep_name}_{_cache_key(df_rep)}.npz"
    use_cache = bool(config["osrm"].get("use_cache", True))

    if use_cache and cache_file.exists():
        loaded = np.load(cache_file, allow_pickle=True)
        duration = loaded["duration_matrix_s"] if "duration_matrix_s" in loaded.files and loaded["duration_matrix_s"].size else None
        source = str(loaded["source"].item()) if hasattr(loaded["source"], "item") else str(loaded["source"])
        return {"client_ids": loaded["client_ids"].astype(str).tolist(), "distance_matrix_m": loaded["distance_matrix_m"], "duration_matrix_s": duration, "source": source}

    source = "haversine"
    duration_matrix = None
    coords = df_rep[["lat", "lon"]].to_numpy(dtype=float)
    distance_matrix = _haversine_matrix(coords)
    if bool(config["osrm"].get("use_osrm", True)):
        try:
            osrm_result = _try_osrm(df_rep, config)
            if osrm_result is not None:
                distance_matrix, duration_matrix = osrm_result
                source = "osrm"
        except Exception:
            if not bool(config["osrm"].get("fallback_to_haversine", True)):
                raise

    distance_matrix = np.nan_to_num(distance_matrix, nan=1e9, posinf=1e9)
    np.fill_diagonal(distance_matrix, 0.0)
    if duration_matrix is not None:
        duration_matrix = np.nan_to_num(duration_matrix, nan=1e9, posinf=1e9)
        np.fill_diagonal(duration_matrix, 0.0)

    if use_cache:
        np.savez_compressed(cache_file, client_ids=df_rep["client_id"].astype(str).to_numpy(), distance_matrix_m=distance_matrix, duration_matrix_s=np.asarray([]) if duration_matrix is None else duration_matrix, source=np.asarray(source))
    return {"client_ids": df_rep["client_id"].astype(str).tolist(), "distance_matrix_m": distance_matrix, "duration_matrix_s": duration_matrix, "source": source}
