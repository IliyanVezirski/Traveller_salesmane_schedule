"""Client clustering for compact candidate route generation."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans


def _auto_cluster_count(n_clients: int) -> int:
    if n_clients <= 3:
        return max(1, n_clients)
    return int(min(12, max(3, round(math.sqrt(n_clients)))))


def cluster_clients(df_rep: pd.DataFrame, distance_matrix: np.ndarray | None, config: dict) -> pd.DataFrame:
    """Assign cluster_id and cluster summary columns to one rep's clients."""
    out = df_rep.copy().reset_index(drop=True)
    if "cluster_manual" in out.columns and out["cluster_manual"].notna().any():
        out["cluster_id"] = out["cluster_manual"].fillna("manual_missing").astype(str)
    else:
        n_clusters = _auto_cluster_count(len(out))
        if n_clusters <= 1:
            labels = np.zeros(len(out), dtype=int)
        else:
            model = KMeans(n_clusters=n_clusters, random_state=int(config["candidate_routes"].get("random_seed", 42)), n_init=10)
            labels = model.fit_predict(out[["lat", "lon"]].to_numpy(dtype=float))
        out["cluster_id"] = labels.astype(str)

    summary = out.groupby("cluster_id").agg(cluster_size=("client_id", "size"), cluster_monthly_visits=("visit_frequency", "sum")).reset_index()
    out = out.merge(summary, on="cluster_id", how="left")
    return out
