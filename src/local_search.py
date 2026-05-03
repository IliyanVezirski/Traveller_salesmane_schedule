"""Local-search extension point for post-solver route-first improvements."""

from __future__ import annotations

from typing import Any

import pandas as pd


def improve_solution(selected_candidates_df: pd.DataFrame, candidates_df: pd.DataFrame, clients_df: pd.DataFrame, calendar_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Return selected candidates unchanged in v1.

    The stable interface allows future moves such as candidate replacement,
    day swaps, and high-km substitutions while preserving frequency validity.
    """
    return selected_candidates_df.copy()
