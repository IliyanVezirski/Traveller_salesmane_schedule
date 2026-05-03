"""Penalty helpers for periodic visit spacing."""

from __future__ import annotations


def create_frequency_2_spacing_penalty(selected_weeks: list[int]) -> int:
    """Return spacing penalty for two monthly visits."""
    weeks = sorted(selected_weeks)
    if len(weeks) != 2:
        return 10_000
    pair = tuple(weeks)
    if pair in {(1, 3), (2, 4)}:
        return 0
    if pair == (1, 4):
        return 1
    if pair in {(1, 2), (3, 4)}:
        return 5
    return 3


def create_frequency_8_consecutive_day_penalty(selected_days: list[int]) -> int:
    """Return penalty when two weekly visits are on consecutive weekdays."""
    days = sorted(selected_days)
    if len(days) != 2:
        return 10_000
    return 1 if abs(days[0] - days[1]) == 1 else 0
