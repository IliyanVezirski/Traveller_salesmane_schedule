"""Calendar construction for the fixed 4x5 working period."""

from __future__ import annotations

import pandas as pd


def build_calendar(config: dict) -> pd.DataFrame:
    """Build day/week/weekday table from configuration."""
    rows: list[dict[str, int | str]] = []
    weekdays = config["working_days"]["weekdays"]
    for week_index in range(1, int(config["working_days"]["weeks"]) + 1):
        for weekday_index, weekday in enumerate(weekdays):
            rows.append(
                {
                    "day_index": len(rows),
                    "week_index": week_index,
                    "weekday": weekday,
                    "weekday_index": weekday_index,
                }
            )
    return pd.DataFrame(rows)
