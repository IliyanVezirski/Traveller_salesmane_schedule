"""Folium map export for final route schedules."""

from __future__ import annotations

from pathlib import Path

import folium
import pandas as pd


def generate_schedule_map(daily_routes_df: pd.DataFrame, output_path: str) -> None:
    """Create an HTML map with route lines grouped by sales rep and day."""
    if daily_routes_df.empty:
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    center = [float(daily_routes_df["lat"].mean()), float(daily_routes_df["lon"].mean())]
    fmap = folium.Map(location=center, zoom_start=8)

    for (sales_rep, day), route_df in daily_routes_df.sort_values("route_order").groupby(["sales_rep", "day_index"]):
        label = f"{sales_rep} - day {day}"
        group = folium.FeatureGroup(name=label)
        coords = []
        for row in route_df.itertuples(index=False):
            coords.append([float(row.lat), float(row.lon)])
            popup = f"{row.client_name}<br>{row.sales_rep}<br>{row.weekday} / day {row.day_index}<br>freq {row.visit_frequency}"
            folium.Marker(location=[float(row.lat), float(row.lon)], popup=popup, tooltip=f"{row.route_order}. {row.client_name}").add_to(group)
        if len(coords) >= 2:
            folium.PolyLine(coords, weight=3, opacity=0.75).add_to(group)
        group.add_to(fmap)
    folium.LayerControl().add_to(fmap)
    fmap.save(path)
