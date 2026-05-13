"""Leaflet map export for final route schedules."""

from __future__ import annotations

import html
import json
from pathlib import Path

import pandas as pd


DAY_COLORS = {
    "Monday": "#1f78b4",
    "Tuesday": "#33a02c",
    "Wednesday": "#e31a1c",
    "Thursday": "#ff7f00",
    "Friday": "#6a3d9a",
}


def _route_payload(daily_routes_df: pd.DataFrame) -> list[dict]:
    routes: list[dict] = []
    sort_cols = [col for col in ["sales_rep", "day_index", "route_order"] if col in daily_routes_df.columns]
    sorted_df = daily_routes_df.sort_values(sort_cols) if sort_cols else daily_routes_df.copy()
    for (sales_rep, day_index), route_df in sorted_df.groupby(["sales_rep", "day_index"], sort=True):
        first = route_df.iloc[0]
        weekday = str(first.get("weekday", ""))
        points = []
        for row in route_df.itertuples(index=False):
            points.append(
                {
                    "lat": float(row.lat),
                    "lon": float(row.lon),
                    "route_order": int(getattr(row, "route_order", 0) or 0),
                    "client_id": str(getattr(row, "client_id", "")),
                    "client_name": str(getattr(row, "client_name", "")),
                    "visit_frequency": int(getattr(row, "visit_frequency", 0) or 0),
                    "territory_weekday": str(getattr(row, "territory_weekday", "") or ""),
                    "global_territory_weekday": str(getattr(row, "global_territory_weekday", "") or ""),
                }
            )
        routes.append(
            {
                "sales_rep": str(sales_rep),
                "day_index": int(day_index),
                "week_index": int(first.get("week_index", 0) or 0),
                "weekday": weekday,
                "color": DAY_COLORS.get(weekday, "#555555"),
                "route_km_total": float(first.get("route_km_total", 0.0) or 0.0),
                "points": points,
            }
        )
    return routes


def _checkboxes(values: list[str], group_name: str) -> str:
    rows = []
    for value in values:
        safe_value = html.escape(value, quote=True)
        rows.append(
            f'<label class="filter-option"><input type="checkbox" data-filter="{group_name}" value="{safe_value}" checked> '
            f"<span>{html.escape(value)}</span></label>"
        )
    return "\n".join(rows)


def generate_schedule_map(daily_routes_df: pd.DataFrame, output_path: str) -> None:
    """Create an HTML map with filters by weekday and sales rep."""
    if daily_routes_df.empty:
        return

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    routes = _route_payload(daily_routes_df)
    center_lat = float(daily_routes_df["lat"].mean())
    center_lon = float(daily_routes_df["lon"].mean())
    weekdays = [day for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"] if day in set(daily_routes_df["weekday"].astype(str))]
    global_territories = [
        day
        for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        if "global_territory_weekday" in daily_routes_df.columns and day in set(daily_routes_df["global_territory_weekday"].dropna().astype(str))
    ]
    sales_reps = sorted(daily_routes_df["sales_rep"].astype(str).unique().tolist())
    routes_json = json.dumps(routes, ensure_ascii=False)

    html_text = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sales PVRP Schedule Map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    html, body, #map {{
      height: 100%;
      margin: 0;
      font-family: Arial, sans-serif;
    }}
    .filter-panel {{
      position: absolute;
      top: 12px;
      right: 12px;
      z-index: 1000;
      width: min(360px, calc(100vw - 24px));
      max-height: calc(100vh - 24px);
      overflow: auto;
      background: rgba(255, 255, 255, 0.96);
      border: 1px solid #c9d3df;
      border-radius: 6px;
      box-shadow: 0 4px 18px rgba(0, 0, 0, 0.18);
      padding: 10px 12px;
      color: #1f2933;
      font-size: 13px;
    }}
    .filter-title {{
      font-weight: 700;
      margin: 2px 0 8px;
    }}
    .filter-section {{
      border-top: 1px solid #e2e8f0;
      padding-top: 8px;
      margin-top: 8px;
    }}
    .filter-actions {{
      display: flex;
      gap: 6px;
      margin: 6px 0;
    }}
    .filter-actions button {{
      border: 1px solid #a9b8c8;
      background: #f7fafc;
      border-radius: 4px;
      padding: 3px 7px;
      cursor: pointer;
      font-size: 12px;
    }}
    .filter-grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 4px;
    }}
    .filter-option {{
      display: flex;
      align-items: center;
      gap: 6px;
      line-height: 1.25;
    }}
    .stats {{
      margin-top: 8px;
      color: #475569;
      font-size: 12px;
    }}
    .leaflet-popup-content {{
      min-width: 180px;
    }}
  </style>
</head>
<body>
  <div id="map"></div>
  <aside class="filter-panel">
    <div class="filter-title">Schedule filters</div>
    <div class="filter-section">
      <strong>Days</strong>
      <div class="filter-actions">
        <button type="button" data-action="all" data-group="weekday">All</button>
        <button type="button" data-action="none" data-group="weekday">None</button>
      </div>
      <div class="filter-grid">
        {_checkboxes(weekdays, "weekday")}
      </div>
    </div>
    <div class="filter-section">
      <strong>Sales reps</strong>
      <div class="filter-actions">
        <button type="button" data-action="all" data-group="sales_rep">All</button>
        <button type="button" data-action="none" data-group="sales_rep">None</button>
      </div>
      <div class="filter-grid">
        {_checkboxes(sales_reps, "sales_rep")}
      </div>
    </div>
    <div class="filter-section">
      <strong>Global territory</strong>
      <div class="filter-actions">
        <button type="button" data-action="all" data-group="global_territory">All</button>
        <button type="button" data-action="none" data-group="global_territory">None</button>
      </div>
      <div class="filter-grid">
        {_checkboxes(global_territories, "global_territory") if global_territories else '<span class="muted">No global territory data</span>'}
      </div>
    </div>
    <div class="stats" id="stats"></div>
  </aside>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const routes = {routes_json};
    const map = L.map("map").setView([{center_lat:.7f}, {center_lon:.7f}], 11);
    L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap contributors"
    }}).addTo(map);

    const layerGroup = L.layerGroup().addTo(map);
    const stats = document.getElementById("stats");

    function selectedValues(group) {{
      return new Set(Array.from(document.querySelectorAll(`input[data-filter="${{group}}"]:checked`)).map(input => input.value));
    }}

    function popupHtml(route, point) {{
      return `
        <strong>${{escapeHtml(point.route_order + ". " + point.client_name)}}</strong><br>
        Client: ${{escapeHtml(point.client_id)}}<br>
        Rep: ${{escapeHtml(route.sales_rep)}}<br>
        Week ${{route.week_index}}, ${{escapeHtml(route.weekday)}} / day ${{route.day_index}}<br>
        Local territory: ${{escapeHtml(point.territory_weekday || "")}}<br>
        Global territory: ${{escapeHtml(point.global_territory_weekday || "")}}<br>
        Frequency: ${{point.visit_frequency}}<br>
        Route km: ${{route.route_km_total.toFixed(1)}}
      `;
    }}

    function escapeHtml(value) {{
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }}

    function render() {{
      const selectedWeekdays = selectedValues("weekday");
      const selectedReps = selectedValues("sales_rep");
      const selectedGlobalTerritories = selectedValues("global_territory");
      layerGroup.clearLayers();
      const bounds = [];
      let visibleRoutes = 0;
      let visibleStops = 0;

      for (const route of routes) {{
      if (!selectedWeekdays.has(route.weekday) || !selectedReps.has(route.sales_rep)) {{
          continue;
        }}
        const hasGlobalTerritoryFilter = document.querySelectorAll('input[data-filter="global_territory"]').length > 0;
        const visiblePoints = route.points.filter(point => !hasGlobalTerritoryFilter || selectedGlobalTerritories.has(point.global_territory_weekday));
        if (visiblePoints.length === 0) {{
          continue;
        }}
        visibleRoutes += 1;
        visibleStops += visiblePoints.length;
        const coords = visiblePoints.map(point => [point.lat, point.lon]);
        if (coords.length >= 2) {{
          L.polyline(coords, {{ color: route.color, weight: 4, opacity: 0.78 }}).addTo(layerGroup);
        }}
        for (const point of visiblePoints) {{
          const marker = L.circleMarker([point.lat, point.lon], {{
            radius: 6,
            color: route.color,
            weight: 2,
            fillColor: route.color,
            fillOpacity: 0.85
          }});
          marker.bindTooltip(`${{point.route_order}}. ${{point.client_name}}`, {{ sticky: true }});
          marker.bindPopup(popupHtml(route, point));
          marker.addTo(layerGroup);
          bounds.push([point.lat, point.lon]);
        }}
      }}

      stats.textContent = `${{visibleRoutes}} routes, ${{visibleStops}} stops visible`;
      if (bounds.length > 0) {{
        map.fitBounds(bounds, {{ padding: [24, 24], maxZoom: 14 }});
      }}
    }}

    document.querySelectorAll("input[data-filter]").forEach(input => input.addEventListener("change", render));
    document.querySelectorAll("button[data-action]").forEach(button => {{
      button.addEventListener("click", () => {{
        const checked = button.dataset.action === "all";
        document.querySelectorAll(`input[data-filter="${{button.dataset.group}}"]`).forEach(input => input.checked = checked);
        render();
      }});
    }});

    render();
  </script>
</body>
</html>
"""
    path.write_text(html_text, encoding="utf-8")
