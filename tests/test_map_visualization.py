import pandas as pd

from src.map_visualization import generate_schedule_map


def test_generated_map_contains_day_and_sales_rep_filters(tmp_path) -> None:
    schedule = pd.DataFrame(
        [
            {
                "day_index": 0,
                "week_index": 1,
                "weekday": "Monday",
                "sales_rep": "Rep A",
                "route_order": 1,
                "client_id": "C1",
                "client_name": "Client 1",
                "lat": 42.70,
                "lon": 23.30,
                "visit_frequency": 4,
                "territory_weekday": "Monday",
                "global_territory_weekday": "Monday",
                "route_km_total": 10.0,
            },
            {
                "day_index": 1,
                "week_index": 1,
                "weekday": "Tuesday",
                "sales_rep": "Rep B",
                "route_order": 1,
                "client_id": "C2",
                "client_name": "Client 2",
                "lat": 42.71,
                "lon": 23.31,
                "visit_frequency": 2,
                "territory_weekday": "Tuesday",
                "global_territory_weekday": "Tuesday",
                "route_km_total": 8.0,
            },
        ]
    )
    output = tmp_path / "schedule_map.html"

    generate_schedule_map(schedule, str(output))

    html = output.read_text(encoding="utf-8")
    assert "Schedule filters" in html
    assert 'data-filter="weekday"' in html
    assert 'data-filter="sales_rep"' in html
    assert 'data-filter="global_territory"' in html
    assert "Global territory" in html
    assert "Rep A" in html
    assert "Monday" in html
