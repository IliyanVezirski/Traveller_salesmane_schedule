from __future__ import annotations

from urllib.parse import unquote

import pandas as pd

from src.osrm_matrix import build_distance_matrix_for_rep


class _FakeResponse:
    status_code = 200
    reason = "OK"
    text = ""

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def _coords_from_url(url: str) -> list[tuple[float, float]]:
    coord_text = unquote(url.rsplit("/table/v1/driving/", 1)[1])
    return [tuple(map(float, coord.split(",", 1))) for coord in coord_text.split(";")]


def _indices(value: str | None, default_size: int) -> list[int]:
    if value is None:
        return list(range(default_size))
    return [int(part) for part in value.split(";") if part != ""]


def test_osrm_large_matrix_is_requested_in_chunks(monkeypatch, tmp_path) -> None:
    calls: list[int] = []

    def fake_get(url: str, params: dict | None = None, timeout: int = 30) -> _FakeResponse:
        coords = _coords_from_url(url)
        calls.append(len(coords))
        assert len(coords) <= 4

        params = params or {}
        source_indices = _indices(params.get("sources"), len(coords))
        destination_indices = _indices(params.get("destinations"), len(coords))

        distances = []
        durations = []
        for source_index in source_indices:
            distance_row = []
            duration_row = []
            source_lon, source_lat = coords[source_index]
            for destination_index in destination_indices:
                destination_lon, destination_lat = coords[destination_index]
                distance = abs(source_lon - destination_lon) * 1000 + abs(source_lat - destination_lat) * 1000
                distance_row.append(distance)
                duration_row.append(distance / 10)
            distances.append(distance_row)
            durations.append(duration_row)

        return _FakeResponse({"code": "Ok", "distances": distances, "durations": durations})

    monkeypatch.setattr("src.osrm_matrix.requests.get", fake_get)
    clients = pd.DataFrame(
        [
            {"client_id": f"C{i}", "client_name": f"C{i}", "sales_rep": "Rep", "lat": 42.0 + i, "lon": 23.0 + i, "visit_frequency": 4}
            for i in range(6)
        ]
    )
    config = {
        "osrm": {
            "url": "http://localhost:5000",
            "use_osrm": True,
            "use_cache": False,
            "fallback_to_haversine": False,
            "request_timeout_seconds": 30,
            "max_table_locations": 4,
        }
    }

    matrix = build_distance_matrix_for_rep(clients, config, str(tmp_path))

    assert matrix["source"] == "osrm"
    assert matrix["distance_matrix_m"].shape == (6, 6)
    assert matrix["duration_matrix_s"].shape == (6, 6)
    assert len(calls) == 9
    assert max(calls) == 4
