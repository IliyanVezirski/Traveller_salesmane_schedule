import pandas as pd

from src.data_loader import load_clients
from src.result_audit import validate_input_clients_for_audit
from src.validation import validate_clients


def _config() -> dict:
    return {
        "working_days": {"weeks": 4, "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]},
        "daily_route": {"max_clients": 20},
    }


def test_load_clients_accepts_single_gps_column(tmp_path) -> None:
    path = tmp_path / "clients.csv"
    pd.DataFrame(
        [
            {
                "client_id": "C1",
                "client_name": "Client 1",
                "sales_rep": "Rep A",
                "gps": "42.69804,23.31229",
                "visit_frequency": 4,
            }
        ]
    ).to_csv(path, index=False)

    clients = load_clients(str(path))
    clean, validation = validate_clients(clients, _config())

    assert clients.loc[0, "lat"] == 42.69804
    assert clients.loc[0, "lon"] == 23.31229
    assert len(clean) == 1
    assert validation.empty


def test_load_clients_accepts_gps_te_alias_and_keeps_old_lat_lon_support(tmp_path) -> None:
    gps_path = tmp_path / "clients_gps_alias.csv"
    pd.DataFrame(
        [
            {
                "client_id": "C1",
                "client_name": "Client 1",
                "sales_rep": "Rep A",
                "GPS-te": "42.69804,23.31229",
                "visit_frequency": 4,
            }
        ]
    ).to_csv(gps_path, index=False)

    gps_clients = load_clients(str(gps_path))
    assert gps_clients.loc[0, "lat"] == 42.69804
    assert gps_clients.loc[0, "lon"] == 23.31229

    lat_lon_path = tmp_path / "clients_lat_lon.csv"
    pd.DataFrame(
        [
            {
                "client_id": "C2",
                "client_name": "Client 2",
                "sales_rep": "Rep A",
                "lat": 42.70,
                "lon": 23.30,
                "visit_frequency": 2,
            }
        ]
    ).to_csv(lat_lon_path, index=False)

    lat_lon_clients = load_clients(str(lat_lon_path))
    assert lat_lon_clients.loc[0, "lat"] == 42.70
    assert lat_lon_clients.loc[0, "lon"] == 23.30


def test_input_audit_accepts_gps_without_lat_lon() -> None:
    clients = pd.DataFrame(
        [
            {
                "client_id": "C1",
                "client_name": "Client 1",
                "sales_rep": "Rep A",
                "gps": "42.69804,23.31229",
                "visit_frequency": 4,
            }
        ]
    )

    assert not [issue for issue in validate_input_clients_for_audit(clients) if issue["severity"] == "ERROR"]
