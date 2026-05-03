import pandas as pd

from src.validation import validate_clients


def test_validation_catches_invalid_frequency() -> None:
    config = {
        "working_days": {
            "weeks": 4,
            "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        },
        "daily_route": {"max_clients": 20},
    }
    clients = pd.DataFrame(
        [
            {
                "client_id": "C-001",
                "client_name": "Invalid Frequency Client",
                "sales_rep": "Rep A",
                "lat": 42.6977,
                "lon": 23.3219,
                "visit_frequency": 3,
            }
        ]
    )

    clean, validation = validate_clients(clients, config)

    assert clean.empty
    assert validation["severity"].eq("ERROR").any()
    assert validation["field"].eq("visit_frequency").any()
