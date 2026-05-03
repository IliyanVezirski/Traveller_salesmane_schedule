from pathlib import Path

import yaml

from src.data_loader import load_clients
from src.osrm_matrix import build_distance_matrix_for_rep
from src.validation import validate_clients


ROOT = Path(__file__).resolve().parents[1]


def test_sample_data_can_be_loaded_and_validated() -> None:
    with (ROOT / "config.yaml").open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)

    clients = load_clients(str(ROOT / "data" / "sample_clients.xlsx"))
    clean, validation = validate_clients(clients, config)

    assert len(clients) == 40
    assert clean["sales_rep"].nunique() == 2
    assert set(clean["visit_frequency"].unique()) == {2, 4, 8}
    assert validation.empty or not validation["severity"].eq("ERROR").any()


def test_osrm_fallback_returns_haversine_matrix(tmp_path) -> None:
    with (ROOT / "config.yaml").open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    config["osrm"].update(
        {
            "use_osrm": True,
            "fallback_to_haversine": True,
            "use_cache": False,
            "url": "http://127.0.0.1:9",
            "request_timeout_seconds": 1,
        }
    )

    clients = load_clients(str(ROOT / "data" / "sample_clients.xlsx"))
    clean, _ = validate_clients(clients, config)
    _, rep_df = next(iter(clean.groupby("sales_rep")))
    matrix = build_distance_matrix_for_rep(rep_df, config, str(tmp_path))

    assert matrix["source"] == "haversine"
    assert matrix["distance_matrix_m"].shape == (20, 20)
