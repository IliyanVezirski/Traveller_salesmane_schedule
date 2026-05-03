from pathlib import Path

import pandas as pd

from scripts.generate_synthetic_clients import REQUIRED_COLUMNS, generate_synthetic_clients
from src.result_audit import validate_input_clients_for_audit


def test_full_1800_sofia_dataset_has_expected_shape_and_capacity(tmp_path: Path) -> None:
    output_path = tmp_path / "synthetic_1800_sofia.xlsx"

    df = generate_synthetic_clients(18, 1800, "full_1800", str(output_path), random_seed=42)

    assert output_path.exists()
    assert list(df.columns) == REQUIRED_COLUMNS
    assert len(df) == 1800
    assert df["sales_rep"].nunique() == 18
    assert df["visit_frequency"].value_counts().to_dict() == {4: 900, 2: 700, 8: 200}
    assert int(df["visit_frequency"].sum()) == 6600
    assert int(df.groupby("sales_rep")["visit_frequency"].sum().max()) <= 400
    assert df["lat"].between(41.90, 43.10).all()
    assert df["lon"].between(22.40, 24.30).all()
    assert not [issue for issue in validate_input_clients_for_audit(df) if issue["severity"] == "ERROR"]


def test_infeasible_capacity_dataset_is_flagged(tmp_path: Path) -> None:
    output_path = tmp_path / "synthetic_infeasible_capacity.xlsx"

    df = generate_synthetic_clients(18, 1800, "infeasible_capacity", str(output_path), random_seed=42)

    assert output_path.exists()
    assert int(pd.to_numeric(df["visit_frequency"], errors="coerce").fillna(0).sum()) > 18 * 20 * 22
    issues = validate_input_clients_for_audit(df)
    assert any(issue["severity"] == "ERROR" and issue.get("field") == "capacity" for issue in issues)
