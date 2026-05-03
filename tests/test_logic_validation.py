from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_master_solver_keeps_route_first_candidate_day_decision_variable() -> None:
    source = (ROOT / "src" / "pvrp_master_solver.py").read_text(encoding="utf-8")

    assert "z: dict[tuple[str, int]" in source
    assert 'model.NewBoolVar(f"z_' in source
    assert "selected_candidates" in source
    assert "client_ids" in source
    assert 'model.NewBoolVar(f"x_' not in source
