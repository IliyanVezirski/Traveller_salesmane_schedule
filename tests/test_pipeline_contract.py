from src.pipeline import run_pipeline


REQUIRED_KEYS = {
    "status",
    "excel_path",
    "map_path",
    "summary_by_sales_rep",
    "summary_by_day",
    "validation",
    "total_route_km",
    "message",
}


def test_run_pipeline_returns_contract_dict_for_failed_input(tmp_path) -> None:
    result = run_pipeline(
        input_path=str(tmp_path / "missing.xlsx"),
        config={},
        output_dir=str(tmp_path / "output"),
    )

    assert REQUIRED_KEYS.issubset(result)
    assert result["status"] == "failed"
    assert isinstance(result["summary_by_sales_rep"], list)
    assert isinstance(result["summary_by_day"], list)
    assert isinstance(result["validation"], list)
    assert result["excel_path"] is None
    assert result["map_path"] is None
