from src.calendar_builder import build_calendar


def test_calendar_has_20_working_days() -> None:
    config = {
        "working_days": {
            "weeks": 4,
            "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        }
    }

    calendar = build_calendar(config)

    assert len(calendar) == 20
    assert calendar["week_index"].nunique() == 4
    assert calendar["weekday"].tolist()[:5] == config["working_days"]["weekdays"]
