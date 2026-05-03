"""Import smoke check for the GUI entry point without starting the event loop."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    import run_gui

    if not callable(run_gui.main):
        raise AssertionError("run_gui.main is not callable.")

    import gui.app
    import gui.main_window

    print("GUI import check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
