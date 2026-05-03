"""Launch the desktop GUI for the sales PVRP scheduler."""

from __future__ import annotations

import sys

from src.app_paths import ensure_runtime_dirs


def main() -> int:
    ensure_runtime_dirs()
    try:
        from gui.app import main as gui_main
    except ModuleNotFoundError as exc:
        if exc.name == "PySide6":
            print("PySide6 не е инсталиран. Изпълнете: pip install -r requirements-gui.txt")
            return 1
        raise
    return gui_main()


if __name__ == "__main__":
    raise SystemExit(main())
