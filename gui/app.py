"""Application entry point for the PySide6 GUI."""

from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from src.version import APP_NAME

from .main_window import MainWindow


def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setOrganizationName("Sales PVRP")
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
