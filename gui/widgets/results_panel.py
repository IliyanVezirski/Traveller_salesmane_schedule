"""Results display and output file actions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QHBoxLayout, QPushButton, QVBoxLayout, QWidget

from .summary_panel import SummaryPanel


class ResultsPanel(QWidget):
    open_excel_requested = Signal()
    open_map_requested = Signal()
    open_output_requested = Signal()
    export_logs_requested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.summary = SummaryPanel()

        self.open_excel_button = QPushButton("Отвори Excel")
        self.open_map_button = QPushButton("Отвори карта")
        self.open_output_button = QPushButton("Отвори папка")
        self.export_logs_button = QPushButton("Експорт лог")

        buttons = QHBoxLayout()
        buttons.addWidget(self.open_excel_button)
        buttons.addWidget(self.open_map_button)
        buttons.addWidget(self.open_output_button)
        buttons.addStretch(1)
        buttons.addWidget(self.export_logs_button)

        layout = QVBoxLayout(self)
        layout.addWidget(self.summary, 1)
        layout.addLayout(buttons)

        self.open_excel_button.clicked.connect(self.open_excel_requested.emit)
        self.open_map_button.clicked.connect(self.open_map_requested.emit)
        self.open_output_button.clicked.connect(self.open_output_requested.emit)
        self.export_logs_button.clicked.connect(self.export_logs_requested.emit)
        self.clear()

    def clear(self) -> None:
        self.summary.clear()
        self.open_excel_button.setEnabled(False)
        self.open_map_button.setEnabled(False)
        self.open_output_button.setEnabled(False)

    def set_result(self, result: dict[str, Any]) -> None:
        self.summary.set_result(result)
        excel = Path(str(result.get("excel_path", "")))
        map_path = Path(str(result.get("map_path", "")))
        self.open_excel_button.setEnabled(result.get("status") == "success" and excel.exists())
        self.open_map_button.setEnabled(result.get("status") == "success" and map_path.exists())
        self.open_output_button.setEnabled(bool(result.get("excel_path")) and excel.parent.exists())
