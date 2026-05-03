"""Input and output path selection widget."""

from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QPushButton,
    QWidget,
)


class FileSelector(QWidget):
    load_requested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.input_path_edit = QLineEdit()
        self.input_path_edit.setPlaceholderText("Изберете input_clients.xlsx")
        self.output_dir_edit = QLineEdit()
        self.output_dir_edit.setPlaceholderText("Изберете output папка")

        self.browse_input_button = QPushButton("Избери файл")
        self.browse_output_button = QPushButton("Избери папка")
        self.load_button = QPushButton("Зареди данни")
        self.load_button.setMinimumHeight(34)

        group = QGroupBox("Входни данни")
        grid = QGridLayout(group)
        grid.addWidget(QLabel("Excel файл:"), 0, 0)
        grid.addWidget(self.input_path_edit, 0, 1)
        grid.addWidget(self.browse_input_button, 0, 2)
        grid.addWidget(QLabel("Output папка:"), 1, 0)
        grid.addWidget(self.output_dir_edit, 1, 1)
        grid.addWidget(self.browse_output_button, 1, 2)
        grid.addWidget(self.load_button, 2, 2)
        grid.setColumnStretch(1, 1)

        layout = QGridLayout(self)
        layout.addWidget(group, 0, 0)

        self.browse_input_button.clicked.connect(self._browse_input)
        self.browse_output_button.clicked.connect(self._browse_output)
        self.load_button.clicked.connect(self.load_requested.emit)

    def input_path(self) -> str:
        return self.input_path_edit.text().strip()

    def output_dir(self) -> str:
        return self.output_dir_edit.text().strip()

    def set_input_path(self, path: str | Path) -> None:
        self.input_path_edit.setText(str(path))

    def set_output_dir(self, path: str | Path) -> None:
        self.output_dir_edit.setText(str(path))

    def set_controls_enabled(self, enabled: bool) -> None:
        self.input_path_edit.setEnabled(enabled)
        self.output_dir_edit.setEnabled(enabled)
        self.browse_input_button.setEnabled(enabled)
        self.browse_output_button.setEnabled(enabled)
        self.load_button.setEnabled(enabled)

    def _browse_input(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Изберете входен файл",
            self.input_path(),
            "Excel/CSV files (*.xlsx *.xlsm *.xls *.csv);;All files (*.*)",
        )
        if path:
            self.input_path_edit.setText(path)

    def _browse_output(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Изберете output папка", self.output_dir())
        if path:
            self.output_dir_edit.setText(path)
