"""Read-only log panel with save and clear actions."""

from __future__ import annotations

from datetime import datetime

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QFileDialog, QHBoxLayout, QPushButton, QTextEdit, QVBoxLayout, QWidget


class LogPanel(QWidget):
    cleared = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.text = QTextEdit()
        self.text.setReadOnly(True)
        self.text.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)

        self.save_button = QPushButton("Запази лог")
        self.clear_button = QPushButton("Изчисти лог")
        buttons = QHBoxLayout()
        buttons.addStretch(1)
        buttons.addWidget(self.save_button)
        buttons.addWidget(self.clear_button)

        layout = QVBoxLayout(self)
        layout.addWidget(self.text)
        layout.addLayout(buttons)

        self.save_button.clicked.connect(self.save_log)
        self.clear_button.clicked.connect(self.clear)

    def append_message(self, message: str, timestamp: bool = True) -> None:
        prefix = datetime.now().strftime("%H:%M:%S") + "  " if timestamp else ""
        self.text.append(prefix + str(message))

    def append_block(self, message: str) -> None:
        self.text.append(str(message))

    def clear(self) -> None:
        self.text.clear()
        self.cleared.emit()

    def to_plain_text(self) -> str:
        return self.text.toPlainText()

    def save_log(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Запази лог", "pvrp_gui.log", "Log files (*.log *.txt);;All files (*.*)")
        if not path:
            return
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.to_plain_text())
