"""Run controls and progress display."""

from __future__ import annotations

from datetime import timedelta

from PySide6.QtCore import QElapsedTimer, QTimer, Signal
from PySide6.QtWidgets import QGroupBox, QHBoxLayout, QLabel, QProgressBar, QPushButton, QVBoxLayout, QWidget


class ProgressPanel(QWidget):
    start_requested = Signal()
    cancel_requested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.start_button = QPushButton("Стартирай оптимизация")
        self.cancel_button = QPushButton("Откажи")
        self.cancel_button.setEnabled(False)
        self.start_button.setMinimumHeight(40)
        self.cancel_button.setMinimumHeight(40)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.stage_label = QLabel("Готово")
        self.elapsed_label = QLabel("Време: 00:00:00")

        buttons = QHBoxLayout()
        buttons.addWidget(self.start_button)
        buttons.addWidget(self.cancel_button)
        buttons.addStretch(1)

        group = QGroupBox("Изпълнение")
        inner = QVBoxLayout(group)
        inner.addLayout(buttons)
        inner.addWidget(self.progress_bar)
        inner.addWidget(self.stage_label)
        inner.addWidget(self.elapsed_label)

        layout = QVBoxLayout(self)
        layout.addWidget(group)
        layout.addStretch(1)

        self._elapsed = QElapsedTimer()
        self._timer = QTimer(self)
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self._update_elapsed)

        self.start_button.clicked.connect(self.start_requested.emit)
        self.cancel_button.clicked.connect(self.cancel_requested.emit)

    def start(self) -> None:
        self.progress_bar.setValue(0)
        self.stage_label.setText("Стартиране...")
        self.start_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self._elapsed.start()
        self._timer.start()
        self._update_elapsed()

    def finish(self, status_text: str = "Готово") -> None:
        self.stage_label.setText(status_text)
        self.start_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self._timer.stop()
        self._update_elapsed()

    def set_running_enabled(self, running: bool) -> None:
        self.start_button.setEnabled(not running)
        self.cancel_button.setEnabled(running)

    def set_progress(self, percent: int, message: str) -> None:
        self.progress_bar.setValue(percent)
        self.stage_label.setText(message)

    def _update_elapsed(self) -> None:
        elapsed_ms = self._elapsed.elapsed() if self._elapsed.isValid() else 0
        text = str(timedelta(milliseconds=elapsed_ms)).split(".")[0]
        self.elapsed_label.setText(f"Време: {text}")
