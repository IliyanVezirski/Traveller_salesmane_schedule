"""Background optimization worker for the desktop GUI."""

from __future__ import annotations

import traceback
from typing import Any

from PySide6.QtCore import QObject, Signal, Slot

from src.pipeline import PipelineCancelled, run_pipeline


class OptimizationWorker(QObject):
    progress_changed = Signal(int, str)
    log_message = Signal(str)
    stage_changed = Signal(str)
    finished = Signal(object)
    failed = Signal(str, str)
    cancelled = Signal()

    def __init__(self, input_path: str, config: dict[str, Any], output_dir: str) -> None:
        super().__init__()
        self.input_path = input_path
        self.config = config
        self.output_dir = output_dir
        self._cancel_requested = False

    @Slot()
    def run(self) -> None:
        try:
            result = run_pipeline(
                input_path=self.input_path,
                config=self.config,
                output_dir=self.output_dir,
                progress_callback=self._on_progress,
                log_callback=self.log_message.emit,
                cancel_checker=self.is_cancel_requested,
            )
            if self._cancel_requested:
                self.cancelled.emit()
                return
            self.finished.emit(result)
        except PipelineCancelled:
            self.cancelled.emit()
        except Exception as exc:
            self.failed.emit(str(exc), traceback.format_exc())

    @Slot()
    def cancel(self) -> None:
        self._cancel_requested = True
        self.log_message.emit("Cancel requested. The current backend stage will finish before stopping.")

    def is_cancel_requested(self) -> bool:
        return self._cancel_requested

    def _on_progress(self, percent: int, message: str) -> None:
        self.progress_changed.emit(percent, message)
        self.stage_changed.emit(message)
