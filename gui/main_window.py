"""Main PySide6 window for the sales PVRP scheduler."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import os
import re
import subprocess
import sys

import pandas as pd
from PySide6.QtCore import QSize, Qt, QThread
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from src.app_paths import get_data_dir, get_resource_path
from src.data_loader import load_clients
from src.osrm_status import check_osrm_status
from src.validation import validate_clients
from src.version import APP_NAME, APP_VERSION

from . import settings_manager
from .validators import format_missing_column_message, validate_config_values, validate_excel_file, validate_output_dir
from .widgets.file_selector import FileSelector
from .widgets.log_panel import LogPanel
from .widgets.parameter_panel import ParameterPanel
from .widgets.progress_panel import ProgressPanel
from .widgets.results_panel import ResultsPanel
from .worker import OptimizationWorker


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} v{APP_VERSION}")
        self.setMinimumSize(1100, 720)

        self.root = settings_manager.project_root()
        icon_path = get_resource_path("gui/resources/app_icon.png")
        if icon_path.exists():
            self.setWindowIcon(QIcon(str(icon_path)))
        self.settings = settings_manager.load_recent_settings()
        self.config_path = Path(self.settings.get("last_config_path") or settings_manager.default_config_path())
        self.config = settings_manager.load_config(self.config_path)
        self.clients_raw: pd.DataFrame | None = None
        self.clients_clean: pd.DataFrame | None = None
        self.validation_df: pd.DataFrame = pd.DataFrame()
        self.last_result: dict[str, Any] | None = None
        self.worker_thread: QThread | None = None
        self.worker: OptimizationWorker | None = None

        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        self.file_selector = FileSelector()
        self.parameter_panel = ParameterPanel()
        self.parameter_panel.set_config(self.config)
        self.progress_panel = ProgressPanel()
        self.results_panel = ResultsPanel()
        self.log_panel = LogPanel()
        self.run_log = QTextEdit()
        self.run_log.setReadOnly(True)
        self.run_log.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)

        self._build_input_tab()
        self._build_parameters_tab()
        self._build_validation_tab()
        self._build_run_tab()
        self._build_results_tab()
        self._build_logs_tab()
        self._connect_signals()
        self._restore_recent_state()
        self._apply_style()
        self._set_run_allowed(False)

    def _build_input_tab(self) -> None:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.addWidget(self.file_selector)

        info_group = QGroupBox("Информация за файла")
        info_layout = QGridLayout(info_group)
        self.info_labels: dict[str, QLabel] = {}
        info_items = [
            ("rows", "Брой редове"),
            ("clients", "Брой клиенти"),
            ("sales_reps", "Брой търговци"),
            ("frequency", "Visit frequency 2 / 4 / 8"),
            ("sales_rep_list", "Търговци"),
        ]
        for row, (key, label) in enumerate(info_items):
            value = QLabel("-")
            value.setWordWrap(True)
            self.info_labels[key] = value
            info_layout.addWidget(QLabel(label + ":"), row, 0)
            info_layout.addWidget(value, row, 1)
        info_layout.setColumnStretch(1, 1)

        self.preview_table = QTableWidget()
        self.preview_table.setAlternatingRowColors(True)
        self.preview_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)

        layout.addWidget(info_group)
        layout.addWidget(QLabel("Preview: първите 50 реда"))
        layout.addWidget(self.preview_table, 1)
        self.tabs.addTab(tab, "Input")

    def _build_parameters_tab(self) -> None:
        self.tabs.addTab(self.parameter_panel, "Parameters")

    def _build_validation_tab(self) -> None:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        top = QHBoxLayout()
        self.validate_button = QPushButton("Валидирай")
        self.validate_button.setMinimumHeight(36)
        self.validation_status_label = QLabel("Заредете файл и стартирайте валидация.")
        top.addWidget(self.validate_button)
        top.addWidget(self.validation_status_label, 1)

        summary_group = QGroupBox("Validation summary")
        summary_layout = QGridLayout(summary_group)
        self.validation_summary_labels: dict[str, QLabel] = {}
        summary_items = [
            ("errors", "Errors"),
            ("warnings", "Warnings"),
            ("valid_clients", "Valid clients"),
            ("sales_reps", "Sales reps"),
            ("capacity_warnings", "Capacity warnings"),
        ]
        for col, (key, label) in enumerate(summary_items):
            title = QLabel(label)
            title.setAlignment(Qt.AlignmentFlag.AlignCenter)
            value = QLabel("0")
            value.setAlignment(Qt.AlignmentFlag.AlignCenter)
            value.setObjectName("metricValue")
            self.validation_summary_labels[key] = value
            summary_layout.addWidget(title, 0, col)
            summary_layout.addWidget(value, 1, col)

        self.validation_table = QTableWidget()
        self.validation_table.setAlternatingRowColors(True)
        self.validation_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)

        layout.addLayout(top)
        layout.addWidget(summary_group)
        layout.addWidget(self.validation_table, 1)
        self.tabs.addTab(tab, "Validation")

    def _build_run_tab(self) -> None:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.addWidget(self.progress_panel)
        layout.addWidget(QLabel("Лог в реално време"))
        layout.addWidget(self.run_log, 1)
        self.tabs.addTab(tab, "Run")

    def _build_results_tab(self) -> None:
        self.tabs.addTab(self.results_panel, "Results")

    def _build_logs_tab(self) -> None:
        self.tabs.addTab(self.log_panel, "Logs")

    def _connect_signals(self) -> None:
        self.file_selector.load_requested.connect(self.load_input_file)
        self.validate_button.clicked.connect(self.validate_input)
        self.progress_panel.start_requested.connect(self.start_optimization)
        self.progress_panel.cancel_requested.connect(self.cancel_optimization)
        self.parameter_panel.load_config_requested.connect(self.load_config_dialog)
        self.parameter_panel.save_config_requested.connect(self.save_config_dialog)
        self.parameter_panel.reset_requested.connect(self.reset_config)
        self.results_panel.open_excel_requested.connect(self.open_excel)
        self.results_panel.open_map_requested.connect(self.open_map)
        self.results_panel.open_output_requested.connect(self.open_output_folder)
        self.results_panel.export_logs_requested.connect(self.log_panel.save_log)

    def _restore_recent_state(self) -> None:
        default_input = get_data_dir() / "input_clients.xlsx"
        if not default_input.exists():
            default_input = get_data_dir() / "sample_clients.xlsx"
        self.file_selector.set_input_path(self.settings.get("last_input_file") or default_input)
        self.file_selector.set_output_dir(self.settings.get("last_output_folder") or settings_manager.default_output_dir())
        size = self.settings.get("window_size")
        if isinstance(size, list) and len(size) == 2:
            self.resize(QSize(int(size[0]), int(size[1])))

    def _apply_style(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow { background: #f6f7f9; }
            QTabWidget::pane { border: 1px solid #cfd4dc; background: white; }
            QGroupBox {
                font-weight: 600;
                border: 1px solid #d5dae2;
                border-radius: 6px;
                margin-top: 10px;
                padding-top: 12px;
                background: #ffffff;
            }
            QPushButton {
                padding: 7px 12px;
                border-radius: 4px;
                border: 1px solid #aab2bd;
                background: #ffffff;
            }
            QPushButton:hover { background: #eef4ff; }
            QPushButton:disabled { color: #8c96a3; background: #eceff3; }
            QLineEdit, QSpinBox, QDoubleSpinBox {
                padding: 4px;
                border: 1px solid #b8c0cc;
                border-radius: 4px;
                background: white;
            }
            QTableWidget { background: white; gridline-color: #e1e5eb; }
            QLabel#metricValue { font-size: 20px; font-weight: 700; color: #1f4e79; }
            """
        )

    def load_input_file(self) -> None:
        path = self.file_selector.input_path()
        errors = validate_excel_file(path)
        if errors:
            self._show_error("Грешка при входния файл", "\n".join(errors))
            return
        try:
            df = load_clients(path)
        except ValueError as exc:
            self._show_error("Грешка при входния файл", self._friendly_load_error(str(exc)))
            return
        except Exception as exc:
            self._show_error("Грешка при зареждане", str(exc))
            return

        self.clients_raw = df
        self.clients_clean = None
        self.validation_df = pd.DataFrame()
        self.last_result = None
        self.results_panel.clear()
        self._set_run_allowed(False)
        self._update_input_info(df)
        self._populate_table(self.preview_table, df.head(50))
        self._append_log(f"Зареден файл: {path}")
        self._append_log(f"Редове: {len(df)}, клиенти: {df['client_id'].nunique()}, търговци: {df['sales_rep'].nunique()}")
        self.tabs.setCurrentIndex(2)

    def validate_input(self) -> bool:
        if self.clients_raw is None:
            self._show_error("Няма данни", "Първо заредете входен файл.")
            return False
        config = self.parameter_panel.config_from_ui()
        config_errors = validate_config_values(config)
        if config_errors:
            self._show_error("Грешни параметри", "\n".join(config_errors))
            self._set_run_allowed(False)
            return False

        try:
            clean_df, validation_df = validate_clients(self.clients_raw, config)
        except Exception as exc:
            self._show_error("Грешка при валидация", str(exc))
            self._set_run_allowed(False)
            return False

        self.clients_clean = clean_df
        self.validation_df = validation_df
        self._populate_table(self.validation_table, validation_df)
        errors = int(validation_df["severity"].eq("ERROR").sum()) if not validation_df.empty else 0
        warnings = int(validation_df["severity"].eq("WARNING").sum()) if not validation_df.empty else 0
        capacity_warnings = (
            int(validation_df["field"].astype(str).eq("capacity").sum())
            if not validation_df.empty and "field" in validation_df.columns
            else 0
        )
        self.validation_summary_labels["errors"].setText(str(errors))
        self.validation_summary_labels["warnings"].setText(str(warnings))
        self.validation_summary_labels["valid_clients"].setText(str(len(clean_df)))
        self.validation_summary_labels["sales_reps"].setText(str(clean_df["sales_rep"].nunique() if not clean_df.empty else 0))
        self.validation_summary_labels["capacity_warnings"].setText(str(capacity_warnings))

        if errors:
            self.validation_status_label.setText("Има критични грешки във входните данни. Коригирайте ги преди оптимизация.")
            self._set_run_allowed(False)
            self._append_log(f"Validation: {errors} errors, {warnings} warnings")
            return False

        status = "Валидацията е успешна." if warnings == 0 else "Има предупреждения, но оптимизацията може да стартира."
        self.validation_status_label.setText(status)
        self._set_run_allowed(True)
        self._append_log(f"Validation: {errors} errors, {warnings} warnings")
        return True

    def start_optimization(self) -> None:
        if self.clients_raw is None:
            self._show_error("Няма данни", "Първо заредете входен файл.")
            return
        if self.validation_df.empty:
            if not self.validate_input():
                return
        elif self.validation_df["severity"].eq("ERROR").any():
            self._show_error(
                "Validation errors",
                "Има критични грешки във входните данни. Коригирайте ги преди оптимизация.",
            )
            return

        config = self.parameter_panel.config_from_ui()
        errors = validate_config_values(config) + validate_output_dir(self.file_selector.output_dir())
        if errors:
            self._show_error("Грешни настройки", "\n".join(errors))
            return

        warnings = int(self.validation_df["severity"].eq("WARNING").sum()) if not self.validation_df.empty else 0
        if warnings:
            QMessageBox.warning(self, "Предупреждения", "Има warnings във входните данни. Оптимизацията ще продължи.")

        self.config = config
        osrm_config = self.config.setdefault("osrm", {})
        if bool(osrm_config.get("use_osrm", True)):
            status = check_osrm_status(str(osrm_config.get("url", "")))
            if not status["available"] and bool(osrm_config.get("fallback_to_haversine", True)):
                self.config["osrm"]["use_osrm"] = False
                message = f"{status['message']} Ще се използва haversine fallback."
                self._append_log("WARNING: " + message)
                QMessageBox.warning(self, "OSRM не е наличен", message)
            elif not status["available"]:
                self._append_log("WARNING: " + status["message"])

        self.last_result = None
        self.results_panel.clear()
        self.tabs.setCurrentIndex(3)
        self._append_log("=== Start optimization ===")
        self._append_log(f"Input file: {self.file_selector.input_path()}")
        self._append_log(f"Config: {self.config_path}")
        self._append_log(f"Output folder: {self.file_selector.output_dir()}")
        self._set_running_state(True)

        self.worker_thread = QThread(self)
        self.worker = OptimizationWorker(self.file_selector.input_path(), config, self.file_selector.output_dir())
        self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)
        self.worker.progress_changed.connect(self._on_worker_progress)
        self.worker.log_message.connect(self._append_log)
        self.worker.finished.connect(self._on_worker_finished)
        self.worker.failed.connect(self._on_worker_failed)
        self.worker.cancelled.connect(self._on_worker_cancelled)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.failed.connect(self.worker_thread.quit)
        self.worker.cancelled.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker_thread.finished.connect(self._clear_worker_refs)
        self.worker_thread.start()

    def cancel_optimization(self) -> None:
        if self.worker:
            self.progress_panel.set_progress(self.progress_panel.progress_bar.value(), "Отказване...")
            self.worker.cancel()

    def load_config_dialog(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Зареди config.yaml", str(self.config_path), "YAML files (*.yaml *.yml);;All files (*.*)")
        if not path:
            return
        try:
            self.config = settings_manager.load_config(path)
            self.config_path = Path(path)
            self.parameter_panel.set_config(self.config)
            self._append_log(f"Зареден config: {path}")
            self._set_run_allowed(False)
        except Exception as exc:
            self._show_error("Грешка при config", str(exc))

    def save_config_dialog(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Запази config", str(self.config_path), "YAML files (*.yaml *.yml);;All files (*.*)")
        if not path:
            return
        try:
            self.config = self.parameter_panel.config_from_ui()
            settings_manager.save_config(self.config, path)
            self.config_path = Path(path)
            self._append_log(f"Запазен config: {path}")
        except Exception as exc:
            self._show_error("Грешка при запис", str(exc))

    def reset_config(self) -> None:
        try:
            self.config_path = settings_manager.default_config_path()
            self.config = settings_manager.load_config(self.config_path)
            self.parameter_panel.set_config(self.config)
            self._set_run_allowed(False)
            self._append_log("Config reset към default config.yaml")
        except Exception as exc:
            self._show_error("Грешка при reset", str(exc))

    def _on_worker_progress(self, percent: int, message: str) -> None:
        self.progress_panel.set_progress(percent, message)

    def _on_worker_finished(self, result: dict[str, Any]) -> None:
        self.last_result = result
        self.results_panel.set_result(result)
        status = result.get("status")
        if status == "success":
            self.progress_panel.set_progress(100, "Завършено успешно")
            self.progress_panel.finish("Завършено успешно")
            self._append_log("Optimization finished successfully.")
            self.tabs.setCurrentIndex(4)
        elif status == "infeasible":
            self.progress_panel.finish("Няма feasible решение")
            self._append_log("No feasible solution.")
            QMessageBox.warning(self, "Няма feasible решение", "Оптимизаторът не намери feasible решение. Вижте Logs за диагностика.")
            self.tabs.setCurrentIndex(5)
        else:
            self.progress_panel.finish("Грешка")
            self._append_log(str(result.get("message", "Optimization failed.")))
            QMessageBox.critical(self, "Грешка", str(result.get("message", "Вижте Logs за детайли.")))
            self.tabs.setCurrentIndex(5)
        self._set_running_state(False)

    def _on_worker_failed(self, message: str, trace: str) -> None:
        self._append_log("ERROR: " + message)
        self._append_log(trace)
        self.progress_panel.finish("Грешка")
        self._set_running_state(False)
        user_message = message
        if "Permission denied" in trace or "PermissionError" in trace:
            user_message = "Файлът final_schedule.xlsx вероятно е отворен. Затворете го и опитайте отново."
        QMessageBox.critical(self, "Грешка", user_message + "\n\nВижте Logs за детайли.")
        self.tabs.setCurrentIndex(5)

    def _on_worker_cancelled(self) -> None:
        self._append_log("Optimization cancelled.")
        self.progress_panel.finish("Отказано")
        self._set_running_state(False)

    def _clear_worker_refs(self) -> None:
        self.worker = None
        self.worker_thread = None

    def open_excel(self) -> None:
        if self.last_result:
            self._open_path(self.last_result.get("excel_path"))

    def open_map(self) -> None:
        if self.last_result:
            self._open_path(self.last_result.get("map_path"))

    def open_output_folder(self) -> None:
        if self.last_result and self.last_result.get("excel_path"):
            self._open_path(str(Path(self.last_result["excel_path"]).parent))
        else:
            self._open_path(self.file_selector.output_dir())

    def _open_path(self, path_value: Any) -> None:
        if not path_value:
            return
        path = Path(str(path_value))
        if not path.exists():
            self._show_error("Файлът липсва", f"Не е намерен: {path}")
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.call(["open", str(path)])
            else:
                subprocess.call(["xdg-open", str(path)])
        except Exception as exc:
            self._show_error("Грешка при отваряне", str(exc))

    def _set_running_state(self, running: bool) -> None:
        if running:
            self.progress_panel.start()
        else:
            self.progress_panel.set_running_enabled(False)
        self.file_selector.set_controls_enabled(not running)
        self.parameter_panel.set_controls_enabled(not running)
        self.validate_button.setEnabled(not running)
        if not running:
            has_errors = not self.validation_df.empty and self.validation_df["severity"].eq("ERROR").any()
            self._set_run_allowed(self.clients_raw is not None and not has_errors)

    def _set_run_allowed(self, allowed: bool) -> None:
        self.progress_panel.start_button.setEnabled(bool(allowed))

    def _update_input_info(self, df: pd.DataFrame) -> None:
        freq_counts = df["visit_frequency"].value_counts(dropna=False).to_dict()
        freq_text = " / ".join(f"{freq}: {freq_counts.get(freq, 0)}" for freq in [2, 4, 8])
        reps = sorted(str(rep) for rep in df["sales_rep"].dropna().unique())
        self.info_labels["rows"].setText(str(len(df)))
        self.info_labels["clients"].setText(str(df["client_id"].nunique()))
        self.info_labels["sales_reps"].setText(str(len(reps)))
        self.info_labels["frequency"].setText(freq_text)
        self.info_labels["sales_rep_list"].setText(", ".join(reps) if reps else "-")

    def _populate_table(self, table: QTableWidget, data: pd.DataFrame) -> None:
        df = data.copy().fillna("")
        table.clear()
        table.setRowCount(len(df))
        table.setColumnCount(len(df.columns))
        table.setHorizontalHeaderLabels([str(c) for c in df.columns])
        for row_idx, row in enumerate(df.itertuples(index=False)):
            for col_idx, value in enumerate(row):
                table.setItem(row_idx, col_idx, QTableWidgetItem(str(value)))
        table.resizeColumnsToContents()

    def _friendly_load_error(self, message: str) -> str:
        match = re.search(r"Missing required columns:\s*(.*)", message)
        if not match:
            return message
        columns = [part.strip() for part in match.group(1).split(",") if part.strip()]
        return "\n".join(format_missing_column_message(column) for column in columns)

    def _append_log(self, message: str) -> None:
        self.log_panel.append_message(message)
        self.run_log.append(message)

    def _show_error(self, title: str, message: str) -> None:
        QMessageBox.critical(self, title, message)
        self._append_log(f"{title}: {message}")

    def closeEvent(self, event: Any) -> None:
        settings_manager.save_recent_settings(
            {
                "last_input_file": self.file_selector.input_path(),
                "last_output_folder": self.file_selector.output_dir(),
                "last_config_path": str(self.config_path),
                "last_osrm_url": self.parameter_panel.osrm_url.text().strip(),
                "window_size": [self.size().width(), self.size().height()],
            }
        )
        super().closeEvent(event)
