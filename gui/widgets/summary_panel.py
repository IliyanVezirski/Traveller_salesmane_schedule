"""Result summary widgets."""

from __future__ import annotations

from typing import Any

import pandas as pd
from PySide6.QtWidgets import QFormLayout, QGroupBox, QLabel, QTableWidget, QTableWidgetItem, QTabWidget, QVBoxLayout, QWidget


class SummaryPanel(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.metric_labels: dict[str, QLabel] = {}
        metrics = [
            ("solver_status", "Solver status"),
            ("total_route_km", "Общо километри"),
            ("planned_visits", "Планирани посещения"),
            ("number_of_sales_reps", "Брой търговци"),
            ("avg_clients_per_route", "Средно клиенти/маршрут"),
            ("min_clients_per_route", "Мин. клиенти/маршрут"),
            ("max_clients_per_route", "Макс. клиенти/маршрут"),
            ("validation_errors", "Validation errors"),
            ("excel_path", "Excel файл"),
            ("map_path", "HTML карта"),
        ]

        group = QGroupBox("Обобщение")
        form = QFormLayout(group)
        for key, label in metrics:
            value = QLabel("-")
            value.setTextInteractionFlags(value.textInteractionFlags())
            value.setWordWrap(True)
            self.metric_labels[key] = value
            form.addRow(label + ":", value)

        self.rep_table = QTableWidget()
        self.day_table = QTableWidget()
        tabs = QTabWidget()
        tabs.addTab(self.rep_table, "По търговец")
        tabs.addTab(self.day_table, "По ден")

        layout = QVBoxLayout(self)
        layout.addWidget(group)
        layout.addWidget(tabs, 1)

    def clear(self) -> None:
        for label in self.metric_labels.values():
            label.setText("-")
        self.rep_table.clear()
        self.rep_table.setRowCount(0)
        self.rep_table.setColumnCount(0)
        self.day_table.clear()
        self.day_table.setRowCount(0)
        self.day_table.setColumnCount(0)

    def set_result(self, result: dict[str, Any]) -> None:
        for key, label in self.metric_labels.items():
            value = result.get(key, "-")
            if isinstance(value, float):
                value = f"{value:,.2f}"
            label.setText(str(value))
        self._populate_table(self.rep_table, result.get("summary_by_sales_rep"))
        self._populate_table(self.day_table, result.get("summary_by_day"))

    @staticmethod
    def _populate_table(table: QTableWidget, data: Any) -> None:
        if data is None:
            df = pd.DataFrame()
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            df = pd.DataFrame(data)
        df = df.fillna("")
        table.clear()
        table.setRowCount(len(df))
        table.setColumnCount(len(df.columns))
        table.setHorizontalHeaderLabels([str(c) for c in df.columns])
        for row_idx, row in enumerate(df.itertuples(index=False)):
            for col_idx, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                table.setItem(row_idx, col_idx, item)
        table.resizeColumnsToContents()
