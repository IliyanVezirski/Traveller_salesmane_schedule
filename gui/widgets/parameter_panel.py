"""Editable runtime-configuration panel."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QDoubleSpinBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)


def _get(config: dict[str, Any], path: tuple[str, ...], default: Any) -> Any:
    current: Any = config
    for part in path:
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def _ensure(config: dict[str, Any], path: tuple[str, ...]) -> dict[str, Any]:
    current = config
    for part in path:
        current = current.setdefault(part, {})
    return current


class ParameterPanel(QWidget):
    load_config_requested = Signal()
    save_config_requested = Signal()
    reset_requested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._config: dict[str, Any] = {}

        self.weeks = self._spin(1, 12)
        self.weekdays = QLineEdit("Monday, Tuesday, Wednesday, Thursday, Friday")

        self.target_clients = self._spin(1, 200)
        self.min_clients = self._spin(1, 200)
        self.max_clients = self._spin(1, 200)

        self.candidates_per_rep = self._spin(1, 500_000)
        self.random_seed = self._spin(0, 999_999)
        self.keep_top_n_per_rep = self._spin(1, 500_000)
        self.remove_duplicates = QCheckBox("Премахвай duplicate candidate routes")

        self.use_osrm = QCheckBox("Използвай OSRM")
        self.osrm_url = QLineEdit()
        self.fallback_to_haversine = QCheckBox("Fallback към haversine")
        self.use_cache = QCheckBox("Използвай cache")

        self.time_limit_seconds = self._spin(1, 86_400)
        self.num_workers = self._spin(1, 128)
        self.log_search_progress = QCheckBox("Solver log search progress")

        self.weight_route_km = self._double_spin(0, 1_000_000)
        self.weight_underfilled = self._double_spin(0, 1_000_000)
        self.weight_over_target = self._double_spin(0, 1_000_000)
        self.weight_bad_spacing_8 = self._double_spin(0, 1_000_000)
        self.weight_bad_spacing_2 = self._double_spin(0, 1_000_000)
        self.weight_cluster_mixing = self._double_spin(0, 1_000_000)

        self.load_config_button = QPushButton("Зареди config")
        self.save_config_button = QPushButton("Запази config")
        self.reset_button = QPushButton("Reset defaults")

        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.addWidget(self._calendar_group())
        content_layout.addWidget(self._daily_route_group())
        content_layout.addWidget(self._candidate_group())
        content_layout.addWidget(self._osrm_group())
        content_layout.addWidget(self._optimization_group())
        content_layout.addWidget(self._weights_group())

        buttons = QHBoxLayout()
        buttons.addStretch(1)
        buttons.addWidget(self.load_config_button)
        buttons.addWidget(self.save_config_button)
        buttons.addWidget(self.reset_button)
        content_layout.addLayout(buttons)
        content_layout.addStretch(1)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(content)
        layout = QVBoxLayout(self)
        layout.addWidget(scroll)

        self.load_config_button.clicked.connect(self.load_config_requested.emit)
        self.save_config_button.clicked.connect(self.save_config_requested.emit)
        self.reset_button.clicked.connect(self.reset_requested.emit)

    def set_config(self, config: dict[str, Any]) -> None:
        self._config = deepcopy(config)
        self.weeks.setValue(int(_get(config, ("working_days", "weeks"), 4)))
        self.weekdays.setText(", ".join(map(str, _get(config, ("working_days", "weekdays"), ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]))))

        self.target_clients.setValue(int(_get(config, ("daily_route", "target_clients"), 20)))
        self.min_clients.setValue(int(_get(config, ("daily_route", "min_clients"), 17)))
        self.max_clients.setValue(int(_get(config, ("daily_route", "max_clients"), 22)))

        self.candidates_per_rep.setValue(int(_get(config, ("candidate_routes", "candidates_per_rep"), 3000)))
        self.random_seed.setValue(int(_get(config, ("candidate_routes", "random_seed"), 42)))
        self.keep_top_n_per_rep.setValue(int(_get(config, ("candidate_routes", "keep_top_n_per_rep"), 3000)))
        self.remove_duplicates.setChecked(bool(_get(config, ("candidate_routes", "remove_duplicates"), True)))

        self.use_osrm.setChecked(bool(_get(config, ("osrm", "use_osrm"), True)))
        self.osrm_url.setText(str(_get(config, ("osrm", "url"), "http://localhost:5000")))
        self.fallback_to_haversine.setChecked(bool(_get(config, ("osrm", "fallback_to_haversine"), True)))
        self.use_cache.setChecked(bool(_get(config, ("osrm", "use_cache"), True)))

        self.time_limit_seconds.setValue(int(_get(config, ("optimization", "time_limit_seconds"), 3600)))
        self.num_workers.setValue(int(_get(config, ("optimization", "num_workers"), 8)))
        self.log_search_progress.setChecked(bool(_get(config, ("optimization", "log_search_progress"), True)))

        self.weight_route_km.setValue(float(_get(config, ("weights", "route_km"), 1000)))
        self.weight_underfilled.setValue(float(_get(config, ("weights", "underfilled_route"), 500)))
        self.weight_over_target.setValue(float(_get(config, ("weights", "over_target_clients"), 300)))
        self.weight_bad_spacing_8.setValue(float(_get(config, ("weights", "bad_spacing_frequency_8"), 2000)))
        self.weight_bad_spacing_2.setValue(float(_get(config, ("weights", "bad_spacing_frequency_2"), 2000)))
        self.weight_cluster_mixing.setValue(float(_get(config, ("weights", "cluster_mixing"), 300)))

    def config_from_ui(self) -> dict[str, Any]:
        config = deepcopy(self._config)
        working_days = _ensure(config, ("working_days",))
        working_days["weeks"] = self.weeks.value()
        working_days["weekdays"] = [part.strip() for part in self.weekdays.text().split(",") if part.strip()]

        daily = _ensure(config, ("daily_route",))
        daily["target_clients"] = self.target_clients.value()
        daily["min_clients"] = self.min_clients.value()
        daily["max_clients"] = self.max_clients.value()

        candidate = _ensure(config, ("candidate_routes",))
        candidate["candidates_per_rep"] = self.candidates_per_rep.value()
        candidate["random_seed"] = self.random_seed.value()
        candidate["keep_top_n_per_rep"] = self.keep_top_n_per_rep.value()
        candidate["remove_duplicates"] = self.remove_duplicates.isChecked()

        osrm = _ensure(config, ("osrm",))
        osrm["use_osrm"] = self.use_osrm.isChecked()
        osrm["url"] = self.osrm_url.text().strip()
        osrm["fallback_to_haversine"] = self.fallback_to_haversine.isChecked()
        osrm["use_cache"] = self.use_cache.isChecked()

        optimization = _ensure(config, ("optimization",))
        optimization["time_limit_seconds"] = self.time_limit_seconds.value()
        optimization["num_workers"] = self.num_workers.value()
        optimization["log_search_progress"] = self.log_search_progress.isChecked()

        weights = _ensure(config, ("weights",))
        weights["route_km"] = self.weight_route_km.value()
        weights["underfilled_route"] = self.weight_underfilled.value()
        weights["over_target_clients"] = self.weight_over_target.value()
        weights["bad_spacing_frequency_8"] = self.weight_bad_spacing_8.value()
        weights["bad_spacing_frequency_2"] = self.weight_bad_spacing_2.value()
        weights["cluster_mixing"] = self.weight_cluster_mixing.value()
        return config

    def set_controls_enabled(self, enabled: bool) -> None:
        for child in self.findChildren(QWidget):
            child.setEnabled(enabled)

    def _calendar_group(self) -> QGroupBox:
        group = QGroupBox("Календар")
        form = QFormLayout(group)
        form.addRow("Седмици:", self.weeks)
        form.addRow("Работни дни:", self.weekdays)
        form.addRow(QLabel("Пример: Monday, Tuesday, Wednesday, Thursday, Friday"))
        return group

    def _daily_route_group(self) -> QGroupBox:
        group = QGroupBox("Дневен маршрут")
        grid = QGridLayout(group)
        grid.addWidget(QLabel("target_clients"), 0, 0)
        grid.addWidget(self.target_clients, 0, 1)
        grid.addWidget(QLabel("min_clients"), 1, 0)
        grid.addWidget(self.min_clients, 1, 1)
        grid.addWidget(QLabel("max_clients"), 2, 0)
        grid.addWidget(self.max_clients, 2, 1)
        return group

    def _candidate_group(self) -> QGroupBox:
        group = QGroupBox("Candidate routes")
        form = QFormLayout(group)
        form.addRow("candidates_per_rep:", self.candidates_per_rep)
        form.addRow("random_seed:", self.random_seed)
        form.addRow("keep_top_n_per_rep:", self.keep_top_n_per_rep)
        form.addRow("", self.remove_duplicates)
        return group

    def _osrm_group(self) -> QGroupBox:
        group = QGroupBox("OSRM")
        form = QFormLayout(group)
        form.addRow("", self.use_osrm)
        form.addRow("OSRM URL:", self.osrm_url)
        form.addRow("", self.fallback_to_haversine)
        form.addRow("", self.use_cache)
        return group

    def _optimization_group(self) -> QGroupBox:
        group = QGroupBox("Оптимизация")
        form = QFormLayout(group)
        form.addRow("time_limit_seconds:", self.time_limit_seconds)
        form.addRow("num_workers:", self.num_workers)
        form.addRow("", self.log_search_progress)
        return group

    def _weights_group(self) -> QGroupBox:
        group = QGroupBox("Weights")
        form = QFormLayout(group)
        form.addRow("route_km:", self.weight_route_km)
        form.addRow("underfilled_route:", self.weight_underfilled)
        form.addRow("over_target_clients:", self.weight_over_target)
        form.addRow("bad_spacing_frequency_8:", self.weight_bad_spacing_8)
        form.addRow("bad_spacing_frequency_2:", self.weight_bad_spacing_2)
        form.addRow("cluster_mixing:", self.weight_cluster_mixing)
        return group

    @staticmethod
    def _spin(minimum: int, maximum: int) -> QSpinBox:
        spin = QSpinBox()
        spin.setRange(minimum, maximum)
        return spin

    @staticmethod
    def _double_spin(minimum: float, maximum: float) -> QDoubleSpinBox:
        spin = QDoubleSpinBox()
        spin.setRange(minimum, maximum)
        spin.setDecimals(0)
        spin.setSingleStep(100)
        return spin
