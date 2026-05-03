"""File logging helpers for CLI, GUI, and packaged runs."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import logging


def setup_run_logger(output_dir: str) -> logging.Logger:
    """Create a run logger that writes to output_dir/logs/run_*.log."""
    root = Path(output_dir)
    logs_dir = root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger_name = f"sales_pvrp.run.{log_path.stem}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)

    logger.info("Log file: %s", log_path)
    return logger
