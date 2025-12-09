"""
Configuration loader for the revenue pipeline.
"""

from pathlib import Path
from typing import Any, Dict

import yaml


def load_config(path: str) -> Dict[str, Any]:

    config_path = Path(path)

    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    _validate_config(config, config_path)

    return config


def _validate_config(config: Dict[str, Any], config_path: Path) -> None:

    if "sources" not in config:
        raise ValueError(f"'sources' section missing in config: {config_path}")

    if "sales" not in config["sources"]:
        raise ValueError(f"'sources.sales' section missing in config: {config_path}")

    if "product" not in config["sources"]:
        raise ValueError(f"'sources.product' section missing in config: {config_path}")

    if "target" not in config:
        raise ValueError(f"'target' section missing in config: {config_path}")

    if "revenue" not in config["target"]:
        raise ValueError(f"'target.revenue' section missing in config: {config_path}")

    if "table" not in config["sources"]["sales"]:
        raise ValueError(f"'sources.sales.table' missing in config: {config_path}")

    if "table" not in config["sources"]["product"]:
        raise ValueError(f"'sources.product.table' missing in config: {config_path}")

    if "table" not in config["target"]["revenue"]:
        raise ValueError(f"'target.revenue.table' missing in config: {config_path}")
