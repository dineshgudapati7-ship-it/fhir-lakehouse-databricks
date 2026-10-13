"""Load pipeline config from conf/pipeline_config.yaml and apply runtime overrides."""
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "conf" / "pipeline_config.yaml"

SUPPORTED_FORMATS = ("json", "xml")
REQUIRED_API_KEYS = ("base_url", "page_size", "max_pages_per_window", "timeout_seconds",
                     "max_retries", "backoff_factor", "default_format")


def load_config(path=None, catalog=None):
    """Return the config dict. `catalog` (e.g. a job parameter) overrides storage.catalog."""
    with open(path or DEFAULT_CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    missing = [k for k in REQUIRED_API_KEYS if k not in cfg.get("api", {})]
    if missing:
        raise ValueError(f"pipeline config is missing api keys: {missing}")
    if not cfg.get("resources"):
        raise ValueError("pipeline config has no resources")

    storage = cfg["storage"]
    if catalog:
        storage["catalog"] = catalog
    storage["raw_volume_path"] = storage["raw_volume_path"].format(catalog=storage["catalog"])
    return cfg


def resource_names(cfg):
    return [r["name"] for r in cfg["resources"]]


def get_resource(cfg, name):
    for r in cfg["resources"]:
        if r["name"].lower() == name.lower():
            return r
    raise ValueError(f"unknown resource '{name}', expected one of {resource_names(cfg)}")


def resolve_format(cfg, fmt=None):
    fmt = (fmt or cfg["api"]["default_format"]).lower()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"unsupported format '{fmt}', expected one of {SUPPORTED_FORMATS}")
    return fmt
