"""Explicit runtime and storage configuration; no sessions or storage access."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from src.common.paths import is_remote_path, join_path, require_separate_paths, to_spark_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LAYERS = ("raw", "bronze", "silver", "gold", "quarantine")


class RuntimeEnv(str, Enum):
    LOCAL = "LOCAL"
    DATABRICKS = "DATABRICKS"


def runtime_env(environ: Mapping[str, str] | None = None) -> RuntimeEnv:
    env = os.environ if environ is None else environ
    default = "DATABRICKS" if env.get("DATABRICKS_RUNTIME_VERSION") else "LOCAL"
    return RuntimeEnv(env.get("RUNTIME_ENV", default).strip().upper())


@dataclass(frozen=True)
class RuntimeConfig:
    runtime: RuntimeEnv
    raw: str
    bronze: str
    silver: str
    gold: str
    quarantine: str

    def __post_init__(self):
        object.__setattr__(self, "runtime", RuntimeEnv(self.runtime))
        for layer in LAYERS:
            value = to_spark_path(getattr(self, layer))
            if self.runtime == RuntimeEnv.DATABRICKS and not is_remote_path(value):
                raise ValueError("Databricks storage requires dbfs:/, abfss:// or /Volumes/")
            object.__setattr__(self, layer, value)
        require_separate_paths([getattr(self, layer) for layer in LAYERS])

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "RuntimeConfig":
        env = os.environ if environ is None else environ
        runtime = runtime_env(env)
        base = env.get("DATA_BASE_PATH")
        if runtime == RuntimeEnv.DATABRICKS:
            base = env.get("DATABRICKS_BASE_PATH") or base
            if not base and not all(env.get(f"{layer.upper()}_PATH") for layer in LAYERS):
                raise ValueError("Set DATABRICKS_BASE_PATH, DATA_BASE_PATH or all layer paths")
        else:
            base = base or str(PROJECT_ROOT / "data")
        paths = {layer: env.get(f"{layer.upper()}_PATH") or join_path(base, layer)
                 for layer in LAYERS}
        return cls(runtime=runtime, **paths)

    def dataset(self, layer: str, name: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer: {layer}")
        return join_path(getattr(self, layer), name)


def require_local_audit() -> None:
    """Legacy audit runners intentionally operate only on repository-local data."""
    if runtime_env() != RuntimeEnv.LOCAL:
        raise ValueError("Local audit runner only; use src.pipelines.run in Databricks")
