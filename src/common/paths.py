"""Local and Spark path handling without filesystem operations on remote URIs."""

import os
import posixpath
import re
from pathlib import Path, PureWindowsPath
from urllib.parse import urlsplit, urlunsplit


def is_remote_path(path: str | Path) -> bool:
    value = str(path)
    return value.startswith(("dbfs:/", "abfss://", "/Volumes/")) or value == "/Volumes"


def to_spark_path(path: str | Path) -> str:
    value = str(path)
    if not value.strip():
        raise ValueError("Path must be nonempty")
    if value.startswith("/Volumes/") or value == "/Volumes":
        return posixpath.normpath(value)
    if value.startswith(("dbfs:/", "abfss://")):
        parts = urlsplit(value)
        if parts.query or parts.fragment or "\\" in value:
            raise ValueError("Storage paths must not contain query, fragment or backslash")
        if parts.scheme == "abfss" and not parts.netloc:
            raise ValueError("abfss requires a storage authority")
        if parts.scheme == "dbfs" and parts.netloc:
            raise ValueError("Use dbfs:/ paths without an authority")
        normalized = posixpath.normpath(parts.path or "/")
        if parts.scheme == "dbfs":
            return "dbfs:" + normalized
        return urlunsplit((parts.scheme, parts.netloc, normalized, "", ""))
    if PureWindowsPath(value).is_absolute() and os.name != "nt":
        return PureWindowsPath(value).as_posix()
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", value) and not re.match(r"^[A-Za-z]:[\\/]", value):
        raise ValueError("Unsupported storage URI")
    return Path(value).resolve().as_posix()


def destination_path(path: str | Path) -> str | Path:
    """Keep Path return values for local callers and strings for Spark locations."""
    normalized = to_spark_path(path)
    return normalized if is_remote_path(normalized) else Path(normalized)


def join_path(base: str | Path, child: str) -> str:
    if not child or child in (".", "..") or any(c in child for c in ("/", "\\", ":")):
        raise ValueError("Child must be a single relative path component")
    return to_spark_path(base).rstrip("/") + "/" + child


def require_separate_paths(paths: list[str | Path]) -> None:
    """Reject equal/nested layer paths, including the dbfs:/Volumes alias."""
    keys = []
    for path in paths:
        key = to_spark_path(path).rstrip("/")
        if key.startswith("dbfs:/Volumes/"):
            key = key[5:]
        if re.match(r"^[A-Za-z]:/", key):
            key = key.casefold()
        keys.append(key)
    for index, left in enumerate(keys):
        for right in keys[index + 1:]:
            if left == right or left.startswith(right + "/") or right.startswith(left + "/"):
                raise ValueError("Layer paths must be separate and non-overlapping")
