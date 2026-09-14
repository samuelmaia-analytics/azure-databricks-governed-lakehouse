"""Pure publication decisions over aggregated metrics; no Spark or storage actions."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from math import isclose, isfinite

from src.quality.checks import CheckType
from src.quality.rules import Severity


class PublicationStatus(str, Enum):
    APPROVED = "APPROVED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class GatePolicy:
    max_error_rate: float = 0.01

    def __post_init__(self):
        if not 0 <= self.max_error_rate <= 1:
            raise ValueError("max_error_rate must be between 0 and 1")


DEFAULT_POLICY = GatePolicy()


@dataclass(frozen=True)
class PublicationDecision:
    dataset: str
    status: PublicationStatus
    reason: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    error_rate: float
    warning_rows: int
    failed_error_checks: int
    failed_warning_checks: int
    checked_at: datetime


def evaluate_publication_gate(
    summary: Mapping, checks: Iterable[Mapping] = (), policy: GatePolicy = DEFAULT_POLICY
) -> PublicationDecision:
    """Accept summary/check dictionaries (Spark aggregate Row.asDict()), for one dataset.

    ERROR dataset checks block; ERROR row checks require review below the threshold.
    An empty dataset requires review. The caller must provide all relevant check results.
    """
    dataset = summary["dataset"]
    if not isinstance(dataset, str) or not dataset.strip():
        raise ValueError("dataset must be nonempty")
    total, valid, invalid, warnings = (
        summary[k] for k in ("total_rows", "valid_rows", "invalid_rows", "warning_rows")
    )
    if any(type(n) is not int or n < 0 for n in (total, valid, invalid, warnings)):
        raise ValueError("Row counts must be nonnegative integers")
    if total != valid + invalid or warnings > total:
        raise ValueError("Inconsistent row counts")
    rate = invalid / total if total else 0.0
    if not isfinite(summary["error_rate"]) or not isclose(summary["error_rate"], rate):
        raise ValueError("error_rate differs from invalid_rows / total_rows")
    errors = warning_checks = 0
    blocked = False
    reasons = []
    seen = set()
    for check in checks:
        if check["dataset"] != dataset:
            raise ValueError("Check belongs to another dataset")
        key = (check["check_type"], check["check_name"])
        if key in seen:
            raise ValueError("Duplicate check result")
        seen.add(key)
        kind = CheckType(check["check_type"])
        severity = Severity(check["severity"])
        if check["status"] not in ("PASS", "FAIL"):
            raise ValueError("Check must have completed with PASS or FAIL")
        if check["status"] == "FAIL":
            reasons.append(f"{severity.value}: {check['check_name']}")
            if severity == Severity.ERROR:
                errors += 1
                blocked |= kind in (CheckType.REFERENTIAL_INTEGRITY, CheckType.UNIQUENESS)
            else:
                warning_checks += 1
    if rate > policy.max_error_rate:
        blocked = True
        reasons.append(f"Taxa de erro {rate:.2%} excede {policy.max_error_rate:.2%}")
    elif invalid:
        reasons.append("Há registros inválidos dentro do limite de revisão")
    if warnings:
        reasons.append("Há registros com WARNING")
    if total == 0:
        reasons.append("Dataset vazio requer revisão")
    status = (PublicationStatus.BLOCKED if blocked else
              PublicationStatus.NEEDS_REVIEW if reasons else PublicationStatus.APPROVED)
    return PublicationDecision(
        dataset, status, "; ".join(reasons) or "Nenhuma falha ou aviso informado",
        total, valid, invalid, rate, warnings, errors, warning_checks, datetime.now(timezone.utc),
    )
