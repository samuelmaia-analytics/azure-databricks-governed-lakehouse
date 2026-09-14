import pytest

from src.quality.publication_gate import GatePolicy, evaluate_publication_gate


def metrics(invalid=0, warnings=0, total=1000):
    return dict(dataset="sample", total_rows=total, valid_rows=total-invalid,
                invalid_rows=invalid, error_rate=invalid/total if total else 0,
                warning_rows=warnings)


def check(kind="REFERENTIAL_INTEGRITY", severity="ERROR", status="FAIL"):
    return dict(dataset="sample", check_name="example", check_type=kind,
                severity=severity, status=status)


@pytest.mark.parametrize("invalid,expected", [
    (0, "APPROVED"), (5, "NEEDS_REVIEW"), (10, "NEEDS_REVIEW"), (11, "BLOCKED"),
])
def test_thresholds(invalid, expected):
    result = evaluate_publication_gate(metrics(invalid))
    assert result.status == expected
    assert result.invalid_rows == invalid
    assert result.total_rows == result.valid_rows + result.invalid_rows
    assert result.checked_at.tzinfo is not None


@pytest.mark.parametrize("kind", ["REFERENTIAL_INTEGRITY", "UNIQUENESS"])
def test_error_dataset_check_blocks(kind):
    result = evaluate_publication_gate(metrics(), [check(kind)])
    assert result.status == "BLOCKED"
    assert result.failed_error_checks == 1


def test_warnings_and_row_checks():
    assert evaluate_publication_gate(metrics(warnings=1)).status == "NEEDS_REVIEW"
    result = evaluate_publication_gate(metrics(), [check(severity="WARNING")])
    assert result.status == "NEEDS_REVIEW"
    assert result.failed_warning_checks == 1
    assert result.failed_error_checks == 0
    assert evaluate_publication_gate(metrics(5), [check("ROW_RULE")]).status == "NEEDS_REVIEW"
    assert evaluate_publication_gate(metrics(), [check(status="PASS")]).status == "APPROVED"


def test_multiple_reasons():
    warning = {**check(severity="WARNING"), "check_name": "warning"}
    result = evaluate_publication_gate(metrics(20, 3), [check(), warning])
    assert result.status == "BLOCKED"
    assert result.failed_error_checks == result.failed_warning_checks == 1
    for text in ("example", "warning", "excede", "registros com WARNING"):
        assert text in result.reason


def test_empty_and_custom_policy():
    result = evaluate_publication_gate(metrics(total=0))
    assert result.status == "NEEDS_REVIEW"
    assert "vazio" in result.reason
    assert evaluate_publication_gate(metrics(20), policy=GatePolicy(0.02)).status == "NEEDS_REVIEW"


def test_invalid_inputs():
    with pytest.raises(ValueError):
        GatePolicy(-1)
    with pytest.raises(ValueError, match="Inconsistent"):
        evaluate_publication_gate({**metrics(), "valid_rows": 2})
    with pytest.raises(ValueError, match="error_rate"):
        evaluate_publication_gate({**metrics(), "error_rate": 0.5})
    with pytest.raises(ValueError, match="another dataset"):
        evaluate_publication_gate(metrics(), [{**check(), "dataset": "other"}])
    with pytest.raises(ValueError, match="Duplicate"):
        evaluate_publication_gate(metrics(), [check(), check()])
    with pytest.raises(ValueError, match="completed"):
        evaluate_publication_gate(metrics(), [check(status="ERROR")])
