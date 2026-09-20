"""CLI and Databricks-callable orchestration: python -m src.pipelines.run --stage full."""

import argparse
import json

from src.common.runtime import RuntimeConfig, RuntimeEnv
from src.common.spark import get_spark_session
from src.pipelines import stages

STAGES = ("bronze", "silver", "gold")


class PipelineFailure(RuntimeError):
    def __init__(self, report):
        self.report = report
        super().__init__(f"Pipeline failed at {report['failed_stage']}")


def run_pipeline(stage="full", *, config=None, spark=None):
    """Run one stage or all stages; never stop a caller/platform-owned session.

    Stages commit tables individually. Earlier successful writes are not rolled
    back on failure. Stage-only runs require valid inputs from the preceding layer.
    """
    if stage not in (*STAGES, "full"):
        raise ValueError(f"Unknown stage: {stage}")
    config = config or RuntimeConfig.from_env()
    session = get_spark_session("Governed Lakehouse", runtime=config.runtime, existing=spark)
    owns_session = spark is None and config.runtime == RuntimeEnv.LOCAL
    report = {"runtime": config.runtime.value, "status": "RUNNING", "stages": {}}
    try:
        for name in STAGES if stage == "full" else (stage,):
            try:
                report["stages"][name] = getattr(stages, f"run_{name}")(session, config)
            except Exception as exc:
                report.update(status="FAILED", failed_stage=name, error_type=type(exc).__name__)
                if isinstance(exc, stages.GateRejected):
                    report["gates"] = exc.decisions
                raise PipelineFailure(report) from exc
        report["status"] = "SUCCESS"
        return report
    finally:
        if owns_session:
            session.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=(*STAGES, "full"), default="full")
    args = parser.parse_args(argv)
    try:
        report = run_pipeline(args.stage)
    except PipelineFailure as exc:
        print(json.dumps(exc.report, ensure_ascii=False))
        raise
    print(json.dumps(report, ensure_ascii=False))
    return report


if __name__ == "__main__":
    main()
