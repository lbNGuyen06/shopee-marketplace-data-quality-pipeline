import importlib.util
import sys
from pathlib import Path
from types import ModuleType


class StubDAG:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class StubBashOperator:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.downstream = None

    def __rshift__(self, other):
        self.downstream = other
        return other


def install_airflow_stubs(monkeypatch) -> None:
    module_names = (
        "airflow",
        "airflow.sdk",
        "airflow.providers",
        "airflow.providers.standard",
        "airflow.providers.standard.operators",
        "airflow.providers.standard.operators.bash",
    )
    modules = {
        name: ModuleType(name)
        for name in module_names
    }
    modules["airflow.sdk"].DAG = StubDAG
    modules[
        "airflow.providers.standard.operators.bash"
    ].BashOperator = StubBashOperator

    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def load_dag_module(monkeypatch):
    install_airflow_stubs(monkeypatch)
    dag_path = (
        Path(__file__).resolve().parents[2]
        / "dags"
        / "shopee_marketplace_quality.py"
    )
    spec = importlib.util.spec_from_file_location(
        "test_shopee_marketplace_quality_dag",
        dag_path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dag_has_safe_incremental_schedule(monkeypatch) -> None:
    module = load_dag_module(monkeypatch)

    assert module.dag.kwargs["dag_id"] == (
        "shopee_marketplace_quality"
    )
    assert module.dag.kwargs["schedule"] == "0 2 * * *"
    assert module.dag.kwargs["catchup"] is False
    assert module.dag.kwargs["max_active_runs"] == 1
    assert module.default_args["retries"] == 3


def test_dag_uses_environment_and_cli_without_credentials(
    monkeypatch,
) -> None:
    module = load_dag_module(monkeypatch)
    batch_command = module.run_incremental_batch.kwargs[
        "bash_command"
    ]
    health_command = module.show_health_report.kwargs[
        "bash_command"
    ]

    assert "shopee-quality run-batch" in batch_command
    assert "${PIPELINE_INITIAL_START" in batch_command
    assert "${INCREMENTAL_OVERLAP_MINUTES" in batch_command
    assert "${SOURCE_FRESHNESS_THRESHOLD_MINUTES" in batch_command
    assert "shopee-quality show-health" in health_command
    assert "${HEALTH_REPORT_DAYS" in health_command
    assert module.run_incremental_batch.kwargs["append_env"] is True
    assert module.show_health_report.kwargs["append_env"] is True
    assert "PASSWORD" not in batch_command
    assert "PASSWORD" not in health_command


def test_health_report_runs_after_successful_batch(monkeypatch) -> None:
    module = load_dag_module(monkeypatch)

    assert module.run_incremental_batch.downstream is (
        module.show_health_report
    )


def test_dag_configuration_can_be_overridden(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_DAG_SCHEDULE", "15 3 * * *")
    monkeypatch.setenv("PIPELINE_RETRY_COUNT", "5")
    monkeypatch.setenv("PIPELINE_RETRY_DELAY_SECONDS", "60")

    module = load_dag_module(monkeypatch)

    assert module.dag.kwargs["schedule"] == "15 3 * * *"
    assert module.default_args["retries"] == 5
    assert module.default_args["retry_delay"].total_seconds() == 60
