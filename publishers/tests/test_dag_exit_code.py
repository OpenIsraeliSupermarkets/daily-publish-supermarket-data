"""A failed DAG operation makes the one-shot run exit non-zero (#122)."""

from unittest.mock import Mock, patch

import pytest

from main import run_operations_once
from publishers.dag_simulator import SupermarketDataPublisher


@pytest.fixture
def publisher(tmp_path):
    """A publisher whose operations are stubbed and whose tz check is skipped."""
    instance = SupermarketDataPublisher(
        long_term_db_target=Mock(),
        short_term_db_target=Mock(),
        app_folder=str(tmp_path),
    )
    with patch.object(SupermarketDataPublisher, "_check_tz", lambda self: None):
        yield instance


def _stub_operations(publisher, failing):
    calls = []

    def execute(operation):
        calls.append(operation)
        if operation in failing:
            raise RuntimeError(f"{operation} broke")

    publisher._execute_single_operation = execute
    return calls


def test_run_returns_failed_operations_and_still_runs_the_rest(publisher):
    calls = _stub_operations(publisher, failing={"publishing"})

    failed = publisher._execute_operations("publishing,clean_all_source_data")

    assert failed == ["publishing"]
    assert calls == ["publishing", "clean_all_source_data"]


def test_run_returns_empty_list_when_all_succeed(publisher):
    _stub_operations(publisher, failing=set())

    assert publisher._execute_operations("scraping,converting") == []


def test_one_shot_exit_code_is_non_zero_when_an_operation_fails(publisher):
    _stub_operations(publisher, failing={"converting"})

    assert run_operations_once(publisher, "scraping,converting,clean_dump_files") == 1


def test_one_shot_exit_code_is_zero_when_all_succeed(publisher):
    _stub_operations(publisher, failing=set())

    assert run_operations_once(publisher, "scraping,converting,clean_dump_files") == 0
