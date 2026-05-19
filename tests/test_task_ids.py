"""Tests for worker_sdk.task_ids."""

from worker_sdk.task_ids import (
    scope_task_id,
    task_path_segment,
    task_result_lookup_keys,
)


def test_scope_task_id_idempotent():
    jid = "job-1779159870874"
    assert scope_task_id(jid, "S0") == "job-1779159870874-S0"
    assert scope_task_id(jid, "job-1779159870874-S0") == "job-1779159870874-S0"


def test_task_path_segment_from_scoped():
    jid = "job-1779159870874"
    assert task_path_segment("job-1779159870874-S0", jid) == "S0"
    assert task_path_segment("S0", jid) == "S0"


def test_task_result_lookup_keys():
    keys = task_result_lookup_keys("S0", "job-abc")
    assert keys[0] == "job-abc-S0"
    assert "S0" in keys
