"""
Publish **step completion** messages to the optional SQS ``queues.results`` URL.

Contract matches ``runner._results_completion_payload`` with required ``status``:
``SUCCEEDED`` | ``FAILED``. Consumed by ``apps/workers/cpu_results`` → ``PostgresDB.apply_results_completion``.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Literal, Optional

from .constants import TaskMessage, get_results_queue_url
from .io_sqs import SQSClient
from .logging import get_logger

_log = get_logger("results_queue")


def join_s3_output_uri(output_prefix: str, filename: str) -> str:
    """``output_prefix`` is a directory ``s3://bucket/key/.../``; append ``filename``."""
    if not isinstance(output_prefix, str) or not output_prefix.startswith("s3://"):
        raise ValueError("output_prefix must be an s3:// directory prefix")
    base = output_prefix if output_prefix.endswith("/") else output_prefix + "/"
    return base + filename


def build_step_completion_payload(
    tm: TaskMessage,
    *,
    status: Literal["SUCCEEDED", "FAILED"],
    primary_result_uri: Optional[str] = None,
    metrics_uri: Optional[str] = None,
    primary_mime: Optional[str] = None,
    primary_size_bytes: Optional[int] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """JSON-serializable body for the configured ``queues.results`` queue (and DLQ inspection)."""
    step = str(tm.step).upper().strip()
    out: Dict[str, Any] = {
        "schema": getattr(tm, "schema", None) or "v1",
        "job_id": str(tm.job_id),
        "task_id": str(tm.task_id),
        "user_id": tm.user_id,
        "step": step,
        "status": status,
        "input_uri": str(getattr(tm, "input_uri", "") or ""),
        "output_prefix": str(tm.output_prefix),
        "params": dict(getattr(tm, "params", {}) or {}),
        "retry_count": int(getattr(tm, "retry_count", 0) or 0),
    }
    if primary_result_uri:
        out["primary_result_uri"] = primary_result_uri
    if metrics_uri:
        out["metrics_uri"] = metrics_uri
    if primary_mime:
        out["primary_mime"] = primary_mime
    if primary_size_bytes is not None:
        out["primary_size_bytes"] = int(primary_size_bytes)
    if error_code:
        out["error_code"] = str(error_code)[:256]
    if error_message:
        out["error_message"] = str(error_message)[:8000]
    if getattr(tm, "trace_id", None) is not None:
        out["trace_id"] = str(tm.trace_id)
    if getattr(tm, "parent_task_id", None) is not None:
        out["parent_task_id"] = str(tm.parent_task_id)
    if extra:
        out["extra"] = extra
    json.dumps(out)
    return out


def publish_step_completion(payload: Dict[str, Any], *, sqs: Optional[SQSClient] = None) -> bool:
    """
    Send one message to the results queue. Returns False if ``queues.results`` is unset
    or publish fails (logged).
    """
    url = get_results_queue_url()
    if not url:
        _log.warning("results queue skipped: no queues.results in WORKER_CONFIG YAML")
        return False
    client = sqs or SQSClient()
    try:
        client.send_message(url, payload)
        return True
    except Exception as e:
        _log.error(e, extra={"task_id": payload.get("task_id"), "job_id": payload.get("job_id")})
        return False
