from __future__ import annotations
from typing import Optional
from .io_s3 import S3Storage
from .logging import get_logger

_logger = get_logger("idempotency")


def _is_slot_task_id(task_id: Optional[str]) -> bool:
    """True if task_id is slot-style (S0, S1, S2, ...)."""
    if not task_id or not isinstance(task_id, str):
        return False
    if len(task_id) < 2 or task_id[0] != "S":
        return False
    return task_id[1:].isdigit()


def result_exists(storage: Optional[S3Storage], step_prefix: str, task_id: Optional[str] = None) -> bool:
    """
    Check if task output already exists (idempotency check).

    step_prefix is step-level: s3://.../job_id/step/
    If task_id is slot-style (S0, S1, ...), checks only that slot's output so fan-out tasks
    are idempotent per slot. Otherwise checks step-level (metrics.json or S0/result.* / legacy).
    """
    if not storage or not step_prefix:
        return False

    prefix = step_prefix.rstrip("/")

    try:
        # Fan-out: this task writes to step/S0/, step/S1/, etc. Check only that slot.
        if _is_slot_task_id(task_id):
            slot_prefix = f"{prefix}/{task_id}/"
            for fname in ("result.json", "result.png", "result.mp4"):
                uri = f"{slot_prefix}{fname}"
                if storage.exists(uri):
                    _logger.info("Task already completed (idempotent)", {"output_prefix": slot_prefix, "found": fname})
                    return True
            _logger.debug("No existing result in slot", {"slot": task_id, "step_prefix": step_prefix})
            return False

        # Single-output / legacy: check step level
        for fname in ("metrics.json", "result.json", "result.png", "result.mp4"):
            uri = f"{prefix}/{fname}"
            if storage.exists(uri):
                _logger.info("Task already completed (idempotent)", {"output_prefix": step_prefix, "found": fname})
                return True
        for slot in ("S0/result.json", "S0/result.png", "S0/result.mp4"):
            uri = f"{prefix}/{slot}"
            if storage.exists(uri):
                _logger.info("Task already completed (idempotent)", {"output_prefix": step_prefix, "found": slot})
                return True
        _logger.debug("No existing result found", {"output_prefix": step_prefix})
        return False
    except Exception as e:
        _logger.warning("Idempotency check failed (assuming not exists)", {"output_prefix": step_prefix, "error": str(e)})
        return False
