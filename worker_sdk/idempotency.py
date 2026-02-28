from __future__ import annotations
from typing import Optional
from .io_s3 import S3Storage
from .logging import get_logger

_logger = get_logger("idempotency")


def result_exists(storage: Optional[S3Storage], output_prefix: str) -> bool:
    """
    Check if task output already exists (idempotency check).

    output_prefix is step-level: s3://.../job_id/step/
    Checks: metrics.json at step, or S0/result.* (slot layout), or result.* at step (legacy).
    """
    if not storage or not output_prefix:
        return False

    prefix = output_prefix.rstrip("/")

    try:
        for fname in ("metrics.json", "result.json", "result.png", "result.mp4"):
            uri = f"{prefix}/{fname}"
            if storage.exists(uri):
                _logger.info("Task already completed (idempotent)", {"output_prefix": output_prefix, "found": fname})
                return True
        # Slot layout: S0/result.*
        for slot in ("S0/result.json", "S0/result.png", "S0/result.mp4"):
            uri = f"{prefix}/{slot}"
            if storage.exists(uri):
                _logger.info("Task already completed (idempotent)", {"output_prefix": output_prefix, "found": slot})
                return True
        _logger.debug("No existing result found", {"output_prefix": output_prefix})
        return False
    except Exception as e:
        _logger.warning("Idempotency check failed (assuming not exists)", {"output_prefix": output_prefix, "error": str(e)})
        return False
