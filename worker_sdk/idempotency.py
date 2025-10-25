from __future__ import annotations
from typing import Optional
from .io_s3 import S3Storage
from .logging import get_logger

_logger = get_logger("idempotency")


def result_exists(storage: Optional[S3Storage], output_prefix: str) -> bool:
    """
    Check if task output already exists (idempotency check).

    Args:
        storage: an instance of S3Storage (or compatible)
        output_prefix: canonical s3://.../<job_id>/<step>/<task_id>/

    Returns:
        True if result.* or metrics.json exists under that prefix, else False.
    """
    if not storage or not output_prefix:
        return False

    try:
        # We'll just check for common result files
        for fname in ("result.json", "result.png", "result.mp4", "metrics.json"):
            uri = f"{output_prefix.rstrip('/')}/{fname}"
            if storage.exists(uri):
                _logger.info("Task already completed (idempotent)", {
                    "output_prefix": output_prefix,
                    "found": fname
                })
                return True
        _logger.debug("No existing result found", {"output_prefix": output_prefix})
        return False
    except Exception as e:
        _logger.warning("Idempotency check failed (assuming not exists)", {
            "output_prefix": output_prefix,
            "error": str(e)
        })
        return False
