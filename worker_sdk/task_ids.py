"""
Task ID conventions (single source of truth).

- **Postgres ``tasks.task_id`` / ``task_results.task_id``** — globally unique:
  ``{job_id}-{slot}`` (e.g. ``job-1779159870874-S0``). The UI may send only ``S0``.
- **S3 path segment** under ``…/{step}/`` — slot only: ``S0``, ``S1``, … (not the full scoped id).

Never use bare ``S0`` as a database primary key across jobs.
"""

from __future__ import annotations

import re
from typing import List

_SLOT_RE = re.compile(r"^S\d+$")


def scope_task_id(job_id: str, task_id: str = "S0") -> str:
    """
    Return a globally unique task id for RDS: ``{job_id}-{slot}``.

    Idempotent when ``task_id`` is already scoped for this job.
    """
    jid = (job_id or "").strip()
    if not jid:
        raise ValueError("job_id must be non-empty")
    tid = (task_id or "").strip() or "S0"
    prefix = f"{jid}-"
    if tid.startswith(prefix):
        return tid
    return f"{prefix}{tid}"


def task_path_segment(task_id: str, job_id: str | None = None) -> str:
    """
    S3 folder name under ``s3://…/{user}/{job}/{step}/``.

    ``job-abc-S0`` → ``S0``; bare ``S0`` → ``S0``.
    """
    tid = (task_id or "").strip() or "S0"
    if job_id:
        prefix = f"{job_id.strip()}-"
        if tid.startswith(prefix):
            suffix = tid[len(prefix) :]
            if suffix:
                return suffix
    if _SLOT_RE.match(tid):
        return tid
    return tid


def task_result_lookup_keys(task_id: str, job_id: str) -> List[str]:
    """
    Keys to try when joining ``tasks`` → ``task_results`` (new scoped + legacy ``S0``).
    """
    scoped = scope_task_id(job_id, task_id)
    slot = task_path_segment(task_id, job_id)
    keys: List[str] = []
    for k in (scoped, slot, (task_id or "").strip()):
        if k and k not in keys:
            keys.append(k)
    return keys
