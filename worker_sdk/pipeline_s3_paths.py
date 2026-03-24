"""
Canonical S3 URI **templates** for pipeline work (runner + gateway must agree).

Layout (all steps):

    s3://{bucket}/{user_id}/{job_id}/{STEP}/{task_id}/…

- No ``users/`` segment after the bucket.
- ``{STEP}`` is the same string as the work message ``step`` field (e.g. ``MODEL_PROFILE``).
- ``task_id`` is the path segment (usually ``S0`` for the root task).

``input_uri`` / ``output_prefix`` are still **normalized** by
``worker_sdk.work_queue_message.normalize_s3_uri`` when assembling a message.
"""

from __future__ import annotations

STEP_MODEL_PROFILE = "MODEL_PROFILE"


def pipeline_step_base_prefix(
    bucket: str,
    user_id: str,
    job_id: str,
    step: str,
    task_id: str,
) -> str:
    """
    Directory prefix for a step (before ``normalize_s3_uri`` adds the trailing ``/``).

    Example: ``s3://vastra/<uid>/<jid>/MODEL_PROFILE/S0``
    """
    u = (user_id or "").strip()
    j = (job_id or "").strip()
    st = (step or "").strip().upper()
    tid = (task_id or "").strip() or "S0"
    b = (bucket or "").strip()
    if not b or not u or not j or not st:
        raise ValueError("bucket, user_id, job_id, and step are required")
    return f"s3://{b}/{u}/{j}/{st}/{tid}"


def model_profile_uris(
    bucket: str,
    user_id: str,
    job_id: str,
    *,
    task_id: str = "S0",
) -> tuple[str, str]:
    """
    Returns ``(input_uri, output_prefix)`` for MODEL_PROFILE.

    - ``input_uri``: ``…/{STEP}/{task_id}/request.json``
    - ``output_prefix``: ``…/{STEP}/{task_id}`` (directory; SDK appends ``/``)
    """
    base = pipeline_step_base_prefix(
        bucket, user_id, job_id, STEP_MODEL_PROFILE, task_id
    )
    return f"{base}/request.json", base
