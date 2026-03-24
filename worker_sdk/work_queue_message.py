"""
Work-queue SQS message contract — **single source of truth** in this module.

**Shape**
    ``WorkQueueMessage`` defines the JSON object. Required keys are all dataclass
    fields except optional ``parent_task_id`` (see ``WORK_QUEUE_MESSAGE_KEYS``).

**APIs**
    Producers: ``assemble_work_queue_message`` → ``to_json_dict``.
    Consumers: ``parse_work_queue_message`` / ``parse_work_queue_message_json``.

**Rules** (enforced here; keep workers/gateway on this SDK — do not re-encode elsewhere)

1. **Root value** — Must be a JSON object (not an array or scalar).

2. **Required keys** — Exactly the set ``WORK_QUEUE_MESSAGE_KEYS``. Missing key →
   ``ValueError``. ``parent_task_id`` is optional; if present must be a string.

3. **Types** — ``params`` must be a JSON object (dict). ``retry_count`` must be an
   integer ≥ 0.

4. **Strings** — ``user_id`` and ``step`` non-empty after strip; ``step`` uppercased;
   ``schema`` defaults to ``"v1"`` if blank when assembling.

5. **task_id** — Trimmed; if empty after trim, use ``"S0"``.

6. **job_id** — Non-empty after strip. If the value is exactly 8 hex digits *or*
   exactly 32 hex digits, it is **normalized to lowercase** (runner/S3 path regexes).

7. **S3 URIs** (``input_uri``, ``output_prefix``) — Caller passes ``expected_bucket``
   (e.g. ``WORK_BUCKET`` env); must match the bucket in the URI:
   - Scheme ``s3://``, non-empty bucket, non-empty key path.
   - Bucket name must match the conservative pattern ``_BUCKET_RE`` (3–63 chars,
     typical AWS rules subset).
   - Bucket in the URI must **equal** ``expected_bucket``.
   - **output_prefix** is a directory prefix: after normalization the key **must end
     with** ``/`` so workers can join keys under it (S3 prefix semantics).
   - No ASCII control characters (``\\x00``–``\\x1f``) in the key.
   - Path segments that are exactly 8- or 32-character hex (URL-decoded per
     segment) are lowercased (user id / job id segments).

8. **Version** — ``WORK_QUEUE_MESSAGE_VERSION`` bumps when the JSON shape changes.

Machine-readable labels for tests/docs: ``WORK_QUEUE_VALIDATION_RULES``.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, fields
from typing import Any, Dict, FrozenSet, Literal, Optional, Tuple
from urllib.parse import unquote

# Bump when the JSON shape changes (document in release notes).
WORK_QUEUE_MESSAGE_VERSION = 1

# Short labels for the rule set (keep in sync with the module docstring **Rules**).
WORK_QUEUE_VALIDATION_RULES: tuple[str, ...] = (
    "json_root_object",
    "required_keys_match_dataclass_minus_parent_task_id",
    "params_object_retry_count_int_non_negative",
    "user_id_step_non_empty_step_uppercase",
    "task_id_default_S0",
    "job_id_hex_8_or_32_lowercased",
    "s3_uri_bucket_matches_expected_bucket",
    "s3_bucket_name_pattern",
    "output_prefix_directory_ends_with_slash",
    "s3_key_no_control_chars",
    "s3_hex_segments_lower",
)

_HEX8 = re.compile(r"^[0-9A-Fa-f]{8}$")
_HEX32 = re.compile(r"^[0-9A-Fa-f]{32}$")
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


@dataclass(frozen=True)
class WorkQueueMessage:
    """
    Canonical JSON body for pipeline work queues (one object per SQS message).

    Field names are the wire JSON keys. ``parent_task_id`` may be omitted on the wire.
    """

    job_id: str
    task_id: str
    user_id: str
    schema: str
    step: str
    input_uri: str
    output_prefix: str
    params: Dict[str, Any]
    retry_count: int
    parent_task_id: Optional[str] = None

    def to_json_dict(self) -> Dict[str, Any]:
        """Serialize for ``json.dumps``; omits ``parent_task_id`` when ``None``."""
        d = asdict(self)
        if d.get("parent_task_id") is None:
            d.pop("parent_task_id", None)
        return d


# Required JSON keys — derived from the dataclass (single source of truth).
WORK_QUEUE_MESSAGE_KEYS: FrozenSet[str] = frozenset(
    f.name for f in fields(WorkQueueMessage) if f.name != "parent_task_id"
)


def normalize_job_id(job_id: str) -> str:
    """Lowercase 8- or 32-char hex job ids for strict runner S3 path checks."""
    s = (job_id or "").strip()
    if not s:
        raise ValueError("job_id must be non-empty")
    if _HEX8.match(s) or (len(s) == 32 and _HEX32.match(s)):
        return s.lower()
    return s


def _normalize_s3_key_hex_segments(key: str) -> str:
    out: list[str] = []
    for seg in key.split("/"):
        if seg == "":
            out.append("")
            continue
        seg_decoded = unquote(seg)
        if _HEX8.match(seg_decoded):
            out.append(seg_decoded.lower())
        elif _HEX32.match(seg_decoded):
            out.append(seg_decoded.lower())
        else:
            out.append(seg)
    return "/".join(out)


def normalize_s3_uri(
    uri: str,
    *,
    role: Literal["output_prefix", "input_uri"],
    expected_bucket: str,
) -> str:
    u = (uri or "").strip()
    if not u:
        raise ValueError(f"{role} is empty")
    if not u.startswith("s3://"):
        raise ValueError(f"{role} must start with s3:// (got {u[:80]!r}…)")
    rest = u[5:]
    if "/" not in rest:
        raise ValueError(f"{role} must be s3://bucket/key/path")
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"{role} must include bucket and key path")
    if not _BUCKET_RE.match(bucket):
        raise ValueError(f"{role} has invalid S3 bucket name: {bucket!r}")
    if bucket != expected_bucket:
        raise ValueError(
            f"{role} must use work bucket {expected_bucket!r}, not {bucket!r}"
        )
    if re.search(r"[\x00-\x1f]", key):
        raise ValueError(f"{role} key contains control characters")
    key_norm = _normalize_s3_key_hex_segments(key)
    if role == "output_prefix":
        # Workers treat this as an S3 *prefix*; join() and validators expect a path ending in /.
        if not key_norm.endswith("/"):
            key_norm = key_norm + "/"
    return f"s3://{bucket}/{key_norm}"


def prepare_task_uris(
    *,
    job_id: str,
    input_uri: str,
    output_prefix: str,
    expected_bucket: str,
) -> Tuple[str, str, str]:
    """Returns ``(job_id, input_uri, output_prefix)`` normalized for tasks + SQS."""
    jid = normalize_job_id(job_id)
    inp = normalize_s3_uri(
        input_uri.strip(), role="input_uri", expected_bucket=expected_bucket
    )
    out = normalize_s3_uri(
        output_prefix.strip(), role="output_prefix", expected_bucket=expected_bucket
    )
    return jid, inp, out


def assemble_work_queue_message(
    *,
    job_id: str,
    task_id: str,
    user_id: str,
    schema: str,
    step: str,
    input_uri: str,
    output_prefix: str,
    params: Dict[str, Any],
    retry_count: int,
    parent_task_id: Optional[str],
    expected_bucket: str,
) -> WorkQueueMessage:
    """Build a validated ``WorkQueueMessage`` (normalizes job id and S3 URIs)."""
    jid, inp, out = prepare_task_uris(
        job_id=job_id,
        input_uri=input_uri,
        output_prefix=output_prefix,
        expected_bucket=expected_bucket,
    )
    tid = (task_id or "").strip() or "S0"
    uid = (user_id or "").strip()
    if not uid:
        raise ValueError("user_id must be non-empty")
    sch = (schema or "").strip() or "v1"
    st = (step or "").strip().upper()
    if not st:
        raise ValueError("step must be non-empty")
    rc = int(retry_count)
    if rc < 0:
        raise ValueError("retry_count must be >= 0")
    p = params if isinstance(params, dict) else {}
    return WorkQueueMessage(
        job_id=jid,
        task_id=tid,
        user_id=uid,
        schema=sch,
        step=st,
        input_uri=inp,
        output_prefix=out,
        params=p,
        retry_count=rc,
        parent_task_id=parent_task_id,
    )


def parse_work_queue_message(
    raw: Dict[str, Any],
    *,
    expected_bucket: str,
) -> WorkQueueMessage:
    """
    Parse a dict (e.g. ``json.loads(SQS MessageBody)``) into ``WorkQueueMessage``.

    Required keys are exactly ``WORK_QUEUE_MESSAGE_KEYS`` (from the dataclass).
    """
    if not isinstance(raw, dict):
        raise ValueError("work message must be a JSON object")
    missing = WORK_QUEUE_MESSAGE_KEYS - raw.keys()
    if missing:
        raise ValueError(f"work message missing keys: {sorted(missing)}")
    params = raw.get("params")
    if not isinstance(params, dict):
        raise ValueError("params must be a JSON object")
    rc = raw.get("retry_count")
    try:
        retry_count = int(rc) if rc is not None else 0
    except (TypeError, ValueError) as e:
        raise ValueError("retry_count must be an integer") from e
    parent = raw.get("parent_task_id")
    if parent is not None and not isinstance(parent, str):
        raise ValueError("parent_task_id must be a string or null")

    return assemble_work_queue_message(
        job_id=str(raw["job_id"]),
        task_id=str(raw["task_id"]),
        user_id=str(raw["user_id"]),
        schema=str(raw["schema"]),
        step=str(raw["step"]),
        input_uri=str(raw["input_uri"]),
        output_prefix=str(raw["output_prefix"]),
        params=params,
        retry_count=retry_count,
        parent_task_id=parent if parent is None else str(parent),
        expected_bucket=expected_bucket,
    )


def parse_work_queue_message_json(
    body: str,
    *,
    expected_bucket: str,
) -> WorkQueueMessage:
    """Parse raw SQS ``MessageBody`` string (UTF-8 JSON object)."""
    try:
        raw = json.loads(body)
    except json.JSONDecodeError as e:
        raise ValueError("work message body is not valid JSON") from e
    return parse_work_queue_message(raw, expected_bucket=expected_bucket)


def from_json_dict(
    raw: Dict[str, Any],
    *,
    expected_bucket: str,
) -> WorkQueueMessage:
    """Alias for :func:`parse_work_queue_message` (symmetric with :meth:`WorkQueueMessage.to_json_dict`)."""
    return parse_work_queue_message(raw, expected_bucket=expected_bucket)
