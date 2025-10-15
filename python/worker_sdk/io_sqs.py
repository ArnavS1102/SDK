"""
SQS client: receive, delete, extend visibility, publish next-step messages.

Core queue operations with production-grade safety:
- Retry logic for transient failures (single + batch)
- FIFO queue detection and enforcement
- Poison-pill protection (receive count tracking)
- Batch send with retry on failures (fresh Entry Ids per retry)
- Strict JSON / size guards (256 KB SQS limit)
- Tuned client timeouts for long-polling
"""

from __future__ import annotations

import json
import random
import re
import time
import uuid
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from .constants import TaskMessage, VALID_STEPS


# ============================================================================
# TYPES & CONFIG
# ============================================================================

RawMessage = Dict[str, Any]
TaskDict = Dict[str, Any]
TaskMessageLike = Union[Dict[str, Any], TaskMessage]

# Poison-pill threshold (let SQS redrive also handle normally)
MAX_RECEIVE_COUNT = 5

# Client-side retries for transient errors
RETRY_ATTEMPTS = 5
RETRIABLE_ERROR_CODES = {
    # Common SQS/HTTP transient codes (single & batch)
    "Throttling",                 # batch failures often use this short code
    "ThrottlingException",
    "ServiceUnavailable",
    "RequestThrottled",
    "InternalError",
    "InternalFailure",
    "ProvisionedThroughputExceededException",
    "RequestTimeout",
    "500",
    "502",
    "503",
    "504",
}

SQS_MAX_BODY_BYTES = 256 * 1024
SQS_MAX_VISIBILITY = 43_200  # 12h hard SQS limit


# ============================================================================
# CLIENT
# ============================================================================

def get_sqs_client(region: Optional[str] = None):
    """Create SQS client (one per process). Tuned for long-polling."""
    return boto3.client(
        "sqs",
        region_name=region,
        config=Config(
            retries={"max_attempts": 6, "mode": "standard"},
            read_timeout=70,     # > 20s long-poll
            connect_timeout=3,
        ),
    )


# ============================================================================
# INTERNAL HELPERS
# ============================================================================

def _is_fifo_queue(queue_url: str) -> bool:
    """Detect FIFO queue by URL suffix."""
    return queue_url.lower().endswith(".fifo")


def _ensure_sqs_size_ok(body: str) -> None:
    """Guard against SQS 256 KB hard limit."""
    if len(body.encode("utf-8")) > SQS_MAX_BODY_BYTES:
        raise ValueError("SQS message > 256KB; upload payload to S3 and send a pointer.")


def _err_code(e: Exception, code: str) -> bool:
    """Match botocore ClientError by error code."""
    return isinstance(e, ClientError) and e.response.get("Error", {}).get("Code") == code


def _retry_with_backoff(func: Callable, *args, **kwargs):
    """Retry a boto3 call with exponential backoff on retriable ClientError codes."""
    delay = 0.25
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in RETRIABLE_ERROR_CODES and attempt < RETRY_ATTEMPTS:
                time.sleep(delay + random.uniform(0, 0.25))
                delay = min(delay * 2, 5.0)
                continue
            raise
        except BotoCoreError:
            if attempt < RETRY_ATTEMPTS:
                time.sleep(delay + random.uniform(0, 0.25))
                delay = min(delay * 2, 5.0)
                continue
            raise
    raise RuntimeError(f"Failed after {RETRY_ATTEMPTS} attempts")


# ============================================================================
# RECEIVING WORK
# ============================================================================

def receive_messages(
    sqs,
    queue_url: str,
    max_messages: int = 1,
    wait_seconds: int = 20,
    visibility_timeout: Optional[int] = None,
) -> List[RawMessage]:
    """
    Long-poll SQS queue and return up to max_messages (1-10).
    Includes ApproximateReceiveCount for poison-pill detection.
    """
    max_n = max(1, min(int(max_messages), 10))
    wait_s = max(0, min(int(wait_seconds), 20))

    params = {
        "QueueUrl": queue_url,
        "MaxNumberOfMessages": max_n,
        "WaitTimeSeconds": wait_s,
        "MessageAttributeNames": ["All"],
        "AttributeNames": ["ApproximateReceiveCount", "SentTimestamp"],
        # Helps the SQS endpoint dedupe receive attempts on client retries
        "ReceiveRequestAttemptId": uuid.uuid4().hex,
    }
    if visibility_timeout is not None:
        params["VisibilityTimeout"] = int(visibility_timeout)

    resp = _retry_with_backoff(sqs.receive_message, **params)
    return resp.get("Messages", [])


def parse_message(
    raw_msg: RawMessage,
    *,
    allow_sns_envelope: bool = True
) -> Tuple[TaskDict, str, int]:
    """
    Parse SQS message. Returns (task_dict, receipt_handle, receive_count).
    'receive_count' is used to detect poison pills.
    """
    if not isinstance(raw_msg, dict):
        raise ValueError("parse_message: expected dict")

    try:
        receipt_handle = raw_msg["ReceiptHandle"]
    except KeyError as e:
        raise ValueError("parse_message: missing ReceiptHandle") from e

    attributes = raw_msg.get("Attributes", {})
    receive_count = int(attributes.get("ApproximateReceiveCount", 1))

    body = raw_msg.get("Body")
    if not isinstance(body, str) or not body.strip():
        raise ValueError("parse_message: Body empty or not string")

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as e:
        raise ValueError(f"parse_message: invalid JSON: {e}") from e

    # Unwrap SNS envelope (Message field may itself be JSON)
    if allow_sns_envelope and isinstance(parsed, dict) and "Message" in parsed:
        inner = parsed.get("Message")
        if isinstance(inner, str):
            try:
                parsed = json.loads(inner)
            except json.JSONDecodeError as e:
                raise ValueError(f"parse_message: SNS Message invalid: {e}") from e
        else:
            parsed = inner

    if not isinstance(parsed, dict):
        raise ValueError("parse_message: payload not a dict")

    return parsed, receipt_handle, receive_count


def is_poison_pill(receive_count: int, max_count: int = MAX_RECEIVE_COUNT) -> bool:
    """True if message has exceeded max receive attempts (poison pill)."""
    return receive_count >= max_count


# ============================================================================
# ACKNOWLEDGEMENT & VISIBILITY
# ============================================================================

def delete_message(sqs, queue_url: str, receipt_handle: str) -> None:
    """
    ACK message: permanently remove from queue.
    IMPORTANT: Only delete AFTER all outputs are durably written (idempotency).
    """
    if not isinstance(receipt_handle, str) or not receipt_handle.strip():
        raise ValueError("delete_message: receipt_handle required")

    _retry_with_backoff(
        sqs.delete_message,
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
    )


def change_visibility(
    sqs,
    queue_url: str,
    receipt_handle: str,
    visibility_timeout: int,
) -> None:
    """Extend (or shorten) message invisibility (use for long-running jobs)."""
    if not isinstance(receipt_handle, str) or not receipt_handle.strip():
        raise ValueError("change_visibility: receipt_handle required")
    if not isinstance(visibility_timeout, int) or visibility_timeout < 0:
        raise ValueError("change_visibility: timeout must be non-negative int")

    visibility_timeout = min(visibility_timeout, SQS_MAX_VISIBILITY)
    _retry_with_backoff(
        sqs.change_message_visibility,
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=visibility_timeout,
    )


def extend_visibility_loop(
    sqs,
    queue_url: str,
    receipt_handle: str,
    base_timeout: int = 60,
    heartbeat_every: int = 30,
    should_continue: Optional[Callable[[], bool]] = None,
) -> None:
    """
    Background heartbeat to extend visibility during long processing.
    Best-effort: doesn't raise. Stops if receipt handle becomes invalid.
    """
    if not isinstance(receipt_handle, str) or not receipt_handle.strip():
        raise ValueError("extend_visibility_loop: receipt_handle required")

    base_timeout = max(1, min(int(base_timeout), SQS_MAX_VISIBILITY))
    heartbeat_every = max(1, int(heartbeat_every))

    # Safety: heartbeat must be < timeout
    if heartbeat_every >= base_timeout:
        heartbeat_every = max(1, base_timeout // 2)

    # One-shot mode
    if should_continue is None:
        try:
            change_visibility(sqs, queue_url, receipt_handle, base_timeout)
        except Exception as e:
            print(f"[extend_visibility] warning: {e}")
        return

    # Loop mode
    try:
        change_visibility(sqs, queue_url, receipt_handle, base_timeout)
        next_tick = time.monotonic() + heartbeat_every

        while should_continue():
            jitter = heartbeat_every * random.uniform(-0.10, 0.10)
            interval = max(1.0, heartbeat_every + jitter)

            sleep_for = max(0.0, next_tick - time.monotonic())
            if sleep_for > 0:
                time.sleep(min(sleep_for, interval))

            try:
                change_visibility(sqs, queue_url, receipt_handle, base_timeout)
            except ClientError as e:
                if _err_code(e, "ReceiptHandleIsInvalid") or _err_code(e, "InvalidParameterValue"):
                    break
                print(f"[extend_visibility] warn: {e}")
            except Exception as e:
                print(f"[extend_visibility] warn: {e}")

            next_tick += interval

    except Exception as e:
        print(f"[extend_visibility] warning: {e}")


@contextmanager
def visibility_heartbeat(
    sqs,
    queue_url: str,
    receipt_handle: str,
    base_timeout: int = 60,
    heartbeat_every: int = 30,
):
    """
    Context manager to automatically maintain visibility during a block.

    Example:
        with visibility_heartbeat(sqs, url, receipt, base_timeout=120, heartbeat_every=45):
            process(task)
    """
    stop = {"v": False}

    def cond() -> bool:
        return not stop["v"]

    import threading
    t = threading.Thread(
        target=extend_visibility_loop,
        args=(sqs, queue_url, receipt_handle, base_timeout, heartbeat_every, cond),
        daemon=True,
    )
    t.start()
    try:
        yield
    finally:
        stop["v"] = True
        t.join(timeout=2)


# ============================================================================
# PUBLISHING (fan-out to next step)
# ============================================================================

def build_output_prefix(bucket: str, job_id: str, step: str, task_id: str) -> str:
    """
    Build canonical output_prefix: s3://<bucket>/work/<job_id>/<step>/<task_id>/
    Validates bucket, job_id, task_id format and step enum. Normalizes step to uppercase.
    """
    if not all([bucket, job_id, step, task_id]):
        raise ValueError("All parameters required: bucket, job_id, step, task_id")
    
    bucket = bucket.strip().strip('/')
    job_id = job_id.strip().strip('/')
    step = step.strip().strip('/').upper()
    task_id = task_id.strip().strip('/')
    
    # Validate bucket (S3 rules: 3-63 chars, lowercase, digits, dots, hyphens)
    if not re.match(r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$', bucket):
        raise ValueError(f"Invalid bucket name: {bucket}")
    
    # Validate job_id and task_id
    if not re.match(r'^[a-zA-Z0-9_-]{1,128}$', job_id):
        raise ValueError(f"Invalid job_id: {job_id}")
    if not re.match(r'^[a-zA-Z0-9_-]{1,256}$', task_id):
        raise ValueError(f"Invalid task_id: {task_id}")
    
    # Validate step against enum
    if step not in VALID_STEPS:
        raise ValueError(f"Invalid step: {step}. Must be one of {VALID_STEPS}")
    
    # Block path traversal
    if '..' in bucket or '..' in job_id or '..' in step or '..' in task_id:
        raise ValueError("Path traversal not allowed")
    
    return f"s3://{bucket}/work/{job_id}/{step}/{task_id}/"


def message_to_dict(message: TaskMessage, preserve_unknown: bool = True) -> TaskDict:
    """Convert TaskMessage to dict (strict JSON validation)."""
    if not getattr(message, "job_id", None):
        raise ValueError("job_id missing")
    if not getattr(message, "task_id", None):
        raise ValueError("task_id missing")
    if not getattr(message, "step", None):
        raise ValueError("step missing")
    if not getattr(message, "input_uri", None):
        raise ValueError("input_uri missing")
    if not getattr(message, "output_prefix", None):
        raise ValueError("output_prefix missing")

    step = str(message.step).upper().strip()
    params = dict(getattr(message, "params", {}) or {})
    retry_count = int(getattr(message, "retry_count", 0) or 0)

    result: Dict[str, Any] = {
        "job_id": str(message.job_id),
        "task_id": str(message.task_id),
        "user_id": getattr(message, "user_id", None),
        "schema": getattr(message, "schema", "v1"),
        "step": step,
        "input_uri": str(message.input_uri),
        "output_prefix": str(message.output_prefix),
        "params": params,
        "retry_count": retry_count,
    }

    trace_id = getattr(message, "trace_id", None)
    if trace_id is not None:
        result["trace_id"] = str(trace_id)

    parent_task_id = getattr(message, "parent_task_id", None)
    if parent_task_id is not None:
        result["parent_task_id"] = str(parent_task_id)

    if preserve_unknown and hasattr(message, "extra") and isinstance(message.extra, dict):
        for k, v in message.extra.items():
            if k not in result:
                result[k] = v

    # Strict JSON validation (no default=str)
    json.dumps(result)
    return result


def send_message(
    sqs,
    queue_url: str,
    payload: TaskMessageLike,
    *,
    delay_seconds: int = 0,
    fifo_group_id: Optional[str] = None,
    fifo_dedup_id: Optional[str] = None,
) -> str:
    """
    Publish a single message with retry/backoff.
    For FIFO queues, fifo_group_id is required (MessageGroupId).
    """
    if not isinstance(payload, dict):
        payload = message_to_dict(payload)

    is_fifo = _is_fifo_queue(queue_url)
    if is_fifo and not fifo_group_id:
        raise ValueError(f"FIFO queue requires fifo_group_id: {queue_url}")

    body = json.dumps(payload)
    _ensure_sqs_size_ok(body)

    params = {
        "QueueUrl": queue_url,
        "MessageBody": body,
        "DelaySeconds": max(0, min(int(delay_seconds), 900)),
    }
    if fifo_group_id:
        params["MessageGroupId"] = fifo_group_id
    if fifo_dedup_id:
        params["MessageDeduplicationId"] = fifo_dedup_id
    elif is_fifo and "task_id" in payload:
        params["MessageDeduplicationId"] = str(payload["task_id"])[:128]

    resp = _retry_with_backoff(sqs.send_message, **params)
    return resp.get("MessageId", "")


def send_messages_batch(
    sqs,
    queue_url: str,
    payloads: List[TaskMessageLike],
    *,
    fifo_group_id: Optional[str] = None,
    dedup_from_task_id: bool = True,
) -> List[str]:
    """
    Publish many messages (batches of 10) with retry on failures.
    For FIFO queues, fifo_group_id is required.
    Fresh Entry Ids are generated on each retry for failed records.
    """
    if not payloads:
        return []

    is_fifo = _is_fifo_queue(queue_url)
    if is_fifo and not fifo_group_id:
        raise ValueError(f"FIFO queue requires fifo_group_id: {queue_url}")

    # Normalize + build all entries
    normalized: List[Dict[str, Any]] = []
    for p in payloads:
        d = p if isinstance(p, dict) else message_to_dict(p)
        # Size guard now so we fail fast before batch call
        body = json.dumps(d)
        _ensure_sqs_size_ok(body)
        normalized.append({"payload": d, "body": body})

    message_ids: List[str] = []

    for start in range(0, len(normalized), 10):
        chunk = normalized[start:start + 10]

        def build_entries(seed: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
            entries: List[Dict[str, Any]] = []
            if seed is None:
                # first attempt
                for idx, item in enumerate(chunk):
                    d = item["payload"]
                    entry: Dict[str, Any] = {
                        "Id": f"m{start + idx}",
                        "MessageBody": item["body"],
                    }
                    if fifo_group_id:
                        entry["MessageGroupId"] = fifo_group_id
                        if dedup_from_task_id and "task_id" in d:
                            entry["MessageDeduplicationId"] = str(d["task_id"])[:128]
                    entries.append(entry)
            else:
                # rebuild entries for retry with fresh Ids
                for e in seed:
                    fresh = dict(e)
                    fresh["Id"] = f"r{uuid.uuid4().hex[:8]}"
                    entries.append(fresh)
            return entries

        failed_entries = build_entries()  # initial batch

        delay = 0.25
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=failed_entries)

                # Collect successes
                for s in resp.get("Successful", []):
                    mid = s.get("MessageId")
                    if mid:
                        message_ids.append(mid)

                failures = resp.get("Failed", [])
                if not failures:
                    break  # done with this chunk

                # Determine retriable failures
                failed_ids = {f.get("Id") for f in failures}
                retriable: List[Dict[str, Any]] = []
                for f in failures:
                    code = f.get("Code", "")
                    # Retriable?
                    if code in RETRIABLE_ERROR_CODES and attempt < RETRY_ATTEMPTS:
                        # find original entry by Id
                        orig = next((e for e in failed_entries if e["Id"] == f["Id"]), None)
                        if orig:
                            retriable.append(orig)
                    else:
                        print(f"[send_batch] permanent failure for {f.get('Id')}: {f.get('Message')} (code={code})")

                if not retriable:
                    break

                # Rebuild with fresh Ids and back off
                failed_entries = build_entries(retriable)
                time.sleep(delay + random.uniform(0, 0.25))
                delay = min(delay * 2, 5.0)

            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in RETRIABLE_ERROR_CODES and attempt < RETRY_ATTEMPTS:
                    time.sleep(delay + random.uniform(0, 0.25))
                    delay = min(delay * 2, 5.0)
                    continue
                raise

    return message_ids


def requeue_with_delay(
    sqs,
    queue_url: str,
    payload: TaskMessageLike,
    *,
    delay_seconds: int,
    fifo_group_id: Optional[str] = None,
    fifo_dedup_id: Optional[str] = None,
) -> str:
    """
    Re-publish task with delay (soft backoff). Increments retry_count.
    NOTE: Callers should delete the original message to avoid duplicates,
          unless intentionally relying on SQS redelivery.
    """
    if not isinstance(payload, dict):
        payload = message_to_dict(payload)

    rc = int(payload.get("retry_count", 0) or 0) + 1
    payload = {**payload, "retry_count": rc}

    return send_message(
        sqs,
        queue_url,
        payload,
        delay_seconds=delay_seconds,
        fifo_group_id=fifo_group_id,
        fifo_dedup_id=fifo_dedup_id,
    )


def move_to_dlq(
    sqs,
    dlq_url: str,
    payload: TaskMessageLike,
    *,
    reason: Optional[str] = None,
) -> str:
    """
    Move poison message to DLQ.
    POLICY: Use this for hard-fail-now cases.
    Normal poison handling should rely on the SQS redrive policy.
    Always delete_message(...) after calling this to stop hot-looping.
    """
    if not isinstance(payload, dict):
        payload = message_to_dict(payload)

    dlq_body = {
        "reason": reason or "poison-pill",
        "retry_count": int(payload.get("retry_count", 0) or 0),
        "original": payload,
    }
    return send_message(sqs, dlq_url, dlq_body)


# ============================================================================
# OPERATIONAL HELPERS
# ============================================================================

def get_queue_stats(
    sqs,
    queue_url: str,
    include_extra: bool = False,
) -> Dict[str, Any]:
    """
    Get approximate queue counts. Used for metrics/dashboards.
    """
    base_attrs = [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ]
    extra_attrs = [
        "VisibilityTimeout",
        "MessageRetentionPeriod",
        "CreatedTimestamp",
        "LastModifiedTimestamp",
        "QueueArn",
    ]
    attr_names = base_attrs + (extra_attrs if include_extra else [])

    resp = _retry_with_backoff(sqs.get_queue_attributes, QueueUrl=queue_url, AttributeNames=attr_names)
    attrs = resp.get("Attributes", {})

    out = {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
        "inflight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
    }

    if include_extra:
        for k in ("VisibilityTimeout", "MessageRetentionPeriod", "CreatedTimestamp", "LastModifiedTimestamp"):
            if k in attrs:
                out[k.lower()] = int(attrs[k])
        if "QueueArn" in attrs:
            out["queue_arn"] = attrs["QueueArn"]

    return out
