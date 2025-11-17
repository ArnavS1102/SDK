"""
SQS queue operations.

Modular design:
- SQSClient class: encapsulates all SQS operations with retries/FIFO handling
- Module-level functions: backward-compatible functional API
- Easy to test: inject custom SQSClient instances
- Easy to extend: subclass SQSClient for LocalQueue/test implementations
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
from .logging import get_logger


# ============================================================================
# TYPES & CONSTANTS
# ============================================================================

RawMessage = Dict[str, Any]
TaskDict = Dict[str, Any]
TaskMessageLike = Union[Dict[str, Any], TaskMessage]

MAX_RECEIVE_COUNT = 5  # Poison-pill threshold
RETRY_ATTEMPTS = 5
RETRIABLE_ERROR_CODES = {
    "Throttling", "ThrottlingException", "ServiceUnavailable",
    "RequestThrottled", "InternalError", "InternalFailure",
    "ProvisionedThroughputExceededException", "RequestTimeout",
    "500", "502", "503", "504",
}
SQS_MAX_BODY_BYTES = 256 * 1024
SQS_MAX_VISIBILITY = 43_200  # 12h hard SQS limit


# ============================================================================
# SQS CLIENT CLASS (core implementation)
# ============================================================================

class SQSClient:
    """
    Encapsulates all SQS operations with retry logic, FIFO handling, and poison-pill protection.
    
    Benefits:
    - Easy to test: mock/stub the class or inject a test instance
    - Easy to configure: pass boto3 client or config at init
    - Easy to extend: subclass for local queue or test implementations
    """
    
    def __init__(self, sqs_client=None, region: Optional[str] = None, max_retries: int = 5, logger=None):
        """
        Initialize SQS client adapter.
        
        Args:
            sqs_client: boto3 SQS client (if None, creates default)
            region: AWS region (used if creating default client)
            max_retries: Number of retry attempts for transient errors
            logger: StructuredLogger instance (if None, creates default)
        """
        self._sqs = sqs_client
        self._region = region
        self.max_retries = max_retries
        self.logger = logger or get_logger("io_sqs")
    
    @property
    def sqs(self):
        """Lazy-load SQS client with long-polling config."""
        if self._sqs is None:
            self._sqs = boto3.client(
                "sqs",
                region_name=self._region,
                config=Config(
                    retries={"max_attempts": 6},
                    read_timeout=70,     # > 20s long-poll
                    connect_timeout=3,
                ),
            )
        return self._sqs
    
    # ------------------------------------------------------------------------
    # RECEIVING & PARSING
    # ------------------------------------------------------------------------
    
    def receive_messages(
        self,
        queue_url: str,
        max_messages: int = 1,
        wait_seconds: int = 20,
        visibility_timeout: Optional[int] = None,
    ) -> List[RawMessage]:
        """Long-poll SQS queue and return up to max_messages (1-10)."""
        max_n = max(1, min(int(max_messages), 10))
        wait_s = max(0, min(int(wait_seconds), 20))
        self.logger.debug("Receiving messages", {"queue_url": queue_url, "max_messages": max_n})
        
        params = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": max_n,
            "WaitTimeSeconds": wait_s,
            "MessageAttributeNames": ["All"],
            "AttributeNames": ["ApproximateReceiveCount", "SentTimestamp"],
            "ReceiveRequestAttemptId": uuid.uuid4().hex,
        }
        if visibility_timeout is not None:
            params["VisibilityTimeout"] = int(visibility_timeout)
        
        resp = self._retry(self.sqs.receive_message, **params)
        messages = resp.get("Messages", [])
        if messages:
            self.logger.info(f"Received {len(messages)} message(s)", {"queue_url": queue_url})
        return messages
    
    def parse_message(
        self,
        raw_msg: RawMessage,
        *,
        allow_sns_envelope: bool = True
    ) -> Tuple[TaskDict, str, int]:
        """Parse SQS message. Returns (task_dict, receipt_handle, receive_count)."""
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
        
        # Unwrap SNS envelope
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
    
    @staticmethod
    def is_poison_pill(receive_count: int, max_count: int = MAX_RECEIVE_COUNT) -> bool:
        """Check if message has exceeded max receive attempts."""
        return receive_count >= max_count
    
    # ------------------------------------------------------------------------
    # ACKNOWLEDGEMENT & VISIBILITY
    # ------------------------------------------------------------------------
    
    def delete_message(self, queue_url: str, receipt_handle: str) -> None:
        """ACK message: permanently remove from queue."""
        if not isinstance(receipt_handle, str) or not receipt_handle.strip():
            raise ValueError("delete_message: receipt_handle required")
        
        self._retry(
            self.sqs.delete_message,
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )
    
    def change_visibility(
        self,
        queue_url: str,
        receipt_handle: str,
        visibility_timeout: int,
    ) -> None:
        """Extend (or shorten) message invisibility."""
        if not isinstance(receipt_handle, str) or not receipt_handle.strip():
            raise ValueError("change_visibility: receipt_handle required")
        if not isinstance(visibility_timeout, int) or visibility_timeout < 0:
            raise ValueError("change_visibility: timeout must be non-negative int")
        
        visibility_timeout = min(visibility_timeout, SQS_MAX_VISIBILITY)
        self._retry(
            self.sqs.change_message_visibility,
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=visibility_timeout,
        )
    
    def extend_visibility_loop(
        self,
        queue_url: str,
        receipt_handle: str,
        base_timeout: int = 60,
        heartbeat_every: int = 30,
        should_continue: Optional[Callable[[], bool]] = None,
    ) -> None:
        """Background heartbeat to extend visibility during long processing."""
        if not isinstance(receipt_handle, str) or not receipt_handle.strip():
            raise ValueError("extend_visibility_loop: receipt_handle required")
        
        base_timeout = max(1, min(int(base_timeout), SQS_MAX_VISIBILITY))
        heartbeat_every = max(1, int(heartbeat_every))
        
        if heartbeat_every >= base_timeout:
            heartbeat_every = max(1, base_timeout // 2)
        
        # One-shot mode
        if should_continue is None:
            try:
                self.change_visibility(queue_url, receipt_handle, base_timeout)
            except Exception as e:
                self.logger.warning("One-shot visibility extension failed", {"error": str(e)})
            return
        
        # Loop mode
        try:
            self.change_visibility(queue_url, receipt_handle, base_timeout)
            next_tick = time.monotonic() + heartbeat_every
            
            while should_continue():
                jitter = heartbeat_every * random.uniform(-0.10, 0.10)
                interval = max(1.0, heartbeat_every + jitter)
                
                sleep_for = max(0.0, next_tick - time.monotonic())
                if sleep_for > 0:
                    time.sleep(min(sleep_for, interval))
                
                try:
                    self.change_visibility(queue_url, receipt_handle, base_timeout)
                except ClientError as e:
                    if self._is_receipt_invalid(e):
                        self.logger.debug("Receipt handle invalid (stopping heartbeat)")
                        break
                    self.logger.warning("Visibility extension error", {"error": str(e)})
                except Exception as e:
                    self.logger.warning("Visibility extension error", {"error": str(e)})
                
                next_tick += interval
        
        except Exception as e:
            self.logger.warning("Visibility heartbeat failed", {"error": str(e)})
    
    @contextmanager
    def visibility_heartbeat(
        self,
        queue_url: str,
        receipt_handle: str,
        base_timeout: int = 60,
        heartbeat_every: int = 30,
    ):
        """Context manager to automatically maintain visibility during a block."""
        stop = {"v": False}
        
        def cond() -> bool:
            return not stop["v"]
        
        import threading
        t = threading.Thread(
            target=self.extend_visibility_loop,
            args=(queue_url, receipt_handle, base_timeout, heartbeat_every, cond),
            daemon=True,
        )
        t.start()
        try:
            yield
        finally:
            stop["v"] = True
            t.join(timeout=2)
    
    # ------------------------------------------------------------------------
    # PUBLISHING
    # ------------------------------------------------------------------------
    
    def send_message(
        self,
        queue_url: str,
        payload: TaskMessageLike,
        *,
        delay_seconds: int = 0,
        fifo_group_id: Optional[str] = None,
        fifo_dedup_id: Optional[str] = None,
    ) -> str:
        """Publish a single message with retry/backoff."""
        if not isinstance(payload, dict):
            payload = self.message_to_dict(payload)
        
        is_fifo = self._is_fifo_queue(queue_url)
        if is_fifo and not fifo_group_id:
            raise ValueError(f"FIFO queue requires fifo_group_id: {queue_url}")
        
        self.logger.debug("Sending message", {
            "queue_url": queue_url,
            "task_id": payload.get("task_id"),
            "delay_seconds": delay_seconds
        })
        
        body = json.dumps(payload)
        self._ensure_size_ok(body)
        
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
        
        resp = self._retry(self.sqs.send_message, **params)
        message_id = resp.get("MessageId", "")
        self.logger.info("Sent message", {
            "queue_url": queue_url,
            "message_id": message_id,
            "task_id": payload.get("task_id")
        })
        return message_id
    
    def send_messages_batch(
        self,
        queue_url: str,
        payloads: List[TaskMessageLike],
        *,
        fifo_group_id: Optional[str] = None,
        dedup_from_task_id: bool = True,
    ) -> List[str]:
        """Publish many messages (batches of 10) with retry on failures."""
        if not payloads:
            return []
        
        self.logger.info(f"Sending {len(payloads)} messages in batch", {"queue_url": queue_url})
        
        is_fifo = self._is_fifo_queue(queue_url)
        if is_fifo and not fifo_group_id:
            raise ValueError(f"FIFO queue requires fifo_group_id: {queue_url}")
        
        # Normalize + validate
        normalized: List[Dict[str, Any]] = []
        for p in payloads:
            d = p if isinstance(p, dict) else self.message_to_dict(p)
            body = json.dumps(d)
            self._ensure_size_ok(body)
            normalized.append({"payload": d, "body": body})
        
        message_ids: List[str] = []
        
        for start in range(0, len(normalized), 10):
            chunk = normalized[start:start + 10]
            message_ids.extend(self._send_batch_chunk(
                queue_url, chunk, start, fifo_group_id, dedup_from_task_id
            ))
        
        return message_ids
    
    def requeue_with_delay(
        self,
        queue_url: str,
        payload: TaskMessageLike,
        *,
        delay_seconds: int,
        fifo_group_id: Optional[str] = None,
        fifo_dedup_id: Optional[str] = None,
    ) -> str:
        """Re-publish task with delay (soft backoff). Increments retry_count."""
        if not isinstance(payload, dict):
            payload = self.message_to_dict(payload)
        
        rc = int(payload.get("retry_count", 0) or 0) + 1
        payload = {**payload, "retry_count": rc}
        
        return self.send_message(
            queue_url, payload,
            delay_seconds=delay_seconds,
            fifo_group_id=fifo_group_id,
            fifo_dedup_id=fifo_dedup_id,
        )
    
    def move_to_dlq(
        self,
        dlq_url: str,
        payload: TaskMessageLike,
        *,
        reason: Optional[str] = None,
    ) -> str:
        """Move poison message to DLQ."""
        if not isinstance(payload, dict):
            payload = self.message_to_dict(payload)
        
        dlq_body = {
            "reason": reason or "poison-pill",
            "retry_count": int(payload.get("retry_count", 0) or 0),
            "original": payload,
        }
        return self.send_message(dlq_url, dlq_body)
    
    # ------------------------------------------------------------------------
    # OPERATIONAL HELPERS
    # ------------------------------------------------------------------------
    
    def get_queue_stats(
        self,
        queue_url: str,
        include_extra: bool = False,
    ) -> Dict[str, Any]:
        """Get approximate queue counts (for metrics/dashboards)."""
        base_attrs = [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ]
        extra_attrs = [
            "VisibilityTimeout", "MessageRetentionPeriod",
            "CreatedTimestamp", "LastModifiedTimestamp", "QueueArn",
        ]
        attr_names = base_attrs + (extra_attrs if include_extra else [])
        
        resp = self._retry(self.sqs.get_queue_attributes, QueueUrl=queue_url, AttributeNames=attr_names)
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
    
    # ------------------------------------------------------------------------
    # HELPERS (public utilities)
    # ------------------------------------------------------------------------
    
    @staticmethod
    def build_output_prefix(bucket: str, user_id: str, job_id: str, step: str, task_id: str) -> str:
        """Build canonical output_prefix: s3://<bucket>/<user_id>/<job_id>/<step>/<task_id>/"""
        if not all([bucket, user_id, job_id, step, task_id]):
            raise ValueError("All parameters required: bucket, user_id, job_id, step, task_id")
        
        bucket = bucket.strip().strip('/')
        user_id = user_id.strip().strip('/')
        job_id = job_id.strip().strip('/')
        step = step.strip().strip('/').upper()
        task_id = task_id.strip().strip('/')
        
        if not re.match(r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$', bucket):
            raise ValueError(f"Invalid bucket name: {bucket}")
        if not re.match(r'^[a-zA-Z0-9_-]{1,128}$', user_id):
            raise ValueError(f"Invalid user_id: {user_id}")
        
        if not re.match(r'^[a-zA-Z0-9_-]{1,128}$', job_id):
            raise ValueError(f"Invalid job_id: {job_id}")
        if not re.match(r'^[a-zA-Z0-9_-]{1,256}$', task_id):
            raise ValueError(f"Invalid task_id: {task_id}")
        
        if step not in VALID_STEPS:
            raise ValueError(f"Invalid step: {step}. Must be one of {VALID_STEPS}")
        
        if '..' in bucket or '..' in user_id or '..' in job_id or '..' in step or '..' in task_id:
            raise ValueError("Path traversal not allowed")
        
        return f"s3://{bucket}/{user_id}/{job_id}/{step}/{task_id}/"
    
    @staticmethod
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
        
        # Strict JSON validation
        json.dumps(result)
        return result
    
    # ------------------------------------------------------------------------
    # PRIVATE HELPERS
    # ------------------------------------------------------------------------
    
    def _send_batch_chunk(
        self,
        queue_url: str,
        chunk: List[Dict[str, Any]],
        start_idx: int,
        fifo_group_id: Optional[str],
        dedup_from_task_id: bool,
    ) -> List[str]:
        """Send a single batch chunk (up to 10 messages) with retry logic."""
        
        def build_entries(seed: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
            entries: List[Dict[str, Any]] = []
            if seed is None:
                for idx, item in enumerate(chunk):
                    d = item["payload"]
                    entry: Dict[str, Any] = {
                        "Id": f"m{start_idx + idx}",
                        "MessageBody": item["body"],
                    }
                    if fifo_group_id:
                        entry["MessageGroupId"] = fifo_group_id
                        if dedup_from_task_id and "task_id" in d:
                            entry["MessageDeduplicationId"] = str(d["task_id"])[:128]
                    entries.append(entry)
            else:
                for e in seed:
                    fresh = dict(e)
                    fresh["Id"] = f"r{uuid.uuid4().hex[:8]}"
                    entries.append(fresh)
            return entries
        
        failed_entries = build_entries()
        message_ids: List[str] = []
        delay = 0.25
        
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.sqs.send_message_batch(QueueUrl=queue_url, Entries=failed_entries)
                
                for s in resp.get("Successful", []):
                    mid = s.get("MessageId")
                    if mid:
                        message_ids.append(mid)
                
                failures = resp.get("Failed", [])
                if not failures:
                    break
                
                # Determine retriable failures
                retriable: List[Dict[str, Any]] = []
                for f in failures:
                    code = f.get("Code", "")
                    if code in RETRIABLE_ERROR_CODES and attempt < self.max_retries:
                        orig = next((e for e in failed_entries if e["Id"] == f["Id"]), None)
                        if orig:
                            retriable.append(orig)
                    else:
                        self.logger.error("Permanent batch send failure", {
                            "entry_id": f.get('Id'),
                            "message": f.get('Message'),
                            "code": code
                        })
                
                if not retriable:
                    break
                
                failed_entries = build_entries(retriable)
                time.sleep(delay + random.uniform(0, 0.25))
                delay = min(delay * 2, 5.0)
            
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in RETRIABLE_ERROR_CODES and attempt < self.max_retries:
                    time.sleep(delay + random.uniform(0, 0.25))
                    delay = min(delay * 2, 5.0)
                    continue
                raise
        
        return message_ids
    
    def _retry(self, func: Callable, *args, **kwargs):
        """Retry a boto3 call with exponential backoff on retriable errors."""
        delay = 0.25
        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in RETRIABLE_ERROR_CODES and attempt < self.max_retries:
                    time.sleep(delay + random.uniform(0, 0.25))
                    delay = min(delay * 2, 5.0)
                    continue
                raise
            except BotoCoreError:
                if attempt < self.max_retries:
                    time.sleep(delay + random.uniform(0, 0.25))
                    delay = min(delay * 2, 5.0)
                    continue
                raise
        raise RuntimeError(f"Failed after {self.max_retries} attempts")
    
    @staticmethod
    def _is_fifo_queue(queue_url: str) -> bool:
        """Detect FIFO queue by URL suffix."""
        return queue_url.lower().endswith(".fifo")
    
    @staticmethod
    def _ensure_size_ok(body: str) -> None:
        """Guard against SQS 256 KB hard limit."""
        if len(body.encode("utf-8")) > SQS_MAX_BODY_BYTES:
            raise ValueError("SQS message > 256KB; upload payload to S3 and send a pointer.")
    
    @staticmethod
    def _is_receipt_invalid(error: ClientError) -> bool:
        """Check if error indicates invalid receipt handle."""
        code = error.response.get("Error", {}).get("Code", "")
        return code in ("ReceiptHandleIsInvalid", "InvalidParameterValue")


# ============================================================================
# MODULE-LEVEL API (backward compatibility)
# ============================================================================

# Default client instance (lazy-initialized)
_default_client: Optional[SQSClient] = None

def _get_client() -> SQSClient:
    """Get or create default SQS client instance."""
    global _default_client
    if _default_client is None:
        _default_client = SQSClient()
    return _default_client


# Expose functional API for backward compatibility
def get_sqs_client(region: Optional[str] = None):
    """Create SQS client (one per process). Tuned for long-polling."""
    return boto3.client(
        "sqs",
        region_name=region,
        config=Config(
            retries={"max_attempts": 6},
            read_timeout=70,
            connect_timeout=3,
        ),
    )

def receive_messages(
    sqs,
    queue_url: str,
    max_messages: int = 1,
    wait_seconds: int = 20,
    visibility_timeout: Optional[int] = None,
) -> List[RawMessage]:
    """Long-poll SQS queue."""
    client = SQSClient(sqs_client=sqs)
    return client.receive_messages(queue_url, max_messages, wait_seconds, visibility_timeout)

def parse_message(
    raw_msg: RawMessage,
    *,
    allow_sns_envelope: bool = True
) -> Tuple[TaskDict, str, int]:
    """Parse SQS message."""
    return _get_client().parse_message(raw_msg, allow_sns_envelope=allow_sns_envelope)

def is_poison_pill(receive_count: int, max_count: int = MAX_RECEIVE_COUNT) -> bool:
    """Check if message is a poison pill."""
    return SQSClient.is_poison_pill(receive_count, max_count)

def delete_message(sqs, queue_url: str, receipt_handle: str) -> None:
    """ACK message."""
    client = SQSClient(sqs_client=sqs)
    client.delete_message(queue_url, receipt_handle)

def change_visibility(sqs, queue_url: str, receipt_handle: str, visibility_timeout: int) -> None:
    """Extend/shorten message invisibility."""
    client = SQSClient(sqs_client=sqs)
    client.change_visibility(queue_url, receipt_handle, visibility_timeout)

def extend_visibility_loop(
    sqs, queue_url: str, receipt_handle: str,
    base_timeout: int = 60, heartbeat_every: int = 30,
    should_continue: Optional[Callable[[], bool]] = None,
) -> None:
    """Background heartbeat to extend visibility."""
    client = SQSClient(sqs_client=sqs)
    client.extend_visibility_loop(queue_url, receipt_handle, base_timeout, heartbeat_every, should_continue)

@contextmanager
def visibility_heartbeat(
    sqs, queue_url: str, receipt_handle: str,
    base_timeout: int = 60, heartbeat_every: int = 30,
):
    """Context manager for visibility heartbeat."""
    client = SQSClient(sqs_client=sqs)
    with client.visibility_heartbeat(queue_url, receipt_handle, base_timeout, heartbeat_every):
        yield

def send_message(
    sqs, queue_url: str, payload: TaskMessageLike,
    *, delay_seconds: int = 0,
    fifo_group_id: Optional[str] = None,
    fifo_dedup_id: Optional[str] = None,
) -> str:
    """Publish a single message."""
    client = SQSClient(sqs_client=sqs)
    return client.send_message(queue_url, payload, delay_seconds=delay_seconds,
                               fifo_group_id=fifo_group_id, fifo_dedup_id=fifo_dedup_id)

def send_messages_batch(
    sqs, queue_url: str, payloads: List[TaskMessageLike],
    *, fifo_group_id: Optional[str] = None, dedup_from_task_id: bool = True,
) -> List[str]:
    """Publish many messages in batches."""
    client = SQSClient(sqs_client=sqs)
    return client.send_messages_batch(queue_url, payloads, fifo_group_id=fifo_group_id,
                                      dedup_from_task_id=dedup_from_task_id)

def requeue_with_delay(
    sqs, queue_url: str, payload: TaskMessageLike,
    *, delay_seconds: int,
    fifo_group_id: Optional[str] = None,
    fifo_dedup_id: Optional[str] = None,
) -> str:
    """Re-publish task with delay."""
    client = SQSClient(sqs_client=sqs)
    return client.requeue_with_delay(queue_url, payload, delay_seconds=delay_seconds,
                                     fifo_group_id=fifo_group_id, fifo_dedup_id=fifo_dedup_id)

def move_to_dlq(sqs, dlq_url: str, payload: TaskMessageLike, *, reason: Optional[str] = None) -> str:
    """Move poison message to DLQ."""
    client = SQSClient(sqs_client=sqs)
    return client.move_to_dlq(dlq_url, payload, reason=reason)

def get_queue_stats(sqs, queue_url: str, include_extra: bool = False) -> Dict[str, Any]:
    """Get approximate queue counts."""
    client = SQSClient(sqs_client=sqs)
    return client.get_queue_stats(queue_url, include_extra)

def build_output_prefix(bucket: str, user_id: str, job_id: str, step: str, task_id: str) -> str:
    """Build canonical output_prefix."""
    return SQSClient.build_output_prefix(bucket, user_id, job_id, step, task_id)

def message_to_dict(message: TaskMessage, preserve_unknown: bool = True) -> TaskDict:
    """Convert TaskMessage to dict."""
    return SQSClient.message_to_dict(message, preserve_unknown)


# Public API
__all__ = [
    "SQSClient",
    "get_sqs_client", "receive_messages", "parse_message", "is_poison_pill",
    "delete_message", "change_visibility", "extend_visibility_loop", "visibility_heartbeat",
    "send_message", "send_messages_batch", "requeue_with_delay", "move_to_dlq",
    "get_queue_stats", "build_output_prefix", "message_to_dict",
]
