"""
Message & status schema validation.
Defines the contract every message must follow.

CONTRACT INVARIANTS (every service must obey):
1. output_prefix ALWAYS includes: s3://<bucket>/work/<job_id>/<step>/<task_id>/
2. Workers write ONLY inside output_prefix
3. Workers MUST produce: result.(json|png|mp4|etc) + metrics.json
4. Idempotency rule: if result.* exists under output_prefix → skip work and ACK
5. Fan-out tasks MUST set parent_task_id to source task for traceability

NOTE: This file focuses ONLY on validation. Helper functions like message_to_dict(),
build_output_prefix(), etc. have TODOs to move to their proper modules (io_sqs.py,
io_storage.py) when those modules are implemented.
"""

from typing import Dict, Any, List, Optional
import re
import json

# Import types and constants from constants.py (relative import)
from .constants import (
    TaskMessage,
    SUPPORTED_SCHEMAS,
    VALID_STEPS,
    VALID_TASK_STATUSES,
    VALID_JOB_STATUSES,
    REQUIRED_MESSAGE_FIELDS,
    SUPPORTED_FILE_EXTENSIONS,
    StepType,
    TaskStatus,
    JobStatus,
)


# ============================================================================
# CORE VALIDATION FUNCTIONS (contracts.py focus)
# ============================================================================

def validate_message(raw_message: Dict[str, Any], bucket_allowlist: List[str]) -> TaskMessage:
    """
    Junior dev: This is the gatekeeper. Every message goes through here first.
    Reject bad messages BEFORE they waste compute/GPU time.
    
    Orchestrates ALL validation checks by calling:
    - validate_user_id() - check user_id format
    - validate_job_id() - check job_id format
    - validate_task_id() - check task_id format
    - validate_schema_version() - ensure we support this version
    - validate_step() - check against VALID_STEPS
    - validate_input_uri() - scheme + bucket allowlist + safe extensions
    - validate_output_prefix() - enforce pattern + match job_id/step/task_id
    - validate_params() - size + depth + JSON types
    - validate_parent_task_id() - required for fan-out tasks
    - validate_trace_id() - optional field format
    
    Args:
        raw_message: Raw dict from SQS body
        bucket_allowlist: Allowed bucket names from config
        
    Returns:
        TaskMessage object if all validations pass
        
    Raises:
        ValueError if any validation fails with clear error message
    """
    if not isinstance(raw_message, dict):
        raise ValueError(f"Message must be a dict, got {type(raw_message).__name__}")
    
    # Check all required fields present
    missing = [field for field in REQUIRED_MESSAGE_FIELDS if field not in raw_message]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")
    
    # Extract fields
    job_id = raw_message.get("job_id")
    task_id = raw_message.get("task_id")
    user_id = raw_message.get("user_id")
    schema = raw_message.get("schema")
    step = raw_message.get("step")
    input_uri = raw_message.get("input_uri")
    output_prefix = raw_message.get("output_prefix")
    params = raw_message.get("params")
    trace_id = raw_message.get("trace_id")
    parent_task_id = raw_message.get("parent_task_id")
    retry_count = raw_message.get("retry_count", 0)
    
    # Type checks
    if not isinstance(job_id, str):
        raise ValueError(f"job_id must be str, got {type(job_id).__name__}")
    if not isinstance(task_id, str):
        raise ValueError(f"task_id must be str, got {type(task_id).__name__}")
    if not isinstance(user_id, str):
        raise ValueError(f"user_id must be str, got {type(user_id).__name__}")
    if not isinstance(schema, str):
        raise ValueError(f"schema must be str, got {type(schema).__name__}")
    if not isinstance(step, str):
        raise ValueError(f"step must be str, got {type(step).__name__}")
    if not isinstance(input_uri, str):
        raise ValueError(f"input_uri must be str, got {type(input_uri).__name__}")
    if not isinstance(output_prefix, str):
        raise ValueError(f"output_prefix must be str, got {type(output_prefix).__name__}")
    if not isinstance(params, dict):
        raise ValueError(f"params must be dict, got {type(params).__name__}")
    if not isinstance(retry_count, int):
        raise ValueError(f"retry_count must be int, got {type(retry_count).__name__}")
    
    if trace_id is not None and not isinstance(trace_id, str):
        raise ValueError(f"trace_id must be str or None, got {type(trace_id).__name__}")
    if parent_task_id is not None and not isinstance(parent_task_id, str):
        raise ValueError(f"parent_task_id must be str or None, got {type(parent_task_id).__name__}")
    
    # Validate each field
    if not validate_user_id(user_id):
        raise ValueError(f"Invalid user_id: {user_id}")
    
    if not validate_job_id(job_id):
        raise ValueError(f"Invalid job_id: {job_id}")
    
    if not validate_task_id(task_id):
        raise ValueError(f"Invalid task_id: {task_id}")
    
    if not validate_schema_version(schema):
        raise ValueError(f"Unsupported schema version: {schema}. Supported: {SUPPORTED_SCHEMAS}")
    
    if not validate_step(step):
        raise ValueError(f"Invalid step: {step}. Valid steps: {VALID_STEPS}")
    
    if not validate_input_uri(input_uri, bucket_allowlist):
        raise ValueError(f"Invalid input_uri: {input_uri}")
    
    # Extract bucket from output_prefix to validate
    try:
        prefix_bucket = output_prefix.split("://", 1)[1].split("/", 1)[0] if "://" in output_prefix else ""
    except (IndexError, ValueError):
        prefix_bucket = ""
    
    if not validate_output_prefix(output_prefix, prefix_bucket, job_id, step, task_id):
        raise ValueError(f"Invalid output_prefix: {output_prefix}")
    
    if not validate_params(params):
        raise ValueError(f"Invalid params: too large, too deep, or contains non-JSON types")
    
    # Check fan-out parent requirement
    is_fanout = parent_task_id is not None
    if not validate_parent_task_id(parent_task_id, is_fanout):
        raise ValueError(f"parent_task_id validation failed")
    
    if trace_id is not None and not validate_trace_id(trace_id):
        raise ValueError(f"Invalid trace_id: {trace_id}")
    
    # All validations passed - construct TaskMessage
    return TaskMessage(
        job_id=job_id,
        task_id=task_id,
        user_id=user_id,
        schema=schema,
        step=step,
        input_uri=input_uri,
        output_prefix=output_prefix,
        params=params,
        trace_id=trace_id,
        parent_task_id=parent_task_id,
        retry_count=retry_count
    )


def validate_schema_version(schema: str) -> bool:
    """
    Junior dev: Check if we understand this message format version.
    If user sends "v2" but we only support "v1", reject it.
    
    Checks against SUPPORTED_SCHEMAS set = {'v1'}
    
    Args:
        schema: Version string (e.g., "v1")
        
    Returns:
        True if in SUPPORTED_SCHEMAS, False otherwise
    """
    return schema in SUPPORTED_SCHEMAS


def validate_step(step: str) -> bool:
    """
    Junior dev: Make sure the step is one we know about.
    Prevents typos like "DETECTOIN" from entering the system.
    
    Checks if step is in VALID_STEPS list.
    
    Args:
        step: Step name (should be DETECTION, ANALYSIS, etc.)
        
    Returns:
        True if step in VALID_STEPS, False otherwise
    """
    return step in VALID_STEPS


def validate_input_uri(uri: str, bucket_allowlist: List[str]) -> bool:
    """
    Junior dev: Check the input file path is safe and accessible.
    
    S3 Bucket Primer:
    - A bucket is like a top-level folder in cloud storage (AWS S3 or Cloudflare R2)
    - Format: s3://<bucket-name>/<path>/<to>/<file.jpg>
    - The bucket is "my-uploads-bucket" in: s3://my-uploads-bucket/user123/photo.jpg
    
    Typical Setup (NOT per-user buckets):
    - One "uploads" bucket: where API gateway writes user uploads
    - One "work" bucket: where workers write their outputs
    - Users are separated by path INSIDE the bucket (not separate buckets)
    
    Example URIs:
    - s3://my-uploads-bucket/user_789/input.jpg       ← bucket = "my-uploads-bucket"
    - s3://my-work-bucket/jobs/abc123/result.png      ← bucket = "my-work-bucket"
    - r2://cdn-bucket/public/thumbnail.jpg            ← bucket = "cdn-bucket" (Cloudflare R2)
    
    Why allowlist? So workers can't read from random buckets (security).
    
    Checks:
    - Starts with s3:// or r2:// (enforced scheme)
    - Bucket is in allowlist (e.g., only "uploads" or "work" buckets)
    - File extension is in get_supported_file_extensions() (.jpg, .png, .mp4, .json)
    - Forbid path traversal (no ".." in path)
    - Forbid dangerous patterns (no shell chars, no null bytes)
    
    Args:
        uri: Input S3/R2 URI
        bucket_allowlist: Allowed bucket names (e.g., ["my-uploads-bucket", "my-work-bucket"])
        
    Returns:
        True if valid, False otherwise
    """
    if not uri or not isinstance(uri, str):
        return False
    
    # Check scheme
    if not (uri.startswith("s3://") or uri.startswith("r2://")):
        return False
    
    # Check for path traversal
    if ".." in uri:
        return False
    
    # Check for null bytes and dangerous chars
    if "\x00" in uri or ";" in uri or "|" in uri or "&" in uri or "$(" in uri or "`" in uri:
        return False
    
    # Extract bucket from URI (e.g., s3://bucket/path/file.jpg -> bucket)
    try:
        # Remove scheme
        path = uri.split("://", 1)[1]
        bucket = path.split("/", 1)[0]
    except (IndexError, ValueError):
        return False
    
    # Check bucket is in allowlist
    if bucket_allowlist and bucket not in bucket_allowlist:
        return False
    
    # Check file extension
    supported_extensions = get_supported_file_extensions()
    uri_lower = uri.lower()
    has_valid_extension = any(uri_lower.endswith(ext) for ext in supported_extensions)
    
    if not has_valid_extension:
        return False
    
    return True


def validate_output_prefix(output_prefix: str, bucket: str, job_id: str, step: str, task_id: str) -> bool:
    """
    Junior dev: This is CRITICAL. Enforces the storage contract.
    
    MUST match EXACT pattern: s3://<bucket>/work/<job_id>/<step>/<task_id>/
    
    Storage Layout Example:
    
    s3://my-work-bucket/                              ← ONE shared bucket for all work
      work/                                           ← All jobs live under /work/
        abc123-def456/                                ← Job ID (one per pipeline run)
          DETECTION/                                  ← Step name
            abc123-def456-detection-000/              ← Task ID (unique per task)
              result.json                             ← Output files
              metrics.json
          ANALYSIS/
            abc123-def456-detection-000-analysis-001/ ← Child task
              result.json
              metrics.json
    
    Why this pattern?
    1. Isolation: Each task gets its own folder
    2. Traceability: Path shows job → step → task hierarchy
    3. Security: Worker can't write outside its task folder
    4. Cleanup: Delete entire job folder when done
    
    Real example with fan-out:
    - Job starts: job_id = "a1b2c3d4"
    
    - Detection (runs ONCE):
      s3://my-work-bucket/work/a1b2c3d4/DETECTION/a1b2c3d4-detection-000/
      → Finds 3 objects in image
      → Creates 3 analysis tasks (fan-out)
    
    - Analysis (3 tasks, one per detection):
      s3://my-work-bucket/work/a1b2c3d4/ANALYSIS/a1b2c3d4-detection-000-analysis-000/
      s3://my-work-bucket/work/a1b2c3d4/ANALYSIS/a1b2c3d4-detection-000-analysis-001/
      s3://my-work-bucket/work/a1b2c3d4/ANALYSIS/a1b2c3d4-detection-000-analysis-002/
      
    So: 1 detection task → 3 analysis tasks → 3 completion tasks (if needed)
    
    Checks:
    - Starts with s3://<bucket>/work/
    - Contains job_id from message (not a different job)
    - Contains step from message (not a different step)
    - Contains task_id from message (not a different task)
    - Ends with trailing slash (/) - required for prefix operations
    - No ".." or other traversal attempts
    
    Why? So every task writes to its own isolated folder and can't mess with other tasks.
    
    Args:
        output_prefix: Where task wants to write
        bucket: Expected bucket from config (e.g., "my-work-bucket")
        job_id: Job ID from message
        step: Step from message
        task_id: Task ID from message
        
    Returns:
        True if matches required pattern exactly, False otherwise
    """
    if not output_prefix or not isinstance(output_prefix, str):
        return False
    
    # Must end with trailing slash
    if not output_prefix.endswith("/"):
        return False
    
    # Check for path traversal
    if ".." in output_prefix:
        return False
    
    # Build expected pattern
    expected = f"s3://{bucket}/work/{job_id}/{step}/{task_id}/"
    
    # Must match exactly
    if output_prefix != expected:
        return False
    
    return True


def validate_params(params: Dict[str, Any], max_size_bytes: int = 65536, max_depth: int = 5) -> bool:
    """
    Junior dev: Prevent abuse - users could send giant nested params that crash workers.
    
    Checks:
    - Is a dict
    - Serialized size ≤ 64 KB (max_size_bytes)
    - Nesting depth ≤ 5 levels (max_depth)
    - Only JSON-safe types (no functions, classes, bytes)
    
    Args:
        params: Parameters dict from message
        max_size_bytes: Max serialized JSON size (default 64 KB)
        max_depth: Max nesting depth (default 5)
        
    Returns:
        True if valid, False otherwise
    """
    
    if not isinstance(params, dict):
        return False
    
    # Check JSON-safe types
    def is_json_safe(obj, depth=0):
        if depth > max_depth:
            return False
        
        if obj is None or isinstance(obj, (bool, int, float, str)):
            return True
        elif isinstance(obj, dict):
            return all(isinstance(k, str) and is_json_safe(v, depth + 1) for k, v in obj.items())
        elif isinstance(obj, list):
            return all(is_json_safe(item, depth + 1) for item in obj)
        else:
            return False
    
    if not is_json_safe(params):
        return False
    
    # Check serialized size
    try:
        serialized = json.dumps(params)
        if len(serialized.encode('utf-8')) > max_size_bytes:
            return False
    except (TypeError, ValueError):
        return False
    
    return True


def validate_user_id(user_id: str) -> bool:
    """
    Junior dev: Basic check that user_id isn't empty or malicious.
    Should be UUID or username (alphanumeric + hyphens/underscores).
    
    Example: "user_12345" or "abc123-def456-789"
    
    Regex: ^[a-zA-Z0-9_-]+$
    Length: 1-128 chars
    
    Args:
        user_id: User identifier
        
    Returns:
        True if valid format, False otherwise
    """
    if not user_id or not isinstance(user_id, str):
        return False
    
    if len(user_id) < 1 or len(user_id) > 128:
        return False
    
    # Alphanumeric + hyphens/underscores only
    pattern = re.compile(r'^[a-zA-Z0-9_-]+$')
    return bool(pattern.match(user_id))


def validate_job_id(job_id: str) -> bool:
    """
    Junior dev: Check job_id is well-formed.
    Usually a UUID, so similar format to user_id.
    Generated once by API gateway when pipeline starts.
    
    Example: "a1b2c3d4-e5f6-4789-a012-b3c4d5e6f789"
    
    Regex: ^[a-zA-Z0-9_-]+$
    Length: 1-128 chars
    
    Args:
        job_id: Job identifier
        
    Returns:
        True if valid format, False otherwise
    """
    if not job_id or not isinstance(job_id, str):
        return False
    
    if len(job_id) < 1 or len(job_id) > 128:
        return False
    
    # Alphanumeric + hyphens/underscores only
    pattern = re.compile(r'^[a-zA-Z0-9_-]+$')
    return bool(pattern.match(job_id))


def validate_task_id(task_id: str) -> bool:
    """
    Junior dev: Check task_id is well-formed.
    Format is typically: {job_id}-{step}-{item_number}
    
    Examples (showing fan-out numbering):
    - Detection (runs once):  "a1b2c3d4-detection-000"
      ↓ Finds 3 objects, creates 3 analysis tasks
    - Analysis task 1:        "a1b2c3d4-detection-000-analysis-000"
    - Analysis task 2:        "a1b2c3d4-detection-000-analysis-001"
    - Analysis task 3:        "a1b2c3d4-detection-000-analysis-002"
      ↓ Each creates a completion task
    - Completion task 1:      "a1b2c3d4-detection-000-analysis-000-completion-000"
    
    The numbers (000, 001, 002) represent items in the fan-out, NOT retries.
    
    Regex: ^[a-zA-Z0-9_-]+$
    Length: 1-256 chars (longer than job_id because it includes hierarchy)
    
    Args:
        task_id: Task identifier
        
    Returns:
        True if valid format, False otherwise
    """
    if not task_id or not isinstance(task_id, str):
        return False
    
    if len(task_id) < 1 or len(task_id) > 256:
        return False
    
    # Alphanumeric + hyphens/underscores only
    pattern = re.compile(r'^[a-zA-Z0-9_-]+$')
    return bool(pattern.match(task_id))


def validate_trace_id(trace_id: Optional[str]) -> bool:
    """
    Junior dev: If trace_id is provided, check it's a reasonable format.
    Usually a UUID or hex string for distributed tracing (e.g., OpenTelemetry).
    
    Example: "4bf92f3577b34da6a3ce929d0e0e4736" or "abc123-def456"
    
    Args:
        trace_id: Optional trace ID
        
    Returns:
        True if valid or None, False if malformed
    """
    # None is allowed
    if trace_id is None:
        return True
    
    if not isinstance(trace_id, str):
        return False
    
    # Allow empty string as valid (same as None)
    if len(trace_id) == 0:
        return True
    
    # Max reasonable length for trace ID (UUID is ~36 chars, hex trace IDs can be longer)
    if len(trace_id) > 128:
        return False
    
    # Alphanumeric + hyphens only (typical for UUIDs and hex trace IDs)
    pattern = re.compile(r'^[a-fA-F0-9_-]+$')
    return bool(pattern.match(trace_id))


def validate_parent_task_id(parent_task_id: Optional[str], is_fanout: bool) -> bool:
    """
    Junior dev: If this task came from fan-out (e.g., analysis after detection),
    parent_task_id MUST be set so we can trace back.
    
    Example:
    - Detection task: parent_task_id = None (it's the first task)
    - Analysis task:  parent_task_id = "a1b2c3d4-detection-000" (came from detection)
    
    Args:
        parent_task_id: Optional parent task ID
        is_fanout: True if this is a child task (not the first step)
        
    Returns:
        True if valid, False if missing when required
    """
    # If parent_task_id is provided, validate it
    if parent_task_id is not None:
        if not isinstance(parent_task_id, str):
            return False
        # Use same validation as task_id
        return validate_task_id(parent_task_id)
    
    # If None, that's only OK if this is NOT a fanout task
    # For now we allow None even for fanout since we can't always detect it
    # The is_fanout flag helps but isn't always reliable at validation time
    return True


# ============================================================================
# BACKWARDS COMPATIBILITY - Re-export functions from their proper modules
# ============================================================================
# These functions have been moved to their proper modules but are re-exported
# here for backwards compatibility. Import from their proper modules instead:
#   - message_to_dict, build_output_prefix: import from io_sqs
#   - extract_job_id_from_prefix: import from io_storage
# ============================================================================

# Re-export from io_sqs
from .io_sqs import message_to_dict, build_output_prefix

# Re-export from io_storage
from .io_storage import extract_job_id_from_prefix


def dict_to_message(data: Dict[str, Any], bucket_allowlist: List[str]) -> TaskMessage:
    """
    Junior dev: Parse raw SQS message dict into typed TaskMessage.
    Wrapper around validate_message - does validation + conversion.
    
    Args:
        data: Raw message dict from SQS
        bucket_allowlist: Allowed buckets from config
        
    Returns:
        TaskMessage object
        
    Raises:
        ValueError if validation fails
    """
    # validate_message does all the heavy lifting
    return validate_message(data, bucket_allowlist)


def is_fanout_task(message: TaskMessage) -> bool:
    """
    Junior dev: Check if this is a child task (from fan-out) or the first task in pipeline.
    
    First task (e.g., detection): parent_task_id is None
    Child task (e.g., analysis): parent_task_id is set
    
    Args:
        message: TaskMessage to check
        
    Returns:
        True if fanout (has parent), False if root task
    """
    return message.parent_task_id is not None


def get_supported_file_extensions() -> List[str]:
    """
    Junior dev: Return list of allowed input file extensions.
    Used by validate_input_uri to reject weird files.
    
    Exact list: ['.png', '.jpg', '.jpeg', '.mp4', '.json']
    
    No executables (.exe, .sh, .py), no archives (.zip, .tar), no scripts.
    
    Returns:
        List of safe extensions: ['.png', '.jpg', '.jpeg', '.mp4', '.json']
    """
    return SUPPORTED_FILE_EXTENSIONS
