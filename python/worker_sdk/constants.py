"""
Constants, enums, and types used across the SDK.
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass


# ============================================================================
# MESSAGE SCHEMA
# ============================================================================

@dataclass
class TaskMessage:
    """
    The standard message format.
    Every SQS message must deserialize into this.
    
    Required envelope fields ensure safe routing, ownership, versioning, and storage.
    """
    # Core identity
    job_id: str              # Pipeline run ID (from API gateway)
    task_id: str             # Unique task ID (deterministic for retries)
    user_id: str             # Owner (for auth + scoping)
    
    # Routing & versioning
    schema: str              # Message format version (e.g., "v1")
    step: str                # Which service: DETECTION | ANALYSIS | COMPLETION
    
    # I/O
    input_uri: str           # Where to read input (s3://...)
    output_prefix: str       # Where to write outputs (enforced pattern)
    params: Dict[str, Any]   # Step-specific parameters
    
    # Traceability (optional but strongly recommended)
    trace_id: Optional[str] = None         # For distributed tracing
    parent_task_id: Optional[str] = None   # MANDATORY for fan-out tasks
    
    # Retry metadata
    retry_count: int = 0     # Attempt number (SQS ApproximateReceiveCount)


# ============================================================================
# SCHEMA VERSION
# ============================================================================

# Supported message schema versions
SUPPORTED_SCHEMAS = {'v1'}


# ============================================================================
# STEP ENUM
# ============================================================================

class StepType:
    """
    Valid pipeline steps.
    Add new steps here as you expand the pipeline.
    """
    DETECTION = "DETECTION"
    ANALYSIS = "ANALYSIS"
    COMPLETION = "COMPLETION"
    # Add more as needed: PREPROCESSING, POSTPROCESSING, etc.


VALID_STEPS = [
    StepType.DETECTION,
    StepType.ANALYSIS,
    StepType.COMPLETION
]


# ============================================================================
# STATUS ENUMS
# ============================================================================

class TaskStatus:
    """
    Valid task states in DB.
    Lifecycle: QUEUED → PROCESSING → DONE/FAILED/SKIPPED
    STALE = lease expired, reaper will reclaim
    """
    QUEUED = "QUEUED"           # Initial state
    PROCESSING = "PROCESSING"   # Claimed by worker
    DONE = "DONE"              # Success
    FAILED = "FAILED"          # Error
    SKIPPED = "SKIPPED"        # Idempotency skip
    STALE = "STALE"            # Lease expired, needs reclaim
    CANCELLED = "CANCELLED"    # User-initiated abort


class JobStatus:
    """
    Valid job states in DB.
    Lifecycle: QUEUED → PROCESSING → DONE/FAILED/CANCELLED
    """
    QUEUED = "QUEUED"           # Initial state
    PROCESSING = "PROCESSING"   # At least one task running
    DONE = "DONE"              # All tasks complete
    FAILED = "FAILED"          # Unrecoverable error
    CANCELLED = "CANCELLED"    # User aborted


VALID_TASK_STATUSES = [
    TaskStatus.QUEUED,
    TaskStatus.PROCESSING,
    TaskStatus.DONE,
    TaskStatus.FAILED,
    TaskStatus.SKIPPED,
    TaskStatus.STALE,
    TaskStatus.CANCELLED
]

VALID_JOB_STATUSES = [
    JobStatus.QUEUED,
    JobStatus.PROCESSING,
    JobStatus.DONE,
    JobStatus.FAILED,
    JobStatus.CANCELLED
]


# ============================================================================
# MESSAGE FIELDS
# ============================================================================

# Required fields - message rejected if any missing
REQUIRED_MESSAGE_FIELDS = [
    "job_id",
    "task_id",
    "user_id",
    "schema",
    "step",
    "input_uri",
    "output_prefix",
    "params"
]


# ============================================================================
# FILE EXTENSIONS & MIME TYPES
# ============================================================================

# Extension to MIME type mapping (single source of truth)
EXTENSION_MIME_TYPES = {
    # Images
    '.png':  'image/png',
    '.jpg':  'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.webp': 'image/webp',
    '.gif':  'image/gif',
    '.tif':  'image/tiff',
    '.tiff': 'image/tiff',
    '.bmp':  'image/bmp',
    '.svg':  'image/svg+xml',
    
    # Videos
    '.mp4':  'video/mp4',
    '.mov':  'video/quicktime',
    '.webm': 'video/webm',
    '.mkv':  'video/x-matroska',
    '.avi':  'video/x-msvideo',
    
    # Audio
    '.mp3':  'audio/mpeg',
    '.wav':  'audio/wav',
    '.m4a':  'audio/mp4',
    '.aac':  'audio/aac',
    '.ogg':  'audio/ogg',
    '.flac': 'audio/flac',
    '.opus': 'audio/opus',
    
    # Documents
    '.pdf':  'application/pdf',
    '.json': 'application/json',
    '.csv':  'text/csv',
    '.tsv':  'text/tab-separated-values',
    '.txt':  'text/plain',
    '.md':   'text/markdown',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.xls':  'application/vnd.ms-excel',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.doc':  'application/msword',
    '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    '.ppt':  'application/vnd.ms-powerpoint',
}

# Allowed input file extensions (derived from mapping keys)
SUPPORTED_FILE_EXTENSIONS = list(EXTENSION_MIME_TYPES.keys())

