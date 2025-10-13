"""
Message & status schema validation.
Defines the contract every message must follow.
"""

from typing import Dict, Any, List
from dataclasses import dataclass


# ============================================================================
# MESSAGE SCHEMA
# ============================================================================

@dataclass
class TaskMessage:
    """
    The standard message format.
    Every SQS message must deserialize into this.
    """
    job_id: str
    task_id: str
    input_uri: str
    output_prefix: str
    params: Dict[str, Any]
    
    # Optional fields
    trace_id: str = None
    retry_count: int = 0
    parent_task_id: str = None


# Required fields - message rejected if any missing
REQUIRED_MESSAGE_FIELDS = [
    "job_id",
    "task_id", 
    "input_uri",
    "output_prefix",
    "params"
]


# ============================================================================
# STATUS ENUMS
# ============================================================================

class TaskStatus:
    """Valid task states in DB"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"  # idempotency skip


class JobStatus:
    """Valid job states in DB"""
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    FAILED = "FAILED"


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_message(raw_message: Dict[str, Any]) -> TaskMessage:
    """
    Check message has all required fields.
    Check field types are correct.
    Reject early if invalid (don't waste compute).
    
    Args:
        raw_message: Raw dict from SQS body
        
    Returns:
        TaskMessage object if valid
        
    Raises:
        ValueError if missing fields or wrong types
    """
    pass


def validate_task_status(status: str) -> bool:
    """
    Check if status is one of allowed TaskStatus values.
    Prevents garbage in DB.
    
    Args:
        status: Status string to check
        
    Returns:
        True if valid, False otherwise
    """
    pass


def validate_job_status(status: str) -> bool:
    """
    Check if status is one of allowed JobStatus values.
    
    Args:
        status: Status string to check
        
    Returns:
        True if valid, False otherwise
    """
    pass


def validate_output_prefix(output_prefix: str, bucket: str) -> bool:
    """
    Check output_prefix starts with correct bucket.
    Enforce namespace rules (can't write outside job prefix).
    
    Args:
        output_prefix: Where service wants to write
        bucket: Allowed bucket from config
        
    Returns:
        True if valid path, False otherwise
    """
    pass


def validate_params(params: Dict[str, Any]) -> bool:
    """
    Basic sanity check on params dict.
    Check it's a dict, not too large (prevent abuse).
    
    Args:
        params: Parameters dict from message
        
    Returns:
        True if valid, False otherwise
    """
    pass


# ============================================================================
# SCHEMA HELPERS
# ============================================================================

def message_to_dict(message: TaskMessage) -> Dict[str, Any]:
    """
    Convert TaskMessage back to dict for SQS publish.
    Used when fanning out to next queue.
    
    Args:
        message: TaskMessage object
        
    Returns:
        Dict ready for json.dumps()
    """
    pass


def dict_to_message(data: Dict[str, Any]) -> TaskMessage:
    """
    Parse dict into TaskMessage.
    Wrapper around validate_message.
    
    Args:
        data: Raw message dict
        
    Returns:
        TaskMessage object
    """
    pass
