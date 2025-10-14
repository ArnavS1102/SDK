"""
SQS client: receive, delete, extend visibility, publish next-step messages.

TODO: Full implementation needed for:
- receive_messages(queue_url: str, max_messages: int, wait_time: int) → List[Message]
- delete_message(queue_url: str, receipt_handle: str) → None
- change_visibility(queue_url: str, receipt_handle: str, timeout: int) → None
- publish_message(queue_url: str, body: dict) → str
- publish_batch(queue_url: str, messages: List[dict]) → List[str]
"""

from typing import Dict, Any

from .constants import TaskMessage


# ============================================================================
# MESSAGE SERIALIZATION (moved from contracts.py)
# ============================================================================

def message_to_dict(message: TaskMessage, preserve_unknown: bool = True) -> Dict[str, Any]:
    """
    Junior dev: Turn a TaskMessage back into a dict so we can send it to the next queue.
    
    Important: If preserve_unknown=True, keep any extra fields from the original message
    that we don't know about. This lets newer services add fields without breaking old ones.
    
    Args:
        message: TaskMessage object
        preserve_unknown: Keep fields not in TaskMessage schema (forward-compat)
        
    Returns:
        Dict ready for json.dumps() and SQS publish
    """
    result = {
        "job_id": message.job_id,
        "task_id": message.task_id,
        "user_id": message.user_id,
        "schema": message.schema,
        "step": message.step,
        "input_uri": message.input_uri,
        "output_prefix": message.output_prefix,
        "params": message.params,
        "retry_count": message.retry_count,
    }
    
    # Add optional fields only if set
    if message.trace_id is not None:
        result["trace_id"] = message.trace_id
    
    if message.parent_task_id is not None:
        result["parent_task_id"] = message.parent_task_id
    
    return result
