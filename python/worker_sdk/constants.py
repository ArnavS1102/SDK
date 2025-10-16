"""
constants.py – dynamic version
Loads all step, queue, and filetype definitions from config.yaml
Also defines core types and enums for message validation.
"""

import os
import yaml
from dataclasses import dataclass
from typing import Dict, Any, List, Optional


# ============================================================================
# CORE TYPES (for message validation)
# ============================================================================

@dataclass
class TaskMessage:
    """Validated task message structure."""
    job_id: str
    task_id: str
    user_id: str
    schema: str
    step: str
    input_uri: str
    output_prefix: str
    params: Dict[str, Any]
    retry_count: int = 0
    trace_id: Optional[str] = None
    parent_task_id: Optional[str] = None


# Message validation constants
SUPPORTED_SCHEMAS = {"v1"}
REQUIRED_MESSAGE_FIELDS = [
    "job_id", "task_id", "user_id", "schema", "step",
    "input_uri", "output_prefix", "params"
]

# File extension mappings
EXTENSION_MIME_TYPES = {
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".mp4": "video/mp4",
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".txt": "text/plain",
}

SUPPORTED_FILE_EXTENSIONS = list(EXTENSION_MIME_TYPES.keys())

# ============================================================================
# CONFIG LOADING
# ============================================================================

def load_config(path: str = None) -> Dict[str, Any]:
    """Load YAML config once and cache it. Always uses config/default.yaml."""
    if path is None:
        # Always use config/default.yaml (no env var override)
        path = os.path.join(os.path.dirname(__file__), "config", "default.yaml")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing config file at {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

AWS_ACCOUNT_ID = CONFIG["aws"]["account_id"]
AWS_REGION = CONFIG["aws"]["region"]
WORK_BUCKET = CONFIG["aws"]["work_bucket"]

# ============================================================================
# STEPS / QUEUES (YAML-based)
# ============================================================================

STEPS = CONFIG["steps"]
QUEUE_NAMES = CONFIG["queues"]

# Dynamic VALID_STEPS (loaded from YAML)
VALID_STEPS = list(STEPS.keys())

def build_sqs_url(queue_name: str) -> str:
    """Construct SQS URL from queue name + account + region."""
    return f"https://sqs.{AWS_REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/{queue_name}"

# Map step → full SQS URL
QUEUE_URLS: Dict[str, str] = {}
for step_name, step_def in STEPS.items():
    q_key = step_def.get("queue")
    if q_key and q_key in QUEUE_NAMES:
        QUEUE_URLS[step_name] = build_sqs_url(QUEUE_NAMES[q_key])

# Add DLQ
DLQ_URL = build_sqs_url(QUEUE_NAMES.get("dlq", "ytbot-dev-video-dlq"))

# ============================================================================
# DYNAMIC STEP INFO ACCESSORS
# ============================================================================

def get_step_names() -> List[str]:
    return list(STEPS.keys())

def get_next_steps(step: str) -> List[str]:
    return STEPS.get(step, {}).get("next", [])

def get_join_step(step: str) -> str:
    return STEPS.get(step, {}).get("join_to")

def get_allowed_extensions(step: str) -> List[str]:
    return STEPS.get(step, {}).get("extensions", [])

def get_primary_filetype(step: str) -> str:
    return STEPS.get(step, {}).get("filetype", "json")

def get_queue_url(step: str) -> str:
    return QUEUE_URLS.get(step)

# ============================================================================
# ID / PREFIX HELPERS
# ============================================================================

def make_output_prefix(job_id: str, step: str, task_id: str) -> str:
    """Standard S3 prefix convention."""
    return f"s3://{WORK_BUCKET}/work/{job_id}/{step}/{task_id}/"

# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    print("Loaded config for steps:", get_step_names())
    print("Queue URLs:", QUEUE_URLS)
    for step in get_step_names():
        print(f"{step} → next: {get_next_steps(step)} join_to: {get_join_step(step)}")
