"""
Configuration loader.
Merges environment variables + YAML files into typed config object.
Everything else reads from this - single source of truth.
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
import os


# ============================================================================
# CONFIG SCHEMA
# ============================================================================

@dataclass
class QueueConfig:
    """Queue URLs and settings"""
    input_queue_url: str
    next_queue_url: Optional[str] = None  # None if final step
    dead_letter_queue_url: Optional[str] = None
    visibility_timeout: int = 300  # seconds
    wait_time: int = 20  # long-poll seconds
    max_messages: int = 1  # messages per poll


@dataclass
class StorageConfig:
    """S3/R2 settings"""
    bucket: str
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None  # for R2/MinIO
    access_key: Optional[str] = None
    secret_key: Optional[str] = None


@dataclass
class DatabaseConfig:
    """Task/Job DB connection"""
    dsn: str  # postgresql://user:pass@host:port/db
    pool_size: int = 10
    timeout: int = 30


@dataclass
class WorkerConfig:
    """Worker behavior settings"""
    service_name: str
    heartbeat_interval: int = 30  # seconds between lease renewals
    lease_duration: int = 120  # how long we claim a task
    max_retries: int = 3
    enable_idempotency: bool = True
    gpu_enabled: bool = False


@dataclass
class LoggingConfig:
    """Logging settings"""
    level: str = "INFO"
    format: str = "json"  # json or text
    include_trace: bool = True


@dataclass
class SDKConfig:
    """
    Complete SDK configuration.
    All modules read from this object.
    """
    queues: QueueConfig
    storage: StorageConfig
    database: DatabaseConfig
    worker: WorkerConfig
    logging: LoggingConfig
    
    # Optional: service-specific params loaded from service's config/*.yaml
    service_params: Dict[str, Any] = None


# ============================================================================
# ENVIRONMENT VARIABLE NAMES
# ============================================================================

# Required env vars (fail if missing)
REQUIRED_ENV_VARS = [
    "QUEUE_URL",
    "BUCKET",
    "DB_DSN",
    "SERVICE_NAME"
]

# Optional env vars with defaults
OPTIONAL_ENV_VARS = {
    "NEXT_QUEUE_URL": None,
    "AWS_REGION": "us-east-1",
    "HEARTBEAT_INTERVAL": "30",
    "LEASE_DURATION": "120",
    "LOG_LEVEL": "INFO"
}


# ============================================================================
# LOADING FUNCTIONS
# ============================================================================

def load_config(config_dir: Optional[str] = None) -> SDKConfig:
    """
    Main entry point.
    Load env vars first, then overlay YAML configs if present.
    
    Priority (highest to lowest):
    1. Environment variables
    2. config/overrides.yaml (if exists)
    3. config/defaults.yaml (if exists)
    
    Args:
        config_dir: Path to config/ directory (default: ./config)
        
    Returns:
        Complete SDKConfig object
        
    Raises:
        ValueError if required vars missing or invalid
    """
    pass


def load_env_vars() -> Dict[str, str]:
    """
    Read all SDK-related env vars.
    Check required ones are present.
    Apply defaults for optional ones.
    
    Returns:
        Dict of env var key -> value
        
    Raises:
        ValueError if required env var missing
    """
    pass


def load_yaml_file(filepath: str) -> Dict[str, Any]:
    """
    Parse single YAML file.
    Return empty dict if file doesn't exist (not an error).
    
    Args:
        filepath: Path to YAML file
        
    Returns:
        Parsed YAML as dict, or {} if file not found
        
    Raises:
        yaml.YAMLError if file exists but invalid YAML
    """
    pass


def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge multiple config dicts.
    Later configs override earlier ones.
    
    Example:
        base = {"queues": {"timeout": 300}}
        override = {"queues": {"timeout": 600}}
        result = merge_configs(base, override)
        # {"queues": {"timeout": 600}}
    
    Args:
        *configs: Variable number of config dicts
        
    Returns:
        Single merged dict
    """
    pass


def parse_queue_config(raw: Dict[str, Any]) -> QueueConfig:
    """
    Convert raw dict to typed QueueConfig.
    Validate URLs are well-formed.
    
    Args:
        raw: Dict with queue settings
        
    Returns:
        QueueConfig object
        
    Raises:
        ValueError if invalid queue config
    """
    pass


def parse_storage_config(raw: Dict[str, Any]) -> StorageConfig:
    """
    Convert raw dict to typed StorageConfig.
    Handle both AWS S3 and R2/MinIO (needs endpoint_url).
    
    Args:
        raw: Dict with storage settings
        
    Returns:
        StorageConfig object
        
    Raises:
        ValueError if invalid storage config
    """
    pass


def parse_database_config(raw: Dict[str, Any]) -> DatabaseConfig:
    """
    Convert raw dict to typed DatabaseConfig.
    Validate DSN format (basic check).
    
    Args:
        raw: Dict with database settings
        
    Returns:
        DatabaseConfig object
        
    Raises:
        ValueError if invalid DB config
    """
    pass


def parse_worker_config(raw: Dict[str, Any]) -> WorkerConfig:
    """
    Convert raw dict to typed WorkerConfig.
    Validate intervals/durations are positive integers.
    
    Args:
        raw: Dict with worker settings
        
    Returns:
        WorkerConfig object
    """
    pass


def parse_logging_config(raw: Dict[str, Any]) -> LoggingConfig:
    """
    Convert raw dict to typed LoggingConfig.
    Validate log level is valid (DEBUG/INFO/WARNING/ERROR).
    
    Args:
        raw: Dict with logging settings
        
    Returns:
        LoggingConfig object
    """
    pass


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_queue_url(url: str) -> bool:
    """
    Check if URL looks like valid SQS queue URL.
    Should start with https:// and contain sqs.
    
    Args:
        url: Queue URL to check
        
    Returns:
        True if valid format, False otherwise
    """
    pass


def validate_s3_uri(uri: str) -> bool:
    """
    Check if URI is valid S3 path.
    Should be s3://bucket/key format.
    
    Args:
        uri: S3 URI to check
        
    Returns:
        True if valid format, False otherwise
    """
    pass


def validate_dsn(dsn: str) -> bool:
    """
    Check if DSN looks valid.
    Should be postgresql://... format.
    
    Args:
        dsn: Database connection string
        
    Returns:
        True if valid format, False otherwise
    """
    pass


# ============================================================================
# HELPERS
# ============================================================================

def get_service_params(config_dir: str) -> Dict[str, Any]:
    """
    Load service-specific config files.
    Looks for config/models.yaml, config/service.yaml, etc.
    This is optional - services can use it for custom settings.
    
    Args:
        config_dir: Path to config/ directory
        
    Returns:
        Dict of merged service-specific configs
    """
    pass


def get_config_value(key: str, default: Any = None) -> Any:
    """
    Get single config value by key.
    Convenience for accessing nested config.
    
    Example:
        timeout = get_config_value("queues.visibility_timeout", 300)
    
    Args:
        key: Dot-separated key path
        default: Default value if key not found
        
    Returns:
        Config value or default
    """
    pass
