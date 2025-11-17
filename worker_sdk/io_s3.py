"""
S3 storage operations.

Modular design:
- S3Storage class: encapsulates all S3 operations with retries/atomicity
- Module-level functions: backward-compatible functional API
- Easy to test: inject custom S3Storage instances
- Easy to extend: subclass S3Storage for local/test implementations
"""

from __future__ import annotations

import io
import json
import mimetypes
import os
import random
import re
import tempfile
import time
import uuid
from typing import Optional, Dict, Any, List, Tuple

from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ReadTimeoutError,
    ConnectionClosedError,
    ConnectTimeoutError,
)

from .constants import VALID_STEPS, SUPPORTED_FILE_EXTENSIONS, EXTENSION_MIME_TYPES
from .logging import get_logger


# ============================================================================
# S3 STORAGE CLASS (core implementation)
# ============================================================================

class S3Storage:
    """
    Encapsulates all S3 operations with retry logic, atomicity, and security.
    
    Benefits:
    - Easy to test: mock/stub the class or inject a test instance
    - Easy to configure: pass boto3 client or config at init
    - Easy to extend: subclass for local filesystem or test implementations
    """
    
    def __init__(self, s3_client=None, max_retries: int = 3, logger=None):
        """
        Initialize S3 storage adapter.
        
        Args:
            s3_client: boto3 S3 client (if None, creates default)
            max_retries: Number of retry attempts for transient errors
            logger: StructuredLogger instance (if None, creates default)
        """
        self._s3 = s3_client
        self.max_retries = max_retries
        self.logger = logger or get_logger("io_s3")
        self._transient_codes = {
            "500", "503", "RequestTimeout", "SlowDown", 
            "InternalError", "ServiceUnavailable"
        }
        self._network_exceptions = (
            EndpointConnectionError, ReadTimeoutError, 
            ConnectionClosedError, ConnectTimeoutError
        )
    
    @property
    def s3(self):
        """Lazy-load S3 client."""
        if self._s3 is None:
            import boto3
            self._s3 = boto3.client('s3')
        return self._s3
    
    # ------------------------------------------------------------------------
    # READ OPERATIONS
    # ------------------------------------------------------------------------
    
    def head(self, uri: str) -> Dict[str, Any]:
        """Get object metadata without downloading content."""
        bucket, key = self._parse_uri(uri)
        self.logger.debug("HEAD request", {"uri": uri})
        
        for attempt in range(self.max_retries):
            try:
                response = self.s3.head_object(Bucket=bucket, Key=key)
                return {
                    "size": response.get("ContentLength"),
                    "content_type": response.get("ContentType", "application/octet-stream"),
                    "etag": response.get("ETag", "").strip('"'),
                    "last_modified": response.get("LastModified"),
                    "metadata": response.get("Metadata", {}),
                }
            except ClientError as e:
                if not self._should_retry(e, attempt):
                    self.logger.error(f"HEAD failed: {uri}", {"error": str(e), "attempt": attempt + 1})
                    self._raise_mapped_error(e, uri)
                self.logger.warning(f"HEAD retry {attempt + 1}/{self.max_retries}", {"uri": uri})
                time.sleep(self._backoff(attempt))
            except self._network_exceptions as e:
                if attempt < self.max_retries - 1:
                    self.logger.warning(f"Network error on HEAD (retry {attempt + 1})", {"uri": uri})
                    time.sleep(self._backoff(attempt))
                    continue
                raise
        
        raise RuntimeError(f"Failed to HEAD after {self.max_retries} attempts: {uri}")
    
    def exists(self, uri: str) -> bool:
        """Check if object exists."""
        try:
            self.head(uri)
            return True
        except FileNotFoundError:
            return False
    
    def get_bytes(
        self,
        uri: str,
        *,
        chunk_size: int = 15 * 1024 * 1024,
        max_size: int = 50 * 1024 * 1024,
    ) -> bytes:
        """Download object as bytes (must be ≤ max_size)."""
        if not self._check_extension(uri, SUPPORTED_FILE_EXTENSIONS):
            raise ValueError(f"File extension not allowed: {uri}")
        
        # Check size up front
        meta = self.head(uri)
        size = meta.get("size")
        self.logger.debug("Downloading object", {"uri": uri, "size": size})
        
        if size and size > max_size:
            raise ValueError(f"Object exceeds {max_size} byte limit ({size} bytes): {uri}")
        
        bucket, key = self._parse_uri(uri)
        
        for attempt in range(self.max_retries):
            try:
                resp = self.s3.get_object(Bucket=bucket, Key=key)
                body = resp["Body"]
                
                chunks = []
                total = 0
                
                for chunk in body.iter_chunks(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > max_size:
                        body.close()
                        raise ValueError(f"Object exceeds {max_size} byte limit while streaming: {uri}")
                    chunks.append(chunk)
                
                body.close()
                self.logger.info("Downloaded object", {"uri": uri, "size": len(b"".join(chunks))})
                return b"".join(chunks)
            
            except ClientError as e:
                if not self._should_retry(e, attempt):
                    self.logger.error(f"Download failed: {uri}", {"error": str(e)})
                    self._raise_mapped_error(e, uri)
                self.logger.warning(f"Download retry {attempt + 1}/{self.max_retries}", {"uri": uri})
                time.sleep(self._backoff(attempt))
            except self._network_exceptions as e:
                if attempt < self.max_retries - 1:
                    self.logger.warning(f"Network error on download (retry {attempt + 1})", {"uri": uri})
                    time.sleep(self._backoff(attempt))
                    continue
                raise
        
        raise RuntimeError(f"Failed to download after {self.max_retries} attempts: {uri}")
    
    def get_json(self, uri: str) -> Dict[str, Any]:
        """Download and parse JSON file."""
        data = self.get_bytes(uri)
        try:
            return json.loads(data.decode('utf-8'))
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in {uri}: {e.msg}", e.doc, e.pos)
    
    # ------------------------------------------------------------------------
    # WRITE OPERATIONS
    # ------------------------------------------------------------------------
    
    def put_bytes(
        self,
        prefix: str,
        filename: str,
        data: bytes,
        content_type: str = None,
        metadata: Dict[str, str] = None,
        max_size: int = 1 * 1024 * 1024 * 1024,
    ) -> str:
        """
        Upload bytes with atomic tmp→final pattern.
        Uses single-part for small files, multipart for large files.
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("put_bytes() expects bytes or bytearray")
        
        if not filename or "/" in filename or "\\" in filename or ".." in filename:
            raise ValueError(f"Invalid filename: {filename}")
        
        if len(data) > max_size:
            raise ValueError(f"Object exceeds {max_size} byte limit ({len(data)} bytes)")
        
        bucket, base_key = self._parse_uri(prefix)
        if base_key and not base_key.endswith("/"):
            base_key = base_key + "/"
        
        if not self._check_extension(filename, SUPPORTED_FILE_EXTENSIONS):
            raise ValueError(f"File extension not allowed: {filename}")
        
        final_key = f"{base_key}{os.path.basename(filename)}"
        tmp_key = f"{final_key}.tmp-{uuid.uuid4().hex}"
        
        if content_type is None:
            content_type = self.guess_content_type(filename)
        
        meta = metadata or {}
        for k, v in meta.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise ValueError("Metadata keys and values must be strings")
        
        # Thresholds
        multipart_threshold = 8 * 1024 * 1024   # 8 MiB
        part_size = 16 * 1024 * 1024            # 16 MiB
        
        for attempt in range(self.max_retries):
            upload_id = None
            try:
                # Upload to tmp key
                if len(data) < multipart_threshold:
                    self.s3.put_object(
                        Bucket=bucket, Key=tmp_key, Body=data,
                        ContentType=content_type, Metadata=meta
                    )
                else:
                    upload_id = self._multipart_upload(
                        bucket, tmp_key, data, content_type, meta, part_size
                    )
                
                # Atomic finalize
                self.s3.copy_object(
                    Bucket=bucket, Key=final_key,
                    CopySource={"Bucket": bucket, "Key": tmp_key},
                    MetadataDirective="COPY"
                )
                
                # Cleanup tmp
                self._delete_silent(bucket, tmp_key)
                
                final_uri = f"s3://{bucket}/{final_key}"
                self.logger.info("Uploaded object", {
                    "uri": final_uri,
                    "size": len(data),
                    "multipart": len(data) >= multipart_threshold
                })
                return final_uri
            
            except ClientError as e:
                if upload_id:
                    self._abort_multipart_silent(bucket, tmp_key, upload_id)
                self._delete_silent(bucket, tmp_key)
                
                if not self._should_retry(e, attempt):
                    self._raise_mapped_error(e, f"s3://{bucket}/{final_key}")
                time.sleep(self._backoff(attempt))
            
            except self._network_exceptions:
                if upload_id:
                    self._abort_multipart_silent(bucket, tmp_key, upload_id)
                self._delete_silent(bucket, tmp_key)
                
                if attempt < self.max_retries - 1:
                    time.sleep(self._backoff(attempt))
                    continue
                raise
            
            except Exception:
                if upload_id:
                    self._abort_multipart_silent(bucket, tmp_key, upload_id)
                self._delete_silent(bucket, tmp_key)
                raise
        
        raise RuntimeError(f"Failed to upload after {self.max_retries} attempts: s3://{bucket}/{final_key}")
    
    def put_json(
        self, 
        prefix: str, 
        filename: str, 
        obj: Dict[str, Any], 
        metadata: Dict[str, str] = None
    ) -> str:
        """Serialize dict to JSON and upload."""
        json_bytes = json.dumps(obj, indent=2).encode("utf-8")
        return self.put_bytes(
            prefix, filename, json_bytes,
            "application/json; charset=utf-8", metadata
        )
    
    def delete(self, uri: str) -> None:
        """Delete object (idempotent)."""
        bucket, key = self._parse_uri(uri)
        self.logger.debug("Deleting object", {"uri": uri})
        
        for attempt in range(self.max_retries):
            try:
                self.s3.delete_object(Bucket=bucket, Key=key)
                self.logger.info("Deleted object", {"uri": uri})
                return
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in ("404", "NoSuchKey", "NotFound"):
                    return  # Idempotent
                if not self._should_retry(e, attempt):
                    self._raise_mapped_error(e, uri)
                time.sleep(self._backoff(attempt))
            except self._network_exceptions:
                if attempt < self.max_retries - 1:
                    time.sleep(self._backoff(attempt))
                    continue
                raise
        
        raise RuntimeError(f"Failed to delete after {self.max_retries} attempts: {uri}")
    
    # ------------------------------------------------------------------------
    # HELPERS (public utilities)
    # ------------------------------------------------------------------------
    
    @staticmethod
    def guess_content_type(filename: str) -> str:
        """Infer MIME type from filename."""
        name = os.path.basename(str(filename)).split("?", 1)[0].split("#", 1)[0]
        ext = os.path.splitext(name)[1].lower()
        
        if ext in EXTENSION_MIME_TYPES:
            ct = EXTENSION_MIME_TYPES[ext]
        else:
            guessed, _ = mimetypes.guess_type(name)
            ct = guessed or "application/octet-stream"
        
        if ct.startswith("text/") and "charset=" not in ct:
            return f"{ct}; charset=utf-8"
        if ct == "application/json":
            return f"{ct}; charset=utf-8"
        
        return ct
    
    @staticmethod
    def ensure_within_prefix(prefix: str, filename: str) -> str:
        """Build safe s3://.../prefix/filename (blocks path traversal)."""
        if not isinstance(prefix, str) or not isinstance(filename, str):
            raise ValueError("prefix and filename must be strings")
        
        prefix = prefix.strip()
        filename = filename.strip()
        
        if not filename:
            raise ValueError("filename cannot be empty")
        
        bucket, base_key = S3Storage._parse_s3_uri(prefix)
        
        if filename.startswith("/") or ".." in filename:
            raise ValueError(f"Path traversal or absolute path not allowed: {filename}")
        
        if "/" in filename or "\\" in filename:
            raise ValueError(f"Slashes not allowed in filename: {filename}")
        
        if any(ord(c) < 32 for c in filename) or any(c in filename for c in (";", "|", "\x00")):
            raise ValueError(f"Dangerous characters in filename: {filename}")
        
        if len(filename.encode("utf-8")) > 512:
            raise ValueError("filename too long")
        
        if base_key:
            base_key = base_key.rstrip("/") + "/"
        
        return f"s3://{bucket}/{base_key}{filename}"
    
    @staticmethod
    def extract_job_id_from_prefix(output_prefix: str) -> Optional[str]:
        """Parse job_id from output_prefix."""
        if not output_prefix or not isinstance(output_prefix, str):
            return None
        
        # s3://bucket/user_id/job_id/step/task_id/
        pattern = r'^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/[a-zA-Z0-9_-]+/([a-zA-Z0-9_-]+)/[^/]+/[^/]+/?$'
        match = re.match(pattern, output_prefix)
        
        return match.group(1) if match else None
    
    @staticmethod
    def extract_user_id_from_prefix(output_prefix: str) -> Optional[str]:
        """Parse user_id from output_prefix."""
        if not output_prefix or not isinstance(output_prefix, str):
            return None
        # s3://bucket/user_id/job_id/step/task_id/
        pattern = r'^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/([a-zA-Z0-9_-]+)/[a-zA-Z0-9_-]+/[^/]+/[^/]+/?$'
        match = re.match(pattern, output_prefix)
        return match.group(1) if match else None
    
    # ------------------------------------------------------------------------
    # PRIVATE HELPERS
    # ------------------------------------------------------------------------
    
    def _parse_uri(self, uri: str) -> Tuple[str, str]:
        """Parse s3://bucket/key → (bucket, key)."""
        return self._parse_s3_uri(uri)
    
    @staticmethod
    def _parse_s3_uri(uri: str) -> Tuple[str, str]:
        """Parse s3://bucket/key → (bucket, key). Static for use in static methods."""
        if not isinstance(uri, str):
            raise ValueError(f"Invalid URI (not a string): {uri!r}")
        
        uri = uri.strip()
        if not uri:
            raise ValueError("Invalid URI: empty string")
        
        if not uri.startswith("s3://"):
            raise ValueError(f"URI must start with s3://: {uri}")
        
        if any(c in uri for c in ("\\", "?", "#")):
            raise ValueError(f"Invalid S3 URI (contains '\\', '?' or '#'): {uri}")
        
        path = uri[len("s3://"):]
        
        if "/" not in path:
            raise ValueError(f"Invalid S3 URI format (missing key): {uri}")
        
        bucket, key = path.split("/", 1)
        if not bucket:
            raise ValueError(f"Invalid S3 URI (empty bucket): {uri}")
        if not key:
            raise ValueError(f"Invalid S3 URI (empty key): {uri}")
        
        return bucket, key
    
    @staticmethod
    def _check_extension(filename: str, allowed_extensions: List[str]) -> bool:
        """Check if filename has allowed extension (case-insensitive)."""
        if not filename:
            return False
        filename_lower = filename.lower()
        return any(filename_lower.endswith(ext.lower()) for ext in allowed_extensions)
    
    def _multipart_upload(
        self, 
        bucket: str, 
        key: str, 
        data: bytes, 
        content_type: str, 
        metadata: Dict[str, str], 
        part_size: int
    ) -> None:
        """Execute multipart upload for large files."""
        create_resp = self.s3.create_multipart_upload(
            Bucket=bucket, Key=key,
            ContentType=content_type, Metadata=metadata
        )
        upload_id = create_resp["UploadId"]
        
        try:
            parts = []
            stream = io.BytesIO(data)
            part_number = 1
            
            while True:
                chunk = stream.read(part_size)
                if not chunk:
                    break
                up = self.s3.upload_part(
                    Bucket=bucket, Key=key,
                    PartNumber=part_number, UploadId=upload_id,
                    Body=chunk
                )
                parts.append({"ETag": up["ETag"], "PartNumber": part_number})
                part_number += 1
            
            self.s3.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id,
                MultipartUpload={"Parts": parts}
            )
        except Exception:
            self._abort_multipart_silent(bucket, key, upload_id)
            raise
    
    def _should_retry(self, error: ClientError, attempt: int) -> bool:
        """Check if error is transient and we have retries left."""
        code = error.response.get("Error", {}).get("Code", "")
        return code in self._transient_codes and attempt < self.max_retries - 1
    
    def _raise_mapped_error(self, error: ClientError, uri: str) -> None:
        """Map ClientError to Python exceptions."""
        code = error.response.get("Error", {}).get("Code", "")
        
        if code in ("404", "NotFound", "NoSuchKey"):
            raise FileNotFoundError(f"File not found: {uri}") from error
        if code == "NoSuchBucket":
            raise FileNotFoundError(f"Bucket not found: {uri}") from error
        if code in ("AccessDenied", "403"):
            raise PermissionError(f"Access denied: {uri}") from error
        if code in ("PermanentRedirect", "AuthorizationHeaderMalformed"):
            raise ValueError(f"Wrong region/endpoint for {uri}: {code}") from error
        
        raise error
    
    @staticmethod
    def _backoff(attempt: int) -> float:
        """Exponential backoff with jitter."""
        return (2 ** attempt) + random.uniform(0, 0.25)
    
    def _delete_silent(self, bucket: str, key: str) -> None:
        """Delete object, ignoring errors."""
        try:
            self.s3.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass
    
    def _abort_multipart_silent(self, bucket: str, key: str, upload_id: str) -> None:
        """Abort multipart upload, ignoring errors."""
        try:
            self.s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception:
            pass


# ============================================================================
# MODULE-LEVEL API (backward compatibility)
# ============================================================================

# Default storage instance (lazy-initialized)
_default_storage: Optional[S3Storage] = None

def _get_storage() -> S3Storage:
    """Get or create default storage instance."""
    global _default_storage
    if _default_storage is None:
        _default_storage = S3Storage()
    return _default_storage


# Expose functional API for backward compatibility
def head(uri: str) -> Dict[str, Any]:
    """Get object metadata without downloading content."""
    return _get_storage().head(uri)

def exists(uri: str) -> bool:
    """Check if object exists."""
    return _get_storage().exists(uri)

def get_bytes(uri: str, *, chunk_size: int = 15 * 1024 * 1024) -> bytes:
    """Download object as bytes."""
    return _get_storage().get_bytes(uri, chunk_size=chunk_size)

def get_json(uri: str) -> Dict[str, Any]:
    """Download and parse JSON file."""
    return _get_storage().get_json(uri)

def put_bytes(
    prefix: str,
    filename: str,
    data: bytes,
    content_type: str = None,
    metadata: Dict[str, str] = None,
) -> str:
    """Upload bytes with atomic tmp→final pattern."""
    return _get_storage().put_bytes(prefix, filename, data, content_type, metadata)

def put_json(
    prefix: str, 
    filename: str, 
    obj: Dict[str, Any], 
    metadata: Dict[str, str] = None
) -> str:
    """Serialize dict to JSON and upload."""
    return _get_storage().put_json(prefix, filename, obj, metadata)

def delete(uri: str) -> None:
    """Delete object (idempotent)."""
    return _get_storage().delete(uri)

def guess_content_type(filename: str) -> str:
    """Infer MIME type from filename."""
    return S3Storage.guess_content_type(filename)

def ensure_within_prefix(prefix: str, filename: str) -> str:
    """Build safe s3://.../prefix/filename (blocks path traversal)."""
    return S3Storage.ensure_within_prefix(prefix, filename)

def extract_job_id_from_prefix(output_prefix: str) -> Optional[str]:
    """Parse job_id from output_prefix."""
    return S3Storage.extract_job_id_from_prefix(output_prefix)
def extract_user_id_from_prefix(output_prefix: str) -> Optional[str]:
    """Parse user_id from output_prefix."""
    return S3Storage.extract_user_id_from_prefix(output_prefix)


# Public API
__all__ = [
    "S3Storage",
    "head", "exists", "get_bytes", "get_json",
    "put_bytes", "put_json", "delete",
    "guess_content_type", "ensure_within_prefix", "extract_job_id_from_prefix", "extract_user_id_from_prefix",
]
