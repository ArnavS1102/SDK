#!/usr/bin/env python3
"""
End-to-end tests for S3 I/O utils against a real S3 bucket.

Covers:
- build_output_prefix (from io_sqs), extract_job_id_from_prefix (from io_storage)
- head, exists
- get_bytes (success and >50MB failure)
- put_bytes (single-part + multipart branches)
- put_json
- delete
- guess_content_type, ensure_within_prefix (indirect and direct)

Run:
  python test_s3io_e2e.py --bucket my-s3io-tests
"""

import argparse
import os
import sys
import uuid
import json
import traceback
from typing import Callable

# >>> UPDATE THIS IMPORT TO YOUR MODULE <<<
# from mypackage.s3io import *
from worker_sdk.io_sqs import build_output_prefix  # noqa: E402
from worker_sdk.io_storage import (  # noqa: E402
    extract_job_id_from_prefix,
    head,
    exists,
    get_bytes,
    get_json,
    put_bytes,
    put_json,
    delete,
    guess_content_type,
    ensure_within_prefix,
)

def _print_ok(name: str): print(f"[PASS] {name}")
def _print_fail(name: str, e: Exception): print(f"[FAIL] {name}: {e}\n{traceback.format_exc()}")

def _assert(cond: bool, msg: str):
    if not cond:
        raise AssertionError(msg)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True, help="S3 bucket used for tests")
    args = ap.parse_args()

    BUCKET = args.bucket

    # --- Fixed input object paths from actual S3 bucket ---
    INPUT_JSON_URI   = f"s3://{BUCKET}/inbox/config/manifest.json"      # 408 bytes
    INPUT_VIDEO_URI  = f"s3://{BUCKET}/inbox/media/scene0.mp4"          # 13.8 MB (under 50MB cap)
    INPUT_BIG_URI    = f"s3://{BUCKET}/inbox/media/sample_vid_loop3.mp4"  # 87.6 MB (over 50MB cap)
    INPUT_TEXT_URI   = f"s3://{BUCKET}/other/random.txt"                # 18 bytes

    # --- Build a canonical output prefix ---
    job_id = f"job_{uuid.uuid4().hex[:8]}"
    task_id = f"task_{uuid.uuid4().hex[:8]}"
    user_id = "user_test"
    step = "DETECTION"   # must be in VALID_STEPS (DETECTION, ANALYSIS, COMPLETION)
    try:
        output_prefix = build_output_prefix(BUCKET, user_id, job_id, step, task_id)
        _assert(output_prefix.endswith("/"), "output_prefix must end with '/'")
        _assert(output_prefix.startswith(f"s3://{BUCKET}/{user_id}/{job_id}/{step}/"), "unexpected prefix shape")
        _print_ok("build_output_prefix")
    except Exception as e:
        _print_fail("build_output_prefix", e); return 1

    # --- extract_job_id_from_prefix ---
    try:
        jid = extract_job_id_from_prefix(output_prefix)
        _assert(jid == job_id, f"extracted job_id mismatch: {jid} != {job_id}")
        _print_ok("extract_job_id_from_prefix")
    except Exception as e:
        _print_fail("extract_job_id_from_prefix", e); return 1

    # --- head (on small objects) ---
    try:
        meta = head(INPUT_JSON_URI)
        _assert(meta["size"] > 0, "manifest.json size should be > 0")
        _assert("content_type" in meta, "head should return content_type")
        _print_ok("head(manifest.json)")
    except Exception as e:
        _print_fail("head(manifest.json)", e); return 1

    # --- exists ---
    try:
        _assert(exists(INPUT_VIDEO_URI) is True, "scene0.mp4 should exist")
        _assert(exists(f"s3://{BUCKET}/nope/doesnotexist.bin") is False, "non-existent key should return False")
        _print_ok("exists")
    except Exception as e:
        _print_fail("exists", e); return 1

    # --- get_bytes success (small JSON) ---
    try:
        b = get_bytes(INPUT_JSON_URI)
        data = json.loads(b.decode("utf-8"))
        _assert(data.get("ok") is True, "expected ok=True in manifest.json")
        _print_ok("get_bytes(manifest.json)")
    except Exception as e:
        _print_fail("get_bytes(manifest.json)", e); return 1

    # --- get_json success ---
    try:
        obj = get_json(INPUT_JSON_URI)
        _assert(obj.get("ok") is True, "expected ok=True via get_json")
        _print_ok("get_json(manifest.json)")
    except Exception as e:
        _print_fail("get_json(manifest.json)", e); return 1

    # --- get_bytes failure on >50MB cap (sample_vid_loop3.mp4 ~87.6MB) ---
    try:
        try:
            _ = get_bytes(INPUT_BIG_URI)
            raise AssertionError("get_bytes should have failed on video > 50MB")
        except ValueError as ve:
            _assert("50 MB" in str(ve), "error should mention 50 MB limit")
        _print_ok("get_bytes(big video) cap enforced")
    except Exception as e:
        _print_fail("get_bytes(big video) cap enforced", e); return 1

    # --- put_bytes single-part branch (<8MB) ---
    try:
        content_small = b"hello-small" * 100  # < 8MB
        target_small = "hello.txt"
        uri_small = ensure_within_prefix(output_prefix, target_small)
        # upload via put_bytes
        put_uri_small = put_bytes(output_prefix, target_small, content_small, "text/plain; charset=utf-8", {"x": "y"})
        _assert(put_uri_small == uri_small, "returned URI mismatch (single-part)")
        # head + verify size
        meta2 = head(put_uri_small)
        _assert(meta2["size"] == len(content_small), "size mismatch for single-part upload")
        _print_ok("put_bytes(single-part)")
    except Exception as e:
        _print_fail("put_bytes(single-part)", e); return 1

    # --- put_bytes multipart branch (>=8MB) ---
    try:
        # create ~10MB payload (use .txt extension which is supported)
        content_big = b"A" * (10 * 1024 * 1024)  # 10MB
        target_big = "ten_mb.txt"
        uri_big = ensure_within_prefix(output_prefix, target_big)
        put_uri_big = put_bytes(output_prefix, target_big, content_big, "text/plain", {"role": "test"})
        _assert(put_uri_big == uri_big, "returned URI mismatch (multipart)")
        meta3 = head(put_uri_big)
        _assert(meta3["size"] == len(content_big), "size mismatch for multipart upload")
        _print_ok("put_bytes(multipart)")
    except Exception as e:
        _print_fail("put_bytes(multipart)", e); return 1

    # --- put_json + read back ---
    try:
        payload = {"frames": 3, "fps": 30, "done": True}
        json_uri = ensure_within_prefix(output_prefix, "metrics.json")
        write_uri = put_json(output_prefix, "metrics.json", payload, {"component": "tests"})
        _assert(write_uri == json_uri, "URI mismatch from put_json")
        back = get_json(json_uri)
        _assert(back["fps"] == 30 and back["done"] is True, "roundtrip JSON mismatch")
        _print_ok("put_json + get_json roundtrip")
    except Exception as e:
        _print_fail("put_json + get_json", e); return 1

    # --- guess_content_type quick checks ---
    try:
        _assert(guess_content_type("report.csv").startswith("text/csv"), "CSV content-type")
        _assert(guess_content_type("photo.jpg").startswith("image/"), "image content-type")
        _assert("application/json" in guess_content_type("data.json"), "JSON content-type")
        _print_ok("guess_content_type")
    except Exception as e:
        _print_fail("guess_content_type", e); return 1

    # --- ensure_within_prefix safety ---
    try:
        safe_uri = ensure_within_prefix(output_prefix, "safe_name.bin")
        _assert(safe_uri.startswith(output_prefix), "ensure_within_prefix should keep us inside prefix")
        try:
            _ = ensure_within_prefix(output_prefix, "../escape.bin")
            raise AssertionError("ensure_within_prefix should have blocked traversal")
        except ValueError:
            pass
        try:
            _ = ensure_within_prefix(output_prefix, "subdir/evil.bin")
            raise AssertionError("ensure_within_prefix should have blocked slashes")
        except ValueError:
            pass
        _print_ok("ensure_within_prefix")
    except Exception as e:
        _print_fail("ensure_within_prefix", e); return 1

    # --- delete (idempotent) ---
    try:
        # delete the small and big uploads and JSON
        for target in ("hello.txt", "ten_mb.txt", "metrics.json"):
            uri = ensure_within_prefix(output_prefix, target)
            delete(uri)        # first time should succeed
            delete(uri)        # second time should be no-op
            _assert(exists(uri) is False, f"{target} should be gone")
        _print_ok("delete (idempotent)")
    except Exception as e:
        _print_fail("delete", e); return 1

    print("\nAll tests ✅ PASS")
    return 0

if __name__ == "__main__":
    sys.exit(main())
