#!/usr/bin/env python3
"""
Quick test script for contracts.py validation functions.
Run: python test_contracts.py

NOTE: This is a TEST file - it imports individual functions to test them in isolation.

In PRODUCTION code (runner.py, hooks.py, etc.), you would ONLY import:
    from worker_sdk.contracts import validate_message, dict_to_message, TaskMessage
    
The individual validators (validate_user_id, validate_job_id, etc.) are INTERNAL
helpers that validate_message() calls. You never call them directly in production.

But in tests, we import them to verify each function works correctly on its own.
"""

import sys
sys.path.insert(0, 'worker_sdk')

# In tests, we import everything to test each function individually
from worker_sdk.contracts import (
    validate_message,           # Main entry point (used in production)
    validate_schema_version,    # Internal helper (tested here)
    validate_step,              # Internal helper (tested here)
    validate_input_uri,         # Internal helper (tested here)
    validate_output_prefix,     # Internal helper (tested here)
    validate_params,            # Internal helper (tested here)
    validate_user_id,           # Internal helper (tested here)
    validate_job_id,            # Internal helper (tested here)
    validate_task_id,           # Internal helper (tested here)
    validate_trace_id,          # Internal helper (tested here)
    validate_parent_task_id,    # Internal helper (tested here)
    message_to_dict,            # Used in production (for publish_next)
    dict_to_message,            # Used in production (wrapper for validate_message)
    build_output_prefix,        # Used in production (for publish_next)
    extract_job_id_from_prefix, # Internal helper
    is_fanout_task,             # Internal helper
    get_supported_file_extensions, # Internal helper
    TaskMessage,                # Used in production
    SUPPORTED_SCHEMAS,          # Constants for testing
    VALID_STEPS,                # Constants for testing
)


def test_valid_message():
    """Test a completely valid message"""
    print("✓ Testing valid message...")
    
    valid_msg = {
        "job_id": "abc123-def456",
        "task_id": "abc123-def456-detection-000",
        "user_id": "user_789",
        "schema": "v1",
        "step": "DETECTION",
        "input_uri": "s3://uploads/input.jpg",
        "output_prefix": "s3://work-bucket/work/abc123-def456/DETECTION/abc123-def456-detection-000/",
        "params": {"threshold": 0.5, "model": "yolo"},
        "trace_id": "4bf92f3577b34da6",
        "parent_task_id": None,
        "retry_count": 0
    }
    
    try:
        msg = validate_message(valid_msg, ["uploads", "work-bucket"])
        print(f"  ✓ Valid message accepted: job_id={msg.job_id}, step={msg.step}")
        return True
    except ValueError as e:
        print(f"  ✗ FAILED: {e}")
        return False


def test_invalid_messages():
    """Test various invalid messages"""
    print("\n✓ Testing invalid message rejection...")
    
    base_msg = {
        "job_id": "abc123",
        "task_id": "abc123-detection-000",
        "user_id": "user_123",
        "schema": "v1",
        "step": "DETECTION",
        "input_uri": "s3://uploads/input.jpg",
        "output_prefix": "s3://work-bucket/work/abc123/DETECTION/abc123-detection-000/",
        "params": {},
    }
    
    tests = [
        ("Missing field", {**base_msg, "job_id": None}, "job_id must be str"),
        ("Bad schema", {**base_msg, "schema": "v99"}, "Unsupported schema"),
        ("Bad step", {**base_msg, "step": "INVALID"}, "Invalid step"),
        ("Bad input_uri", {**base_msg, "input_uri": "http://bad.com/file.jpg"}, "Invalid input_uri"),
        ("Path traversal", {**base_msg, "input_uri": "s3://uploads/../etc/passwd"}, "Invalid input_uri"),
        ("Bad extension", {**base_msg, "input_uri": "s3://uploads/virus.exe"}, "Invalid input_uri"),
        ("Wrong output_prefix", {**base_msg, "output_prefix": "s3://wrong/path/"}, "Invalid output_prefix"),
        ("No trailing slash", {**base_msg, "output_prefix": "s3://work-bucket/work/abc123/DETECTION/abc123-detection-000"}, "Invalid output_prefix"),
    ]
    
    passed = 0
    for name, msg, expected_error in tests:
        try:
            validate_message(msg, ["uploads", "work-bucket"])
            print(f"  ✗ FAILED {name}: Should have rejected")
        except ValueError as e:
            if expected_error in str(e):
                print(f"  ✓ {name}: Correctly rejected")
                passed += 1
            else:
                print(f"  ⚠ {name}: Rejected but wrong error: {e}")
    
    return passed == len(tests)


def test_id_validators():
    """Test individual ID validators"""
    print("\n✓ Testing ID validators...")
    
    tests = [
        ("user_id valid", lambda: validate_user_id("user_123"), True),
        ("user_id empty", lambda: validate_user_id(""), False),
        ("user_id special char", lambda: validate_user_id("user@123"), False),
        ("job_id valid", lambda: validate_job_id("abc-123-def"), True),
        ("job_id too long", lambda: validate_job_id("x" * 200), False),
        ("task_id valid", lambda: validate_task_id("job-detection-000"), True),
        ("task_id valid long", lambda: validate_task_id("job-det-000-ana-001-comp-000"), True),
        ("trace_id valid", lambda: validate_trace_id("4bf92f3577b34da6"), True),
        ("trace_id None", lambda: validate_trace_id(None), True),
    ]
    
    passed = 0
    for name, test_fn, expected in tests:
        result = test_fn()
        if result == expected:
            print(f"  ✓ {name}")
            passed += 1
        else:
            print(f"  ✗ {name}: expected {expected}, got {result}")
    
    return passed == len(tests)


def test_uri_validators():
    """Test URI validation"""
    print("\n✓ Testing URI validators...")
    
    tests = [
        ("s3 valid", lambda: validate_input_uri("s3://uploads/file.jpg", ["uploads"]), True),
        ("r2 valid", lambda: validate_input_uri("r2://uploads/file.png", ["uploads"]), True),
        ("wrong scheme", lambda: validate_input_uri("http://uploads/file.jpg", ["uploads"]), False),
        ("wrong bucket", lambda: validate_input_uri("s3://forbidden/file.jpg", ["uploads"]), False),
        ("bad extension", lambda: validate_input_uri("s3://uploads/file.exe", ["uploads"]), False),
        ("path traversal", lambda: validate_input_uri("s3://uploads/../etc/file.jpg", ["uploads"]), False),
        ("shell injection", lambda: validate_input_uri("s3://uploads/file.jpg; rm -rf /", ["uploads"]), False),
    ]
    
    passed = 0
    for name, test_fn, expected in tests:
        result = test_fn()
        if result == expected:
            print(f"  ✓ {name}")
            passed += 1
        else:
            print(f"  ✗ {name}: expected {expected}, got {result}")
    
    return passed == len(tests)


def test_params_validator():
    """Test params validation"""
    print("\n✓ Testing params validator...")
    
    tests = [
        ("simple dict", {"key": "value"}, True),
        ("nested dict", {"outer": {"inner": "value"}}, True),
        ("too deep", {"a": {"b": {"c": {"d": {"e": {"f": "too deep"}}}}}}, False),
        ("has list", {"items": [1, 2, 3]}, True),
        ("not dict", "string", False),
    ]
    
    passed = 0
    for name, params, expected in tests:
        result = validate_params(params)
        if result == expected:
            print(f"  ✓ {name}")
            passed += 1
        else:
            print(f"  ✗ {name}: expected {expected}, got {result}")
    
    return passed == len(tests)


def test_helpers():
    """Test helper functions"""
    print("\n✓ Testing helper functions...")
    
    # Test build_output_prefix
    prefix = build_output_prefix("my-bucket", "job123", "DETECTION", "task-001")
    expected = "s3://my-bucket/work/job123/DETECTION/task-001/"
    if prefix == expected:
        print(f"  ✓ build_output_prefix")
    else:
        print(f"  ✗ build_output_prefix: got {prefix}")
        return False
    
    # Test extract_job_id_from_prefix
    job_id = extract_job_id_from_prefix(prefix)
    if job_id == "job123":
        print(f"  ✓ extract_job_id_from_prefix")
    else:
        print(f"  ✗ extract_job_id_from_prefix: got {job_id}")
        return False
    
    # Test get_supported_file_extensions
    exts = get_supported_file_extensions()
    if '.jpg' in exts and '.png' in exts and '.mp4' in exts:
        print(f"  ✓ get_supported_file_extensions")
    else:
        print(f"  ✗ get_supported_file_extensions: missing extensions")
        return False
    
    # Test message_to_dict and dict_to_message
    msg = TaskMessage(
        job_id="job123",
        task_id="job123-det-000",
        user_id="user1",
        schema="v1",
        step="DETECTION",
        input_uri="s3://uploads/file.jpg",
        output_prefix="s3://work-bucket/work/job123/DETECTION/job123-det-000/",
        params={"key": "value"},
    )
    
    msg_dict = message_to_dict(msg)
    if msg_dict["job_id"] == "job123" and msg_dict["step"] == "DETECTION":
        print(f"  ✓ message_to_dict")
    else:
        print(f"  ✗ message_to_dict failed")
        return False
    
    # Test is_fanout_task
    msg_root = TaskMessage(
        job_id="job123", task_id="t1", user_id="u1", schema="v1", step="DETECTION",
        input_uri="s3://uploads/f.jpg", output_prefix="s3://work-bucket/work/job123/DETECTION/t1/",
        params={}, parent_task_id=None
    )
    msg_child = TaskMessage(
        job_id="job123", task_id="t2", user_id="u1", schema="v1", step="ANALYSIS",
        input_uri="s3://uploads/f.jpg", output_prefix="s3://work-bucket/work/job123/ANALYSIS/t2/",
        params={}, parent_task_id="t1"
    )
    
    if not is_fanout_task(msg_root) and is_fanout_task(msg_child):
        print(f"  ✓ is_fanout_task")
    else:
        print(f"  ✗ is_fanout_task failed")
        return False
    
    return True


def test_enum_checks():
    """Test enum validation"""
    print("\n✓ Testing enum validators...")
    
    tests = [
        ("schema v1", lambda: validate_schema_version("v1"), True),
        ("schema v99", lambda: validate_schema_version("v99"), False),
        ("step DETECTION", lambda: validate_step("DETECTION"), True),
        ("step INVALID", lambda: validate_step("INVALID"), False),
    ]
    
    passed = 0
    for name, test_fn, expected in tests:
        result = test_fn()
        if result == expected:
            print(f"  ✓ {name}")
            passed += 1
        else:
            print(f"  ✗ {name}: expected {expected}, got {result}")
    
    return passed == len(tests)


def main():
    print("=" * 60)
    print("CONTRACTS.PY VALIDATION TEST SUITE")
    print("=" * 60)
    
    results = []
    
    results.append(("Valid message", test_valid_message()))
    results.append(("Invalid messages", test_invalid_messages()))
    results.append(("ID validators", test_id_validators()))
    results.append(("URI validators", test_uri_validators()))
    results.append(("Params validator", test_params_validator()))
    results.append(("Helper functions", test_helpers()))
    results.append(("Enum validators", test_enum_checks()))
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status}: {name}")
    
    print(f"\n{passed}/{total} test suites passed")
    
    if passed == total:
        print("\n🎉 All tests passed! contracts.py is working correctly.")
        return 0
    else:
        print("\n⚠️  Some tests failed. Check the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

