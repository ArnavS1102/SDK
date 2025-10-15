#!/usr/bin/env python3
"""
End-to-end tester for io_sqs.

Covers:
  - Smoke: send → receive → heartbeat(context) → delete
  - Batch: 12-message batch send
  - Backoff: requeue_with_delay then delete original; verify delayed reappearance
  - Poison pill: use is_poison_pill() to decide DLQ
  - Queue stats: get_queue_stats() before and after ops
  - Direct heartbeat: call extend_visibility_loop() directly (not the context manager)
"""

import os
import sys
import time
import json
import argparse
import threading
from typing import Dict, List, Tuple

# ⬇️ change this import to your actual module name if needed
from worker_sdk.io_sqs import (
    get_sqs_client, receive_messages, parse_message, delete_message,
    send_message, send_messages_batch, visibility_heartbeat,
    is_poison_pill, requeue_with_delay, move_to_dlq, get_queue_stats,
    extend_visibility_loop, change_visibility
)

# ---------- helpers ----------

def get_queue_url_map(sqs, prefix: str) -> Dict[str, str]:
    """Return {queue_name: queue_url} for all queues matching prefix."""
    urls: List[str] = []
    token = None
    while True:
        resp = sqs.list_queues(QueueNamePrefix=prefix, NextToken=token) if token else sqs.list_queues(QueueNamePrefix=prefix)
        urls.extend(resp.get("QueueUrls", []))
        token = resp.get("NextToken")
        if not token:
            break
    out = {}
    for url in urls:
        name = url.rstrip("/").split("/")[-1]
        out[name] = url
    return out

def name_for_step(qname: str) -> str:
    if qname.endswith("-requested"):
        stem = qname.rsplit("-", 1)[0]
        step = stem.split("-")[-1]
    else:
        step = qname.split("-")[-1]
    return step.upper()

def ensure_env() -> Tuple[str, str]:
    region = os.getenv("AWS_REGION") or "us-east-1"
    profile = os.getenv("AWS_PROFILE")
    if profile:
        import boto3
        boto3.setup_default_session(profile_name=profile, region_name=region)
    return region, profile or "<default>"

# ---------- NEW: stats ----------

def tc_change_visibility(sqs, url: str, step: str, initial_vt: int = 2, bump_to: int = 8):
    """
    Proves change_visibility works:
      - Receive with a short initial VT
      - Immediately bump VT to a longer window
      - Poll during the window: message must stay invisible
      - After the window lapses: message must reappear and be deletable
    """
    print(f"[CHANGE-VIS] -> {url} (initial={initial_vt}s → bump_to={bump_to}s)")

    # Send a single test message
    payload = {
        "job_id": "job-cvis",
        "task_id": f"t-{step.lower()}-cvis",
        "step": step,
        "input_uri": "na",
        "output_prefix": "na",
        "params": {}
    }
    _ = send_message(sqs, url, payload)

    # Receive it with a short visibility timeout
    msgs = receive_messages(sqs, url, max_messages=1, wait_seconds=5, visibility_timeout=initial_vt)
    if not msgs:
        print("  WARN: no messages available for change_visibility test")
        return

    task, rh, tries = parse_message(msgs[0])
    tid = task.get("task_id")
    print(f"  received task_id={tid} tries={tries}")

    # Immediately bump the visibility to a longer window
    change_visibility(sqs, url, rh, bump_to)
    start = time.time()
    invisible_ok = True

    # During the bumped window, the same message should NOT be received
    check_until = start + (bump_to - 2)  # leave a little buffer
    while time.time() < check_until:
        probe = receive_messages(sqs, url, max_messages=10, wait_seconds=1)
        if not probe:
            continue
        # Look specifically for OUR message by task_id
        found_ours = False
        for raw in probe:
            try:
                t2, rh2, _ = parse_message(raw)
                if t2.get("task_id") == tid:
                    found_ours = True
                    # We didn't expect to see it; put it back by NOT deleting
                    print("  ERROR: message became visible during bumped window")
                    invisible_ok = False
                    break
            except Exception:
                # ignore stray parsing failures in probe
                pass
        time.sleep(0.2)

    print(f"  stayed invisible during bump: {'yes' if invisible_ok else 'no'}")

    # Wait for the bumped VT to lapse (ensure > bump_to seconds since bump)
    remaining = bump_to - (time.time() - start)
    if remaining > 0:
        time.sleep(remaining + 1.0)

    # Now it should reappear; consume and delete
    reappeared = False
    deadline = time.time() + 10
    while time.time() < deadline:
        probe2 = receive_messages(sqs, url, max_messages=10, wait_seconds=2)
        if not probe2:
            continue
        for raw in probe2:
            try:
                t3, rh3, _ = parse_message(raw)
                if t3.get("task_id") == tid:
                    delete_message(sqs, url, rh3)
                    reappeared = True
                    print("  reappeared after bump and deleted ✓")
                    break
            except Exception:
                pass
        if reappeared:
            break

    if not reappeared:
        print("  WARN: message did not reappear as expected after bumped visibility timeout")


def tc_queue_stats(sqs, url: str):
    print(f"[STATS] -> {url}")
    before = get_queue_stats(sqs, url, include_extra=True)
    print(f"  before: {before}")

    # small activity to change counters a bit
    _ = send_message(sqs, url, {
        "job_id":"job-stats","task_id":"t-stats","step":"STATS",
        "input_uri":"s3://dummy/in","output_prefix":"s3://dummy/out","params":{}
    })
    msgs = receive_messages(sqs, url, max_messages=1, wait_seconds=2, visibility_timeout=10)
    if msgs:
        _, rh, _ = parse_message(msgs[0])
        delete_message(sqs, url, rh)

    after = get_queue_stats(sqs, url, include_extra=True)
    print(f"  after : {after}")

# ---------- baseline tests ----------

def tc_smoke(sqs, url: str, step: str):
    print(f"[SMOKE] -> {url}")
    payload = {
        "job_id": "job-smoke",
        "task_id": f"t-{step.lower()}-1",
        "step": step,
        "input_uri": "s3://dummy/input",
        "output_prefix": "s3://dummy/output",
        "params": {},
    }
    mid = send_message(sqs, url, payload)
    print(f"  sent: {mid}")

    msgs = receive_messages(sqs, url, max_messages=1, wait_seconds=10, visibility_timeout=30)
    if not msgs:
        print("  WARN: no messages returned")
        return

    task, rh, tries = parse_message(msgs[0])
    print(f"  received tries={tries} task_id={task.get('task_id')}")
    # visibility_heartbeat uses extend_visibility_loop internally
    with visibility_heartbeat(sqs, url, rh, base_timeout=30, heartbeat_every=10):
        time.sleep(2)
    delete_message(sqs, url, rh)
    print("  deleted ✓")

def tc_batch(sqs, url: str, step: str):
    print(f"[BATCH] -> {url}")
    payloads = []
    for i in range(12):
        payloads.append({
            "job_id": "job-batch",
            "task_id": f"tb-{step.lower()}-{i}",
            "step": step,
            "input_uri": "s3://dummy/input",
            "output_prefix": "s3://dummy/output",
            "params": {"i": i},
        })
    mids = send_messages_batch(sqs, url, payloads, fifo_group_id=None, dedup_from_task_id=True)
    print(f"  sent {len(mids)} ids ✓")

def tc_delayed_requeue(sqs, url: str, step: str, delay: int = 5):
    print(f"[BACKOFF] -> {url} (delay={delay}s)")
    payload = {
        "job_id": "job-backoff",
        "task_id": f"t-{step.lower()}-backoff",
        "step": step,
        "input_uri": "s3://dummy/in",
        "output_prefix": "s3://dummy/out",
        "params": {},
    }
    mid = send_message(sqs, url, payload)
    print(f"  sent: {mid}")

    msgs = receive_messages(sqs, url, wait_seconds=5, visibility_timeout=20)
    if not msgs:
        print("  WARN: no messages returned")
        return
    task, rh, tries = parse_message(msgs[0])
    print(f"  got try={tries}, requeueing with delay then deleting original")
    requeue_with_delay(sqs, url, task, delay_seconds=delay)
    delete_message(sqs, url, rh)

    # Wait for delayed copy to show back up
    deadline = time.time() + delay + 20
    while time.time() < deadline:
        msgs2 = receive_messages(sqs, url, wait_seconds=2)
        if msgs2:
            t2, rh2, tries2 = parse_message(msgs2[0])
            print(f"  delayed copy arrived (tries={tries2}), deleting ✓")
            delete_message(sqs, url, rh2)
            return
    print("  WARN: delayed copy did not arrive in time")

# ---------- NEW: direct heartbeat (extend_visibility_loop) ----------

def tc_direct_heartbeat(sqs, url: str, step: str):
    """
    Uses extend_visibility_loop directly.
    Plan:
      - Send & receive one message with short base_timeout=6, heartbeat_every=2
      - Start extend_visibility_loop in a thread for ~7s
      - Poll the queue in parallel; the message should stay invisible
      - Stop heartbeat, wait 6s; message should be visible again → delete
    """
    print(f"[HEARTBEAT-DIRECT] -> {url}")
    mid = send_message(sqs, url, {
        "job_id":"job-hb","task_id":f"t-{step.lower()}-hb","step":step,
        "input_uri":"na","output_prefix":"na","params":{}
    })
    print(f"  sent: {mid}")

    msgs = receive_messages(sqs, url, max_messages=1, wait_seconds=5, visibility_timeout=2)
    if not msgs:
        print("  WARN: no messages to heartbeat")
        return
    _, rh, _ = parse_message(msgs[0])

    # run heartbeat 7 seconds
    stop = {"v": False}
    def should_continue(): return not stop["v"]
    t = threading.Thread(
        target=extend_visibility_loop,
        args=(sqs, url, rh, 6, 2, should_continue),
        daemon=True
    )
    t.start()

    invisible = True
    t0 = time.time()
    while time.time() - t0 < 7:
        probe = receive_messages(sqs, url, max_messages=1, wait_seconds=1)
        if probe:
            invisible = False
            # put back anything we accidentally pulled (shouldn't happen if invisibility is maintained)
            _, rh_probe, _ = parse_message(probe[0])
            # do NOT delete; just let it reappear later
        time.sleep(0.5)

    stop["v"] = True
    t.join(timeout=2)
    print(f"  stayed invisible during heartbeat: {'yes' if invisible else 'no'}")

    # After heartbeat stops, wait for visibility to lapse (~6s)
    time.sleep(7)

    # Now the message should be visible again; consume & delete it
    msgs2 = receive_messages(sqs, url, max_messages=1, wait_seconds=5)
    if msgs2:
        _, rh2, _ = parse_message(msgs2[0])
        delete_message(sqs, url, rh2)
        print("  reappeared after heartbeat and deleted ✓")
    else:
        print("  WARN: message did not reappear as expected")

# ---------- NEW: poison using is_poison_pill() ----------

def tc_poison_with_helper(sqs, url: str, dlq_url: str, step: str, max_receives: int = 5, hard_timeout_s: int = 120):
    print(f"[POISON→DLQ (helper)] -> {url} (threshold={max_receives})")

    # optional: purge to isolate
    try:
        print("  purging queue…")
        sqs.purge_queue(QueueUrl=url)
        time.sleep(2)
    except Exception as e:
        print(f"  WARN purge failed/denied: {e}")

    unique_task_id = f"t-{step.lower()}-poison-{int(time.time())}"
    payload = {
        "job_id": "job-poison",
        "task_id": unique_task_id,
        "step": step,
        "input_uri": "na",
        "output_prefix": "na",
        "params": {},
    }
    _ = send_message(sqs, url, payload)
    print(f"  sent poison task_id={unique_task_id}")

    def is_target(raw):
        try:
            body = raw.get("Body", "")
            parsed = json.loads(body) if isinstance(body, str) else {}
            if isinstance(parsed, dict) and "Message" in parsed:
                inner = parsed["Message"]
                parsed = json.loads(inner) if isinstance(inner, str) else inner
            return isinstance(parsed, dict) and parsed.get("task_id") == unique_task_id
        except Exception:
            return False

    deadline = time.time() + hard_timeout_s
    while time.time() < deadline:
        msgs = receive_messages(sqs, url, max_messages=10, wait_seconds=2, visibility_timeout=1)
        if not msgs:
            continue

        m = next((x for x in msgs if is_target(x)), None)
        if not m:
            # Delete strays only if this is a dedicated test queue; else skip
            continue

        task, rh, tries = parse_message(m)
        print(f"  receive_count(target)={tries}")

        if is_poison_pill(tries, max_count=max_receives):
            print("  threshold reached → move_to_dlq + delete original")
            move_to_dlq(sqs, dlq_url, task, reason="poison-pill-test")
            delete_message(sqs, url, rh)
            break

        # let visibility lapse to force re-delivery
        time.sleep(1.3)
    else:
        print(f"  ERROR: timed out after {hard_timeout_s}s without reaching threshold")

    # verify DLQ (peek)
    msgs_dlq = receive_messages(sqs, dlq_url, wait_seconds=3)
    if msgs_dlq:
        tdlq, _, _ = parse_message(msgs_dlq[0])
        tid = (tdlq.get("original") or {}).get("task_id")
        print(f"  DLQ has message for task_id={tid} ✓")

# ---------- main ----------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", required=True, help="Queue name prefix to test")
    parser.add_argument("--include", nargs="*", default=None, help="Optional explicit queue names to include")
    parser.add_argument("--skip-poison", action="store_true", help="Skip poison→DLQ test (helper)")
    parser.add_argument("--skip-batch", action="store_true", help="Skip batch test")
    parser.add_argument("--skip-backoff", action="store_true", help="Skip delayed requeue test")
    parser.add_argument("--skip-stats", action="store_true", help="Skip get_queue_stats test")
    parser.add_argument("--skip-heartbeat-direct", action="store_true", help="Skip direct extend_visibility_loop test")
    parser.add_argument("--skip-change-vis", action="store_true", help="Skip explicit change_visibility test")

    args = parser.parse_args()

    region, prof = ensure_env()
    print(f"Using AWS region={region} profile={prof}")

    sqs = get_sqs_client(region)

    qmap = get_queue_url_map(sqs, args.prefix)
    if not qmap:
        print(f"No queues found with prefix {args.prefix}")
        sys.exit(1)

    # Find DLQ
    dlq_name = f"{args.prefix.rstrip('-')}-dlq"
    dlq_url = qmap.get(dlq_name)
    if not dlq_url:
        for n, u in qmap.items():
            if n.endswith("-dlq"):
                dlq_url = u
                break

    # Work queues
    candidates = [n for n in sorted(qmap) if n.endswith("-requested")]
    if args.include:
        candidates = [n for n in args.include if n in qmap]

    if not candidates:
        print("No *-requested queues found to test.")
        print("Queues discovered:", ", ".join(sorted(qmap)))
        sys.exit(1)

    print("Discovered queues:")
    for n in candidates:
        print(f"  - {n} -> {qmap[n]}")
    if dlq_url:
        print(f"DLQ: {dlq_url}")
    else:
        print("WARN: No DLQ found — poison test will be skipped.")
        args.skip_poison = True

    for qname in candidates:
        url = qmap[qname]
        step = name_for_step(qname)

        try:
            if not args.skip_stats:
                tc_queue_stats(sqs, url)
            tc_smoke(sqs, url, step)
            if not args.skip_batch:
                tc_batch(sqs, url, step)
            if not args.skip_backoff:
                tc_delayed_requeue(sqs, url, step, delay=5)
            if not args.skip_change_vis:
                tc_change_visibility(sqs, url, step, initial_vt=2, bump_to=8)
            if not args.skip_heartbeat_direct:
                tc_direct_heartbeat(sqs, url, step)
            if not args.skip_poison and dlq_url:
                tc_poison_with_helper(sqs, url, dlq_url, step, max_receives=5)
        except Exception as e:
            print(f"[ERROR] while testing {qname}: {e}")

    print("Done.")

if __name__ == "__main__":
    main()
