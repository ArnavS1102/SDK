# Contract Checklist

Verify your service obeys the SDK contract:

1. **Valid Message Schema**
   - Message has required fields: `job_id`, `task_id`, `input_uri`, `output_prefix`, `params`
   - Runner doesn't reject it

2. **Output Files Exist**
   - After processing, check `output_prefix/result.*` exists
   - Check `output_prefix/metrics.json` exists

3. **Idempotency Works**
   - Send same message twice
   - Second run should skip work and ACK immediately

4. **Publishes Next Step**
   - If not final step, verify next queue receives messages
   - Check `task_id` is deterministic (same input → same ID)

5. **Updates DB**
   - Task status moves: PENDING → PROCESSING → DONE
   - Job status becomes DONE when all tasks complete

