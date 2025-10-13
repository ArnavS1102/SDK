# Load Test Recipe

How to verify autoscaling works:

## Setup
1. Deploy your service with KEDA enabled
2. Set `minReplicaCount: 0`, `maxReplicaCount: 10`, `targetValue: 1`

## Test
1. **Inject 100 messages** into the input queue
   ```bash
   python scripts/generate_test_messages.py --count 100 --queue-url $QUEUE_URL
   ```

2. **Watch scaling**
   ```bash
   kubectl get pods -w
   ```
   - Should scale from 0 → ~100 replicas (or max)
   - Each replica processes ~1 message

3. **Wait for queue drain**
   - All messages processed
   - After cooldown (120s), should scale back to 0

## Verify
- Check CloudWatch: SQS `ApproximateNumberOfMessages` drops to 0
- Check logs: all messages processed successfully
- Check DLQ: should be empty
- Check DB: all tasks marked DONE

