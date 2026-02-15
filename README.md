# Event Stream De-Duplicator (In-Memory)

## Problem Summary
We receive events in batches. Events can be duplicated, delayed, and out-of-order. We must return events that should be processed now, ensuring:
1. Accept events in any order
2. Each `eventId` processed exactly once
3. Process in timestamp order, and maintain a watermark:
   - Once an event with timestamp `T` is processed, any future event with timestamp `<= T` is **outdated** and must not be processed (even if new `eventId`).
   - Only events with timestamp strictly greater than the last processed timestamp are eligible.

## Assumptions
1. **Single logical consumer**: The buffer is used by one logical processor (no distributed coordination required).
2. **In-memory state only**: State is not persisted; restarting the process loses watermark and processed IDs.
3. **Exactly-once by eventId means "processed" set**:
   - If an `eventId` has ever been processed, it is never processed again, regardless of timestamp.
4. **Strict timestamp rule is enforced**:
   - If multiple events share the same timestamp, only the first processed at that timestamp can pass.
   - The remaining events at the same timestamp become outdated immediately after watermark advances to that timestamp.
5. **Deterministic ordering within a batch**:
   - Events are ordered by `(timestamp asc, eventId ordinal asc)` before processing to keep results stable.
6. Input batches can be any size; design does not depend on batch size.

## Design
`DeDuplicationBuffer` maintains:
- `LastProcessedTimestamp` (watermark)
- A `HashSet<string>` of processed `eventId`s

`DeDupe(batch)`:
1. Filters out events that are already processed
2. Sorts remaining events by timestamp, then eventId
3. Iterates in order and processes only events with timestamp strictly greater than the current watermark
4. Updates watermark and `processedEventIds` as it processes

## Time/Space Complexity (Big-O)

Let `n` = number of events in the input batch.

### Time Complexity (per batch)
- Filtering: `O(n)` average (HashSet membership is `O(1)` average)
- Sorting eligible events: `O(k log k)` where `k <= n`
- Single pass processing: `O(k)`

**Overall:** `O(n + k log k)` which is `O(n log n)` in the worst case.

### Space Complexity
- Buffer state: `O(P)` where `P` is the number of processed unique eventIds stored in memory
- Per batch working list for eligible items: `O(k)` worst case `O(n)`

**Overall:** `O(P + n)` (dominant long-lived memory is `O(P)`).

## How to run
From the repo root:

```bash
dotnet test
