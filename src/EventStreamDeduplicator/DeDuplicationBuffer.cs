namespace EventStreamDeduplicator;

/// <summary>
/// In-memory de-duplication + watermark buffer for streaming events.
///
/// Rules:
/// - Exactly-once by EventId: once processed, never process again.
/// - Watermark: process only events with timestamp strictly greater than the last processed timestamp.
/// - Process results in non-decreasing timestamp order (in practice strictly increasing due to the strict watermark rule).
///
/// Not designed to be thread-safe (single logical consumer assumption) as this assumption was confirmed by email.
/// </summary>
public sealed class DeDuplicationBuffer
{
    private readonly HashSet<string> _processedEventIds;

    /// <summary>
    /// The most recent timestamp that has been processed (watermark).
    /// If null, nothing has been processed yet.
    /// </summary>
    public DateTimeOffset? LastProcessedTimestamp { get; private set; }

    public DeDuplicationBuffer(IEqualityComparer<string>? eventIdComparer = null)
    {
        _processedEventIds = new HashSet<string>(eventIdComparer ?? StringComparer.Ordinal);
    }

    /// <summary>
    /// De-duplicates and applies watermark rules to return the events that should be processed now.
    ///
    /// Input can be any order; output is ordered by timestamp ascending (then eventId ascending for stability).
    /// </summary>
    public IReadOnlyList<Payload> DeDupe(IReadOnlyCollection<Payload> batch)
    {
        if (batch is null) throw new ArgumentNullException(nameof(batch));

        // Step 1: keep only events that aren't already processed.
        var candidates = new List<Payload>(batch.Count);

        foreach (var e in batch)
        {
            if (string.IsNullOrWhiteSpace(e.EventId))
                throw new ArgumentException("Payload.EventId must be a non-empty string.", nameof(batch));

            if (!_processedEventIds.Contains(e.EventId))
            {
                candidates.Add(e);
            }
        }

        if (candidates.Count == 0)
            return Array.Empty<Payload>();

        // Step 2: sort to ensure timestamp order for processing
        candidates.Sort(static (a, b) =>
        {
            var tsCompare = a.Timestamp.CompareTo(b.Timestamp);
            if (tsCompare != 0) return tsCompare;
            return string.CompareOrdinal(a.EventId, b.EventId);
        });

        // Step 3: process in order, advancing watermark as we go
        var results = new List<Payload>(capacity: candidates.Count);
        var watermark = LastProcessedTimestamp;

        foreach (var e in candidates)
        {
            // If it became processed earlier in this same call due to duplicates in the batch, skip.
            if (_processedEventIds.Contains(e.EventId))
                continue;

            // Apply strict watermark rule:
            // eligible only if timestamp is strictly greater than last processed timestamp.
            if (watermark.HasValue)
            {
                if (e.Timestamp <= watermark.Value)
                    continue;
            }
            // If watermark is null, everything is eligible (first processed sets watermark)

            // Process "now": add to output and update state.
            results.Add(e);
            _processedEventIds.Add(e.EventId);
            watermark = e.Timestamp;
        }

        LastProcessedTimestamp = watermark;
        return results;
    }
}