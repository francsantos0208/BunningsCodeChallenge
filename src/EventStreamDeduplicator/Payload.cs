namespace EventStreamDeduplicator;

/// <summary>
/// Represents a single event payload from the stream.
/// </summary>
public sealed record Payload(
    string EventId,
    DateTimeOffset Timestamp,
    string Message
);