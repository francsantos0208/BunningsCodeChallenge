namespace EventStreamDeduplicator.Tests;

public sealed class DeDuplicationBufferTests
{
    [Fact]
    public void DeDupe_OutOfOrderAndDuplicates_ReturnsOrderedAndProcessesEachEventIdOnce()
    {
        var buffer = new DeDuplicationBuffer();

        // Out-of-order input, includes a duplicate eventId ("B")
        var batch = new List<Payload>
        {
            new("C", new DateTimeOffset(2026, 1, 1, 12, 0, 2, TimeSpan.Zero), "c"),
            new("B", new DateTimeOffset(2026, 1, 1, 12, 0, 1, TimeSpan.Zero), "b"),
            new("A", new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero), "a"),
            new("B", new DateTimeOffset(2026, 1, 1, 12, 0, 1, TimeSpan.Zero), "b-dup"),
        };

        var result = buffer.DeDupe(batch);

        Assert.Equal(new[] { "A", "B", "C" }, result.Select(x => x.EventId).ToArray());
        Assert.True(result.Select(x => x.Timestamp).SequenceEqual(result.Select(x => x.Timestamp).OrderBy(x => x)));
        Assert.Equal(new DateTimeOffset(2026, 1, 1, 12, 0, 2, TimeSpan.Zero), buffer.LastProcessedTimestamp);
    }

    [Fact]
    public void DeDupe_WatermarkRejectsOlderOrEqualTimestamps_EvenIfNewEventId()
    {
        var buffer = new DeDuplicationBuffer();

        // First batch processes up to T=10
        var batch1 = new List<Payload>
        {
            new("A", new DateTimeOffset(2026, 1, 1, 12, 0, 10, TimeSpan.Zero), "a")
        };

        var r1 = buffer.DeDupe(batch1);
        Assert.Single(r1);
        Assert.Equal("A", r1[0].EventId);
        Assert.Equal(new DateTimeOffset(2026, 1, 1, 12, 0, 10, TimeSpan.Zero), buffer.LastProcessedTimestamp);

        // Second batch contains *new* eventIds but with older and equal timestamps => must be rejected
        var batch2 = new List<Payload>
        {
            new("B", new DateTimeOffset(2026, 1, 1, 12, 0,  9, TimeSpan.Zero), "older"),
            new("C", new DateTimeOffset(2026, 1, 1, 12, 0, 10, TimeSpan.Zero), "equal"),
            new("D", new DateTimeOffset(2026, 1, 1, 12, 0, 11, TimeSpan.Zero), "newer"),
        };

        var r2 = buffer.DeDupe(batch2);

        Assert.Single(r2);
        Assert.Equal("D", r2[0].EventId);
        Assert.Equal(new DateTimeOffset(2026, 1, 1, 12, 0, 11, TimeSpan.Zero), buffer.LastProcessedTimestamp);
    }

    [Fact]
    public void DeDupe_RejectsAlreadyProcessedEventIdAcrossBatches_AndHandlesSameTimestampStrictness()
    {
        var buffer = new DeDuplicationBuffer();

        // Batch 1: two events with same timestamp; only one can pass due to strict watermark (>).
        // Ordering tie-breaker is EventId ordinal asc, so "A" will process, then "B" becomes outdated immediately.
        var t = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

        var batch1 = new List<Payload>
        {
            new("B", t, "b"),
            new("A", t, "a")
        };

        var r1 = buffer.DeDupe(batch1);

        Assert.Single(r1);
        Assert.Equal("A", r1[0].EventId);
        Assert.Equal(t, buffer.LastProcessedTimestamp);

        // Batch 2: "A" reappears with newer timestamp (should still be rejected: already processed),
        // and "C" with newer timestamp should be accepted.
        var batch2 = new List<Payload>
        {
            new("A", t.AddSeconds(10), "a-again-new-ts"),
            new("C", t.AddSeconds(1), "c")
        };

        var r2 = buffer.DeDupe(batch2);

        Assert.Single(r2);
        Assert.Equal("C", r2[0].EventId);
        Assert.Equal(t.AddSeconds(1), buffer.LastProcessedTimestamp);
    }
}
