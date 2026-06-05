namespace BaiKT4.Contracts.Models;

public sealed record EventProcessingLog
{
    public required string EventId { get; init; }

    public string? UserId { get; init; }

    public string? Content { get; init; }

    public required string Intent { get; init; }

    public required string Sentiment { get; init; }

    public required string Status { get; init; }

    public required string ActionTaken { get; init; }

    public string? ReviewReason { get; init; }

    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
}
