using System.Text.Json.Nodes;

namespace BaiKT4.Contracts.Models;

public sealed record SendFailedEvent
{
    public required string FailureId { get; init; }

    public required ReplyCommand Command { get; init; }

    public required string FailureReason { get; init; }

    public required string FailureType { get; init; }

    public int RetryCount { get; init; }

    public bool Retryable { get; init; } = true;

    public DateTimeOffset FailedAt { get; init; } = DateTimeOffset.UtcNow;

    public JsonObject Metadata { get; init; } = [];
}
