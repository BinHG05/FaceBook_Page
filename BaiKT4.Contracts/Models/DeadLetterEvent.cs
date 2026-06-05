using System.Text.Json.Nodes;

namespace BaiKT4.Contracts.Models;

public sealed record DeadLetterEvent
{
    public required string DeadLetterId { get; init; }

    public required ReplyCommand Command { get; init; }

    public required string Reason { get; init; }

    public int RetryCount { get; init; }

    public DateTimeOffset MovedAt { get; init; } = DateTimeOffset.UtcNow;

    public JsonObject Metadata { get; init; } = [];
}
