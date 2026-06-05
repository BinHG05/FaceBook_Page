using System.Text.Json.Nodes;

namespace BaiKT4.Contracts.Models;

public sealed record ReplyCommand
{
    public required string CommandId { get; init; }

    public required string EventId { get; init; }

    public required string PageId { get; init; }

    public required string UserId { get; init; }

    public string? CommentId { get; init; }

    public string? PostId { get; init; }

    public required string ActionType { get; init; }

    public string? ReplyMessage { get; init; }

    public required string IdempotencyKey { get; init; }

    public string Status { get; init; } = "received";

    public int RetryCount { get; init; }

    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;

    public JsonObject Metadata { get; init; } = [];
}
