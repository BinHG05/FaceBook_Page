using System.Text.Json.Nodes;

namespace BaiKT4.Contracts.Models;

public sealed record NormalizedWebhookEvent
{
    public required string EventId { get; init; }

    public required string Source { get; init; }

    public required string EventType { get; init; }

    public required string Topic { get; init; }

    public string? SourceEventId { get; init; }

    public string? PageId { get; init; }

    public string? PageName { get; init; }

    public string? ObjectType { get; init; }

    public DateTimeOffset ReceivedAt { get; init; }

    public DateTimeOffset? OccurredAt { get; init; }

    public FacebookActor? Actor { get; init; }

    public FacebookTarget? Target { get; init; }

    public string? MessageText { get; init; }

    public JsonObject Metadata { get; init; } = [];

    public JsonNode? RawPayload { get; init; }
}
