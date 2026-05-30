using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using BaiKT4.WebhookService.Models;
using BaiKT4.WebhookService.Options;
using Microsoft.Extensions.Options;

namespace BaiKT4.WebhookService.Services;

public sealed class FacebookWebhookNormalizer : IFacebookWebhookNormalizer
{
    private readonly KafkaOptions _kafkaOptions;

    public FacebookWebhookNormalizer(IOptions<KafkaOptions> kafkaOptions)
    {
        _kafkaOptions = kafkaOptions.Value;
    }

    public IReadOnlyList<NormalizedWebhookEvent> Normalize(string rawPayload)
    {
        using var document = JsonDocument.Parse(rawPayload);
        var root = document.RootElement;
        var normalizedEvents = new List<NormalizedWebhookEvent>();
        var receivedAt = DateTimeOffset.UtcNow;
        var objectType = root.TryGetProperty("object", out var objectElement)
            ? objectElement.GetString()
            : null;

        if (!root.TryGetProperty("entry", out var entryArray) || entryArray.ValueKind != JsonValueKind.Array)
        {
            return normalizedEvents;
        }

        foreach (var entry in entryArray.EnumerateArray())
        {
            var pageId = TryGetString(entry, "id");
            var pageName = TryGetString(entry, "name");
            var entryUnixTime = TryGetInt64(entry, "time");
            var entryOccurredAt = entryUnixTime.HasValue
                ? DateTimeOffset.FromUnixTimeSeconds(entryUnixTime.Value)
                : (DateTimeOffset?)null;

            if (entry.TryGetProperty("changes", out var changes) && changes.ValueKind == JsonValueKind.Array)
            {
                foreach (var change in changes.EnumerateArray())
                {
                    normalizedEvents.Add(CreateChangeEvent(change, pageId, pageName, objectType, entryOccurredAt, receivedAt));
                }
            }

            if (entry.TryGetProperty("messaging", out var messagingArray) && messagingArray.ValueKind == JsonValueKind.Array)
            {
                foreach (var message in messagingArray.EnumerateArray())
                {
                    normalizedEvents.Add(CreateMessagingEvent(message, pageId, pageName, objectType, entryOccurredAt, receivedAt));
                }
            }
        }

        return normalizedEvents;
    }

    private NormalizedWebhookEvent CreateChangeEvent(
        JsonElement change,
        string? pageId,
        string? pageName,
        string? objectType,
        DateTimeOffset? entryOccurredAt,
        DateTimeOffset receivedAt)
    {
        var field = TryGetString(change, "field") ?? "unknown";
        var value = change.TryGetProperty("value", out var valueElement)
            ? valueElement
            : default;
        var verb = TryGetString(value, "verb");
        var item = TryGetString(value, "item");
        var eventType = field == "feed" && item == "comment"
            ? verb == "add" ? "comment.created" : $"comment.{verb ?? "updated"}"
            : $"{field}.{verb ?? item ?? "updated"}";

        var actor = value.ValueKind == JsonValueKind.Undefined
            ? null
            : new FacebookActor(
                TryNestedString(value, "from", "id"),
                TryNestedString(value, "from", "name"));

        var target = new FacebookTarget(
            TryFirstNonEmpty(
                TryGetString(value, "comment_id"),
                TryGetString(value, "post_id"),
                TryGetString(value, "item_id")),
            TryFirstNonEmpty(
                TryGetString(value, "post_id"),
                TryGetString(value, "parent_id")));

        return new NormalizedWebhookEvent
        {
            EventId = Guid.NewGuid().ToString("N"),
            Source = "facebook",
            Topic = _kafkaOptions.TopicName,
            EventType = eventType,
            SourceEventId = TryFirstNonEmpty(TryGetString(value, "comment_id"), TryGetString(value, "post_id")),
            PageId = pageId,
            PageName = pageName,
            ObjectType = objectType ?? "page",
            ReceivedAt = receivedAt,
            OccurredAt = TryParseDateTime(TryGetString(value, "created_time")) ?? entryOccurredAt,
            Actor = actor,
            Target = target,
            MessageText = TryGetString(value, "message"),
            Metadata = new JsonObject
            {
                ["field"] = field,
                ["item"] = item,
                ["verb"] = verb
            },
            RawPayload = JsonNode.Parse(change.GetRawText())
        };
    }

    private NormalizedWebhookEvent CreateMessagingEvent(
        JsonElement message,
        string? pageId,
        string? pageName,
        string? objectType,
        DateTimeOffset? entryOccurredAt,
        DateTimeOffset receivedAt)
    {
        var messageType = message.TryGetProperty("message", out var messageElement) ? "message.received" :
            message.TryGetProperty("delivery", out _) ? "message.delivered" :
            message.TryGetProperty("read", out _) ? "message.read" :
            "message.unknown";

        var sourceEventId = TryNestedString(message, "message", "mid");
        var text = TryNestedString(message, "message", "text");
        var timestampMilliseconds = TryGetInt64(message, "timestamp");
        var occurredAt = timestampMilliseconds.HasValue
            ? DateTimeOffset.FromUnixTimeMilliseconds(timestampMilliseconds.Value)
            : entryOccurredAt;

        return new NormalizedWebhookEvent
        {
            EventId = Guid.NewGuid().ToString("N"),
            Source = "facebook",
            Topic = _kafkaOptions.TopicName,
            EventType = messageType,
            SourceEventId = sourceEventId,
            PageId = pageId,
            PageName = pageName,
            ObjectType = objectType ?? "page",
            ReceivedAt = receivedAt,
            OccurredAt = occurredAt,
            Actor = new FacebookActor(
                TryNestedString(message, "sender", "id"),
                null),
            Target = new FacebookTarget(
                TryNestedString(message, "recipient", "id"),
                null),
            MessageText = text,
            Metadata = new JsonObject
            {
                ["mid"] = sourceEventId,
                ["hasAttachments"] = message.TryGetProperty("message", out var messageNode) &&
                    messageNode.TryGetProperty("attachments", out _)
            },
            RawPayload = JsonNode.Parse(message.GetRawText())
        };
    }

    private static string? TryGetString(JsonElement element, string propertyName)
    {
        if (element.ValueKind == JsonValueKind.Undefined || !element.TryGetProperty(propertyName, out var property))
        {
            return null;
        }

        return property.ValueKind switch
        {
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Number => property.GetRawText(),
            JsonValueKind.True => bool.TrueString,
            JsonValueKind.False => bool.FalseString,
            _ => null
        };
    }

    private static long? TryGetInt64(JsonElement element, string propertyName)
    {
        if (element.ValueKind == JsonValueKind.Undefined || !element.TryGetProperty(propertyName, out var property))
        {
            return null;
        }

        if (property.ValueKind == JsonValueKind.Number && property.TryGetInt64(out var numberValue))
        {
            return numberValue;
        }

        return long.TryParse(property.GetRawText(), out var parsedValue) ? parsedValue : null;
    }

    private static string? TryNestedString(JsonElement element, string parentPropertyName, string childPropertyName)
    {
        if (!element.TryGetProperty(parentPropertyName, out var parentElement))
        {
            return null;
        }

        return TryGetString(parentElement, childPropertyName);
    }

    private static DateTimeOffset? TryParseDateTime(string? rawDateTime)
    {
        if (string.IsNullOrWhiteSpace(rawDateTime))
        {
            return null;
        }

        return DateTimeOffset.TryParse(rawDateTime, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var value)
            ? value
            : null;
    }

    private static string? TryFirstNonEmpty(params string?[] candidates)
    {
        return candidates.FirstOrDefault(candidate => !string.IsNullOrWhiteSpace(candidate));
    }
}
