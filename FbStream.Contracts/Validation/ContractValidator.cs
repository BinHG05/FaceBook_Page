using FbStream.Contracts.Models;

namespace FbStream.Contracts.Validation;

public class ContractValidationException : Exception
{
    public string Subject { get; }

    public ContractValidationException(string subject, string message)
        : base($"[Data Contract Violation on '{subject}'] {message}")
    {
        Subject = subject;
    }
}

public static class ContractValidator
{
    private static readonly HashSet<string> AllowedActionTypes =
    [
        "auto_reply",
        "hide_comment",
        "blacklist",
        "alert_admin",
        "none"
    ];

    public static void Validate(NormalizedWebhookEvent evt)
    {
        ArgumentNullException.ThrowIfNull(evt);

        if (string.IsNullOrWhiteSpace(evt.EventId))
        {
            throw new ContractValidationException("raw_events", "Field 'EventId' must not be empty.");
        }

        if (string.IsNullOrWhiteSpace(evt.Source))
        {
            throw new ContractValidationException("raw_events", "Field 'Source' must not be empty (e.g. 'facebook').");
        }

        if (string.IsNullOrWhiteSpace(evt.EventType))
        {
            throw new ContractValidationException("raw_events", "Field 'EventType' must not be empty (e.g. 'feed_comment').");
        }

        if (string.IsNullOrWhiteSpace(evt.Topic))
        {
            throw new ContractValidationException("raw_events", "Field 'Topic' must not be empty.");
        }

        if (evt.ReceivedAt == default)
        {
            throw new ContractValidationException("raw_events", "Field 'ReceivedAt' timestamp must be set.");
        }
    }

    public static void Validate(ReplyCommand cmd)
    {
        ArgumentNullException.ThrowIfNull(cmd);

        if (string.IsNullOrWhiteSpace(cmd.CommandId))
        {
            throw new ContractValidationException("reply_commands", "Field 'CommandId' must not be empty.");
        }

        if (string.IsNullOrWhiteSpace(cmd.EventId))
        {
            throw new ContractValidationException("reply_commands", "Field 'EventId' must not be empty.");
        }

        if (string.IsNullOrWhiteSpace(cmd.PageId))
        {
            throw new ContractValidationException("reply_commands", "Field 'PageId' must not be empty.");
        }

        if (string.IsNullOrWhiteSpace(cmd.UserId))
        {
            throw new ContractValidationException("reply_commands", "Field 'UserId' must not be empty.");
        }

        if (string.IsNullOrWhiteSpace(cmd.IdempotencyKey))
        {
            throw new ContractValidationException("reply_commands", "Field 'IdempotencyKey' must not be empty for deduplication.");
        }

        if (!AllowedActionTypes.Contains(cmd.ActionType))
        {
            throw new ContractValidationException("reply_commands",
                $"Invalid ActionType '{cmd.ActionType}'. Allowed values: {string.Join(", ", AllowedActionTypes)}");
        }
    }
}

