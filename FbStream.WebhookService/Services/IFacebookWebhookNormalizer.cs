using FbStream.Contracts.Models;

namespace FbStream.WebhookService.Services;

public interface IFacebookWebhookNormalizer
{
    IReadOnlyList<NormalizedWebhookEvent> Normalize(string rawPayload);
}
