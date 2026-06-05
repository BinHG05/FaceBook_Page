using BaiKT4.Contracts.Models;

namespace BaiKT4.WebhookService.Services;

public interface IFacebookWebhookNormalizer
{
    IReadOnlyList<NormalizedWebhookEvent> Normalize(string rawPayload);
}
