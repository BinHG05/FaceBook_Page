using BaiKT4.WebhookService.Models;

namespace BaiKT4.WebhookService.Services;

public interface IFacebookWebhookNormalizer
{
    IReadOnlyList<NormalizedWebhookEvent> Normalize(string rawPayload);
}
