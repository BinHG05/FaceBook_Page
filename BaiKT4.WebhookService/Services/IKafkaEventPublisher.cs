using BaiKT4.Contracts.Models;

namespace BaiKT4.WebhookService.Services;

public interface IKafkaEventPublisher
{
    Task PublishAsync(IEnumerable<NormalizedWebhookEvent> events, CancellationToken cancellationToken);
}
