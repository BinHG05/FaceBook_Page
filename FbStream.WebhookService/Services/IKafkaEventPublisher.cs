using FbStream.Contracts.Models;

namespace FbStream.WebhookService.Services;

public interface IKafkaEventPublisher
{
    Task PublishAsync(IEnumerable<NormalizedWebhookEvent> events, CancellationToken cancellationToken);
}
