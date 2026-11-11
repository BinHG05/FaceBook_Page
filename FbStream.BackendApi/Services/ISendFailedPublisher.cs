using FbStream.Contracts.Models;

namespace FbStream.BackendApi.Services;

public interface ISendFailedPublisher
{
    Task PublishAsync(SendFailedEvent sendFailedEvent, CancellationToken cancellationToken);
}
