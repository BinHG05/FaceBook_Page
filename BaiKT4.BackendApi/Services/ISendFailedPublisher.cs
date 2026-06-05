using BaiKT4.Contracts.Models;

namespace BaiKT4.BackendApi.Services;

public interface ISendFailedPublisher
{
    Task PublishAsync(SendFailedEvent sendFailedEvent, CancellationToken cancellationToken);
}
