using FbStream.Contracts.Models;

namespace FbStream.RetryService.Services;

public interface IRetryPublisher
{
    Task PublishRetryAsync(ReplyCommand command, CancellationToken cancellationToken);

    Task PublishDeadLetterAsync(DeadLetterEvent deadLetterEvent, CancellationToken cancellationToken);
}
