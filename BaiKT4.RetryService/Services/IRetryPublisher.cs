using BaiKT4.Contracts.Models;

namespace BaiKT4.RetryService.Services;

public interface IRetryPublisher
{
    Task PublishRetryAsync(ReplyCommand command, CancellationToken cancellationToken);

    Task PublishDeadLetterAsync(DeadLetterEvent deadLetterEvent, CancellationToken cancellationToken);
}
