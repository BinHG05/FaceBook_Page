using FbStream.Contracts.Models;

namespace FbStream.CoreService.Services;

public interface IReplyCommandPublisher
{
    Task PublishAsync(IEnumerable<ReplyCommand> commands, CancellationToken cancellationToken);
}
