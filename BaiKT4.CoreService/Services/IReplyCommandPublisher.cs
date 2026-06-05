using BaiKT4.Contracts.Models;

namespace BaiKT4.CoreService.Services;

public interface IReplyCommandPublisher
{
    Task PublishAsync(IEnumerable<ReplyCommand> commands, CancellationToken cancellationToken);
}
