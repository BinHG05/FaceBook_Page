using BaiKT4.Contracts.Models;

namespace BaiKT4.BackendApi.Services;

public interface IFacebookGraphService
{
    Task<object> GetPostsAsync(CancellationToken cancellationToken);

    Task<object> CreatePostAsync(string message, CancellationToken cancellationToken);

    Task<object> GetCommentsAsync(string postId, CancellationToken cancellationToken);

    Task DispatchCommandAsync(ReplyCommand command, CancellationToken cancellationToken);
}
