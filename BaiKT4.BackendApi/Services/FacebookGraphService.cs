using System.Net;
using System.Text;
using System.Text.Json;
using BaiKT4.BackendApi.Options;
using BaiKT4.Contracts.Models;
using Microsoft.Extensions.Options;

namespace BaiKT4.BackendApi.Services;

public sealed class FacebookGraphService : IFacebookGraphService
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly FacebookGraphOptions _options;
    private readonly ILogger<FacebookGraphService> _logger;
    private readonly object _circuitLock = new();
    private int _failureCount;
    private DateTimeOffset? _circuitOpenedUntil;

    public FacebookGraphService(
        IHttpClientFactory httpClientFactory,
        IOptions<FacebookGraphOptions> options,
        ILogger<FacebookGraphService> logger)
    {
        _httpClientFactory = httpClientFactory;
        _options = options.Value;
        _logger = logger;
    }

    public Task<object> GetPostsAsync(CancellationToken cancellationToken)
    {
        return GetAsync($"{_options.PageId}/posts?fields=id,message,created_time", cancellationToken);
    }

    public Task<object> CreatePostAsync(string message, CancellationToken cancellationToken)
    {
        return PostAsync($"{_options.PageId}/feed", new Dictionary<string, string>
        {
            ["message"] = message
        }, cancellationToken);
    }

    public Task<object> GetCommentsAsync(string postId, CancellationToken cancellationToken)
    {
        return GetAsync($"{postId}/comments?fields=id,message,from,created_time", cancellationToken);
    }

    public async Task DispatchCommandAsync(ReplyCommand command, CancellationToken cancellationToken)
    {
        // Kiểm tra xem Circuit Breaker có đang mở không (ngăn chặn gọi tiếp nếu API Facebook liên tục lỗi)
        if (IsCircuitOpen())
        {
            throw new InvalidOperationException("Facebook Graph API circuit breaker is open.");
        }

        // ==========================================
        // LUỒNG 1: BƯỚC 10.1: Chế độ giả lập (Simulate Mode) hoặc không cấu hình Token.
        // Giúp kiểm thử hệ thống mà không cần Token thật hoặc tránh bị rate limit từ Facebook.
        // ==========================================
        if (_options.SimulateMode || string.IsNullOrWhiteSpace(_options.PageAccessToken))
        {
            _logger.LogInformation("Simulated Facebook action {ActionType} for command {CommandId}.", command.ActionType, command.CommandId);
            ResetCircuit();
            return;
        }

        try
        {
            // ==========================================
            // LUỒNG 1: BƯỚC 10.2: Thực hiện hành động thật qua gọi HTTP POST API Facebook Graph.
            // ==========================================
            switch (command.ActionType)
            {
                case "hide_comment":
                    // Ẩn bình luận spam/tiêu cực
                    await PostAsync($"{command.CommentId}", new Dictionary<string, string>
                    {
                        ["is_hidden"] = "true"
                    }, cancellationToken);
                    break;
                case "auto_reply":
                    // Tự động bình luận phản hồi lại bình luận của người dùng
                    if (string.IsNullOrWhiteSpace(command.CommentId) || string.IsNullOrWhiteSpace(command.ReplyMessage))
                    {
                        throw new InvalidOperationException("Reply command requires commentId and replyMessage.");
                    }

                    // Gọi API: POST /{comment_id}/comments với message là nội dung trả lời
                    await PostAsync($"{command.CommentId}/comments", new Dictionary<string, string>
                    {
                        ["message"] = command.ReplyMessage
                    }, cancellationToken);
                    break;
                case "blacklist":
                case "alert_admin":
                    _logger.LogInformation("Action {ActionType} is handled as internal/manual workflow for command {CommandId}.", command.ActionType, command.CommandId);
                    break;
                default:
                    _logger.LogInformation("No Facebook dispatch required for action {ActionType}.", command.ActionType);
                    break;
            }

            ResetCircuit();
        }
        catch
        {
            RegisterFailure();
            throw;
        }
    }

    private async Task<object> GetAsync(string relativePath, CancellationToken cancellationToken)
    {
        if (_options.SimulateMode || string.IsNullOrWhiteSpace(_options.PageAccessToken))
        {
            return new
            {
                simulated = true,
                path = relativePath
            };
        }

        using var request = new HttpRequestMessage(HttpMethod.Get, BuildUrl(relativePath));
        return await SendAsync(request, cancellationToken);
    }

    private async Task<object> PostAsync(string relativePath, IReadOnlyDictionary<string, string> formData, CancellationToken cancellationToken)
    {
        if (_options.SimulateMode || string.IsNullOrWhiteSpace(_options.PageAccessToken))
        {
            return new
            {
                simulated = true,
                path = relativePath,
                payload = formData
            };
        }

        using var request = new HttpRequestMessage(HttpMethod.Post, BuildUrl(relativePath))
        {
            Content = new FormUrlEncodedContent(formData)
        };

        return await SendAsync(request, cancellationToken);
    }

    private async Task<object> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (IsCircuitOpen())
        {
            throw new InvalidOperationException("Facebook Graph API circuit breaker is open.");
        }

        try
        {
            var client = _httpClientFactory.CreateClient(nameof(FacebookGraphService));
            using var response = await client.SendAsync(request, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

            _logger.LogInformation("Facebook Graph {Method} {Uri} -> {StatusCode}", request.Method, request.RequestUri, response.StatusCode);

            if (!response.IsSuccessStatusCode)
            {
                RegisterFailure();
                throw new HttpRequestException(
                    $"Facebook Graph API returned {(int)response.StatusCode}: {responseBody}",
                    null,
                    response.StatusCode);
            }

            ResetCircuit();
            return JsonSerializer.Deserialize<object>(responseBody) ?? new { raw = responseBody };
        }
        catch
        {
            RegisterFailure();
            throw;
        }
    }

    private string BuildUrl(string relativePath)
    {
        var separator = relativePath.Contains('?') ? "&" : "?";
        return $"{_options.BaseUrl.TrimEnd('/')}/{relativePath}{separator}access_token={WebUtility.UrlEncode(_options.PageAccessToken)}";
    }

    private bool IsCircuitOpen()
    {
        lock (_circuitLock)
        {
            return _circuitOpenedUntil.HasValue && _circuitOpenedUntil.Value > DateTimeOffset.UtcNow;
        }
    }

    private void RegisterFailure()
    {
        lock (_circuitLock)
        {
            _failureCount++;
            if (_failureCount >= _options.CircuitBreakerFailureThreshold)
            {
                _circuitOpenedUntil = DateTimeOffset.UtcNow.AddSeconds(_options.CircuitBreakerBreakSeconds);
            }
        }
    }

    private void ResetCircuit()
    {
        lock (_circuitLock)
        {
            _failureCount = 0;
            _circuitOpenedUntil = null;
        }
    }
}
