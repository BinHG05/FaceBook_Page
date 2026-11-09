using System.Security.Cryptography;
using System.Text;
using BaiKT4.BackendApi.Options;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Options;

namespace BaiKT4.BackendApi.Security;

public sealed class AdminApiKeyFilter : IAsyncActionFilter
{
    private const string ApiKeyHeaderName = "X-Admin-Api-Key";
    private readonly AdminAuthOptions _options;

    public AdminApiKeyFilter(IOptions<AdminAuthOptions> options)
    {
        _options = options.Value;
    }

    public Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
    {
        if (string.IsNullOrWhiteSpace(_options.ApiKey))
        {
            context.Result = new ObjectResult(new
            {
                success = false,
                error = "admin_auth_not_configured",
                message = "Admin API key is not configured."
            })
            {
                StatusCode = StatusCodes.Status503ServiceUnavailable
            };
            return Task.CompletedTask;
        }

        if (!context.HttpContext.Request.Headers.TryGetValue(ApiKeyHeaderName, out var suppliedKey)
            || !IsValidApiKey(suppliedKey.ToString(), _options.ApiKey))
        {
            context.Result = new UnauthorizedObjectResult(new
            {
                success = false,
                error = "unauthorized",
                message = "A valid X-Admin-Api-Key header is required."
            });
            return Task.CompletedTask;
        }

        return next();
    }

    private static bool IsValidApiKey(string suppliedKey, string configuredKey)
    {
        var suppliedBytes = Encoding.UTF8.GetBytes(suppliedKey);
        var configuredBytes = Encoding.UTF8.GetBytes(configuredKey);

        return suppliedBytes.Length == configuredBytes.Length
            && CryptographicOperations.FixedTimeEquals(suppliedBytes, configuredBytes);
    }
}
