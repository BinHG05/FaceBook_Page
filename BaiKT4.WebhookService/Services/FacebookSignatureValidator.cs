using System.Security.Cryptography;
using System.Text;
using BaiKT4.WebhookService.Options;
using Microsoft.Extensions.Options;

namespace BaiKT4.WebhookService.Services;

public sealed class FacebookSignatureValidator : IFacebookSignatureValidator
{
    private readonly FacebookWebhookOptions _options;

    public FacebookSignatureValidator(IOptions<FacebookWebhookOptions> options)
    {
        _options = options.Value;
    }

    public bool IsSignatureValid(string rawPayload, string? signatureHeader)
    {
        if (string.IsNullOrWhiteSpace(_options.AppSecret))
        {
            return _options.AcceptUnsignedPayloads;
        }

        if (string.IsNullOrWhiteSpace(signatureHeader))
        {
            return false;
        }

        const string prefix = "sha256=";
        if (!signatureHeader.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var expectedSignature = signatureHeader[prefix.Length..];
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(_options.AppSecret));
        var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(rawPayload));
        var actualSignature = Convert.ToHexStringLower(hash);

        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(actualSignature),
            Encoding.UTF8.GetBytes(expectedSignature));
    }
}
