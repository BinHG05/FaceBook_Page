namespace BaiKT4.WebhookService.Services;

public interface IFacebookSignatureValidator
{
    bool IsSignatureValid(string rawPayload, string? signatureHeader);
}
