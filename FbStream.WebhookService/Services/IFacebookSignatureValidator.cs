namespace FbStream.WebhookService.Services;

public interface IFacebookSignatureValidator
{
    bool IsSignatureValid(string rawPayload, string? signatureHeader);
}
