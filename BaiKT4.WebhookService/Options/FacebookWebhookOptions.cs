namespace BaiKT4.WebhookService.Options;

public sealed class FacebookWebhookOptions
{
    public const string SectionName = "FacebookWebhook";

    public string VerifyToken { get; set; } = string.Empty;

    public string AppSecret { get; set; } = string.Empty;

    public string PageId { get; set; } = string.Empty;

    public bool AcceptUnsignedPayloads { get; set; } = true;
}
