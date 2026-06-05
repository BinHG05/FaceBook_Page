namespace BaiKT4.BackendApi.Options;

public sealed class FacebookGraphOptions
{
    public const string SectionName = "FacebookGraph";

    public string BaseUrl { get; set; } = "https://graph.facebook.com/v22.0";

    public string PageId { get; set; } = string.Empty;

    public string PageAccessToken { get; set; } = string.Empty;

    public bool SimulateMode { get; set; } = true;

    public int CircuitBreakerFailureThreshold { get; set; } = 5;

    public int CircuitBreakerBreakSeconds { get; set; } = 30;
}
