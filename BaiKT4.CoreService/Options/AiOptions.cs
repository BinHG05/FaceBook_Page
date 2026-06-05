namespace BaiKT4.CoreService.Options;

public sealed class AiOptions
{
    public const string SectionName = "Ai";

    public string Provider { get; set; } = "gemini";

    public string ApiBaseUrl { get; set; } = "https://generativelanguage.googleapis.com/v1beta";

    public string Model { get; set; } = "gemini-3.1-flash-lite";

    public string ApiKey { get; set; } = string.Empty;

    public int CircuitBreakerFailureThreshold { get; set; } = 5;

    public int CircuitBreakerBreakSeconds { get; set; } = 30;
}
