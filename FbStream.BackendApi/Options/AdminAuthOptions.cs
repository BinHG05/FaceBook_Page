namespace FbStream.BackendApi.Options;

public sealed class AdminAuthOptions
{
    public const string SectionName = "AdminAuth";

    public string ApiKey { get; set; } = string.Empty;
}
