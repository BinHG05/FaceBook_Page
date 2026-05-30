using BaiKT4.WebhookService.Options;
using BaiKT4.WebhookService.Services;

LoadDotEnv(Path.Combine(Directory.GetCurrentDirectory(), "..", "core_service", ".env"));

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<FacebookWebhookOptions>(
    builder.Configuration.GetSection(FacebookWebhookOptions.SectionName));
builder.Services.Configure<KafkaOptions>(
    builder.Configuration.GetSection(KafkaOptions.SectionName));

builder.Services.AddHttpClient();
builder.Services.AddSingleton<IKafkaEventPublisher, KafkaEventPublisher>();
builder.Services.AddSingleton<IFacebookSignatureValidator, FacebookSignatureValidator>();
builder.Services.AddSingleton<IFacebookWebhookNormalizer, FacebookWebhookNormalizer>();
builder.Services.AddControllers();
builder.Services.AddHostedService<CommentProcessingWorker>();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
{
    service = "webhook-service",
    status = "running",
    endpoints = new[]
    {
        "GET /health",
        "GET /webhook",
        "POST /webhook"
    }
}));

app.MapGet("/health", () => Results.Ok(new
{
    status = "healthy",
    service = "webhook-service",
    time = DateTimeOffset.UtcNow
}));

app.UseAuthorization();
app.MapControllers();

app.Run();

static void LoadDotEnv(string envFilePath)
{
    if (!File.Exists(envFilePath))
    {
        return;
    }

    foreach (var rawLine in File.ReadAllLines(envFilePath))
    {
        var line = rawLine.Trim();
        if (string.IsNullOrWhiteSpace(line) || line.StartsWith("#"))
        {
            continue;
        }

        var separatorIndex = line.IndexOf('=');
        if (separatorIndex <= 0)
        {
            continue;
        }

        var key = line[..separatorIndex].Trim();
        var value = line[(separatorIndex + 1)..].Trim().Trim('"');
        if (string.IsNullOrWhiteSpace(key))
        {
            continue;
        }

        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(key)))
        {
            Environment.SetEnvironmentVariable(key, value);
        }
    }
}
