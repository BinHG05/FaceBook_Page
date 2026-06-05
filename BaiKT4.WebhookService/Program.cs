using BaiKT4.WebhookService.Options;
using BaiKT4.WebhookService.Services;

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
