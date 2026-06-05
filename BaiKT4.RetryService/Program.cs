using BaiKT4.RetryService.Options;
using BaiKT4.RetryService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.AddHttpClient();
builder.Services.AddSingleton<IRetryPublisher, RetryPublisher>();
builder.Services.AddHostedService<RetryWorker>();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
{
    service = "retry-service",
    status = "running",
    endpoints = new[] { "GET /health" }
}));

app.MapGet("/health", () => Results.Ok(new
{
    service = "retry-service",
    status = "healthy",
    time = DateTimeOffset.UtcNow
}));

app.Run();
