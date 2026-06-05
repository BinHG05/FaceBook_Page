using BaiKT4.CoreService.Options;
using BaiKT4.CoreService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.Configure<AiOptions>(builder.Configuration.GetSection(AiOptions.SectionName));
builder.Services.AddHttpClient();
builder.Services.AddSingleton<IReplyCommandPublisher, ReplyCommandPublisher>();
builder.Services.AddHostedService<CoreEventProcessingWorker>();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
{
    service = "core-service",
    status = "running",
    endpoints = new[] { "GET /health" }
}));

app.MapGet("/health", () => Results.Ok(new
{
    service = "core-service",
    status = "healthy",
    time = DateTimeOffset.UtcNow
}));

app.Run();
