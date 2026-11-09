using FbStream.BackendApi.Options;
using FbStream.BackendApi.Security;
using FbStream.BackendApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.Configure<FacebookGraphOptions>(builder.Configuration.GetSection(FacebookGraphOptions.SectionName));
builder.Services.Configure<AdminAuthOptions>(builder.Configuration.GetSection(AdminAuthOptions.SectionName));
builder.Services.AddHttpClient();
builder.Services.AddControllers();
builder.Services.AddScoped<AdminApiKeyFilter>();
builder.Services.AddSingleton<ISendFailedPublisher, SendFailedPublisher>();
builder.Services.AddSingleton<IFacebookGraphService, FacebookGraphService>();
builder.Services.AddHostedService<CommandDispatchWorker>();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
{
    service = "backend-api",
    status = "running",
    endpoints = new[]
    {
        "GET /health",
        "GET /posts",
        "POST /post",
        "GET /comments"
    }
}));

app.MapGet("/health", () => Results.Ok(new
{
    service = "backend-api",
    status = "healthy",
    time = DateTimeOffset.UtcNow
}));

app.MapControllers();

app.Run();
