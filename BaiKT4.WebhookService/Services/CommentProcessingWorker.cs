using System.Text;
using System.Text.Json;
using BaiKT4.WebhookService.Options;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace BaiKT4.WebhookService.Services;

public class CommentProcessingWorker : BackgroundService
{
    private readonly ILogger<CommentProcessingWorker> _logger;
    private readonly KafkaOptions _kafkaOptions;
    private readonly IConfiguration _configuration;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly string _groqApiKey;
    private readonly string _connectionString;

    public CommentProcessingWorker(
        ILogger<CommentProcessingWorker> logger,
        IOptions<KafkaOptions> kafkaOptions,
        IConfiguration configuration,
        IHttpClientFactory httpClientFactory)
    {
        _logger = logger;
        _kafkaOptions = kafkaOptions.Value;
        _configuration = configuration;
        _httpClientFactory = httpClientFactory;

        _groqApiKey = _configuration["GROQ_API_KEY"]
            ?? _configuration["GroqApiKey"]
            ?? string.Empty;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")
                            ?? "Server=localhost;Database=CoreServiceDB;User Id=sa;Password=bin?123;TrustServerCertificate=True;";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        EnsureDatabaseCreated();

        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9094",
            GroupId = "core-service-csharp-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(_kafkaOptions.TopicName);

        _logger.LogInformation("Core Service (C#) dang lang nghe topic: {TopicName}...", _kafkaOptions.TopicName);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(stoppingToken);
                    if (consumeResult?.Message == null) continue;

                    await ProcessMessageAsync(consumeResult.Message.Value, stoppingToken);
                }
                catch (ConsumeException e)
                {
                    _logger.LogError("Loi Consume Kafka: {Reason}", e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }

    private async Task ProcessMessageAsync(string messageValue, CancellationToken cancellationToken)
    {
        try
        {
            using var doc = JsonDocument.Parse(messageValue);
            var root = doc.RootElement;

            string content = root.TryGetProperty("messageText", out var textEl) ? textEl.GetString() ?? "" : "";
            string userId = root.TryGetProperty("actor", out var actorEl) && actorEl.TryGetProperty("id", out var idEl)
                ? idEl.GetString() ?? "unknown"
                : "unknown";

            if (string.IsNullOrWhiteSpace(content)) return;

            _logger.LogInformation("Nhan event tu User {UserId}: {Content}", userId, content);

            bool isSpam = IsSimpleSpam(content);
            string intent = "spam";
            string sentiment = "trung_tinh";

            if (!isSpam)
            {
                var aiResult = await AnalyzeWithGroqAsync(content, cancellationToken);
                intent = aiResult.Intent;
                sentiment = aiResult.Sentiment;
            }

            string action = GetActionDecision(userId, intent, sentiment, isSpam);

            SaveToDatabase(userId, content, intent, sentiment, action);

            _logger.LogInformation("Xu ly xong -> Intent: {Intent}, Action: {Action}", intent, action);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Loi khi xu ly message tu Kafka.");
        }
    }

    private bool IsSimpleSpam(string content)
    {
        var lowerContent = content.ToLowerInvariant();
        if (lowerContent.Contains("http://") || lowerContent.Contains("https://") || lowerContent.Contains("bit.ly"))
            return true;

        string[] forbidden = { "scam", "lua dao", "nhan qua" };
        return forbidden.Any(word => lowerContent.Contains(word));
    }

    private string GetActionDecision(string userId, string intent, string sentiment, bool isSpam)
    {
        if (isSpam || intent == "spam")
        {
            if (CheckSpamCount24h(userId) >= 2) return "blacklist";
            return "hide";
        }

        if (sentiment == "tieu_cuc") return "alert_admin";
        if (intent == "hoi_gia") return "auto_reply_price";

        return "none";
    }

    private int CheckSpamCount24h(string userId)
    {
        try
        {
            using var conn = new SqlConnection(_connectionString);
            conn.Open();
            var cmd = new SqlCommand(@"
                SELECT COUNT(*) FROM EventLogs
                WHERE user_id = @userId AND action_taken = 'hide' AND created_at > DATEADD(hour, -24, GETDATE())
            ", conn);
            cmd.Parameters.AddWithValue("@userId", userId);
            return (int)cmd.ExecuteScalar();
        }
        catch (Exception ex)
        {
            _logger.LogError("Loi check spam: {Message}", ex.Message);
            return 0;
        }
    }

    private void SaveToDatabase(string userId, string content, string intent, string sentiment, string action)
    {
        try
        {
            using var conn = new SqlConnection(_connectionString);
            conn.Open();
            var cmd = new SqlCommand(@"
                INSERT INTO EventLogs (user_id, content, intent, sentiment, status, action_taken)
                VALUES (@uid, @content, @intent, @sentiment, 'processed', @action)
            ", conn);
            cmd.Parameters.AddWithValue("@uid", userId);
            cmd.Parameters.AddWithValue("@content", content);
            cmd.Parameters.AddWithValue("@intent", intent);
            cmd.Parameters.AddWithValue("@sentiment", sentiment);
            cmd.Parameters.AddWithValue("@action", action);
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogError("Loi luu DB: {Message}", ex.Message);
        }
    }

    private void EnsureDatabaseCreated()
    {
        try
        {
            var masterConnectionString = new SqlConnectionStringBuilder(_connectionString)
            {
                InitialCatalog = "master"
            }.ConnectionString;

            using (var masterConn = new SqlConnection(masterConnectionString))
            {
                masterConn.Open();
                var dbName = new SqlConnectionStringBuilder(_connectionString).InitialCatalog;
                var cmdDb = new SqlCommand($@"
                    IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{dbName}')
                    BEGIN
                        CREATE DATABASE [{dbName}];
                    END", masterConn);
                cmdDb.ExecuteNonQuery();
            }

            using (var conn = new SqlConnection(_connectionString))
            {
                conn.Open();
                var cmdTable = new SqlCommand(@"
                    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EventLogs' and xtype='U')
                    CREATE TABLE EventLogs (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        user_id VARCHAR(50),
                        content NVARCHAR(MAX),
                        intent VARCHAR(50),
                        sentiment VARCHAR(50),
                        status VARCHAR(50),
                        action_taken VARCHAR(100),
                        created_at DATETIME DEFAULT GETDATE()
                    )
                ", conn);
                cmdTable.ExecuteNonQuery();
            }

            _logger.LogInformation("Da tu dong tao Database va Bang thanh cong (hoac da ton tai).");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Khong the tu dong tao DB/Table: {Message}", ex.Message);
        }
    }

    private async Task<(string Intent, string Sentiment)> AnalyzeWithGroqAsync(string content, CancellationToken ct)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(_groqApiKey))
            {
                _logger.LogWarning("Chua cau hinh GROQ_API_KEY, bo qua phan tich AI.");
                return ("khac", "trung_tinh");
            }

            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("Authorization", $"Bearer {_groqApiKey}");

            var requestBody = new
            {
                model = "llama-3.1-8b-instant",
                temperature = 0,
                response_format = new { type = "json_object" },
                messages = new object[]
                {
                    new
                    {
                        role = "system",
                        content = "You are a helpful assistant that strictly outputs JSON. Format: {\"intent\":\"hoi_gia|khieu_nai|khen|spam|khac\", \"sentiment\":\"tich_cuc|tieu_cuc|trung_tinh\"}"
                    },
                    new { role = "user", content = $"Phan tich: \"{content}\"" }
                }
            };

            var response = await client.PostAsync(
                "https://api.groq.com/openai/v1/chat/completions",
                new StringContent(JsonSerializer.Serialize(requestBody), Encoding.UTF8, "application/json"),
                ct);

            if (response.IsSuccessStatusCode)
            {
                var responseString = await response.Content.ReadAsStringAsync(ct);
                using var doc = JsonDocument.Parse(responseString);
                var aiResponseText = doc.RootElement
                    .GetProperty("choices")[0]
                    .GetProperty("message")
                    .GetProperty("content")
                    .GetString();

                if (!string.IsNullOrEmpty(aiResponseText))
                {
                    using var resultDoc = JsonDocument.Parse(aiResponseText);
                    string resIntent = resultDoc.RootElement.TryGetProperty("intent", out var i)
                        ? i.GetString() ?? "khac"
                        : "khac";
                    string resSentiment = resultDoc.RootElement.TryGetProperty("sentiment", out var s)
                        ? s.GetString() ?? "trung_tinh"
                        : "trung_tinh";
                    return (resIntent, resSentiment);
                }
            }
            else
            {
                var errorString = await response.Content.ReadAsStringAsync(ct);
                _logger.LogError("Groq API Error {StatusCode}: {Error}", response.StatusCode, errorString);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError("Loi goi Groq API: {Message}", ex.Message);
        }

        return ("khac", "trung_tinh");
    }
}
