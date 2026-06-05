using System.Text;
using System.Text.Json;
using BaiKT4.Contracts.Models;
using BaiKT4.CoreService.Options;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace BaiKT4.CoreService.Services;

public sealed class CoreEventProcessingWorker : BackgroundService
{
    private sealed record ModerationDecision(
        string ActionType,
        string Status,
        string? ReplyMessage,
        string? ReviewReason = null,
        bool QueueForReview = false,
        bool ShouldBlacklist = false);

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ILogger<CoreEventProcessingWorker> _logger;
    private readonly KafkaOptions _kafkaOptions;
    private readonly AiOptions _aiOptions;
    private readonly IConfiguration _configuration;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IReplyCommandPublisher _replyCommandPublisher;
    private readonly string _connectionString;
    private readonly object _circuitLock = new();
    private int _aiFailureCount;
    private DateTimeOffset? _aiCircuitOpenedUntil;

    public CoreEventProcessingWorker(
        ILogger<CoreEventProcessingWorker> logger,
        IOptions<KafkaOptions> kafkaOptions,
        IOptions<AiOptions> aiOptions,
        IConfiguration configuration,
        IHttpClientFactory httpClientFactory,
        IReplyCommandPublisher replyCommandPublisher)
    {
        _logger = logger;
        _kafkaOptions = kafkaOptions.Value;
        _aiOptions = aiOptions.Value;
        _configuration = configuration;
        _httpClientFactory = httpClientFactory;
        _replyCommandPublisher = replyCommandPublisher;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")
            ?? "Server=localhost;Database=BaiKT4Db;Integrated Security=True;TrustServerCertificate=True;";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        EnsureDatabaseObjects();

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        // ==========================================
        // LUỒNG 1: COMMENT -> RAW_EVENTS -> REPLY_COMMAND -> REPLY THẬT
        // BƯỚC 4: Core Service khởi tạo Kafka Consumer tiêu thụ (consume) topic 'raw_events'.
        // ==========================================
        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(_kafkaOptions.RawEventsTopic);

        _logger.LogInformation("Core Service consuming topic {Topic}.", _kafkaOptions.RawEventsTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Đọc sự kiện thô từ Kafka
                    var consumeResult = consumer.Consume(stoppingToken);
                    if (consumeResult?.Message?.Value is null)
                    {
                        continue;
                    }

                    // Tiến hành xử lý sự kiện thô nhận được
                    await ProcessMessageAsync(consumeResult.Message.Value, stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
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
        NormalizedWebhookEvent? webhookEvent = null;

        try
        {
            // ==========================================
            // LUỒNG 1: COMMENT -> RAW_EVENTS -> REPLY_COMMAND -> REPLY THẬT
            // BƯỚC 5: Deserialize JSON thành NormalizedWebhookEvent.
            // ==========================================
            webhookEvent = JsonSerializer.Deserialize<NormalizedWebhookEvent>(messageValue, JsonOptions);
            if (webhookEvent is null || string.IsNullOrWhiteSpace(webhookEvent.EventId))
            {
                return;
            }

            UpsertEventStatus(webhookEvent.EventId, "received");

            // Chống trùng lặp (Idempotency) dựa trên EventId
            if (IsAlreadyProcessed(webhookEvent.EventId))
            {
                _logger.LogInformation("Skip duplicate event {EventId}.", webhookEvent.EventId);
                return;
            }

            var content = webhookEvent.MessageText?.Trim() ?? string.Empty;
            var userId = webhookEvent.Actor?.Id ?? "unknown";

            // Nếu bình luận rỗng (không chứa chữ) thì bỏ qua
            if (string.IsNullOrWhiteSpace(content))
            {
                SaveEventLog(new EventProcessingLog
                {
                    EventId = webhookEvent.EventId,
                    UserId = userId,
                    Content = content,
                    Intent = "khac",
                    Sentiment = "trung_tinh",
                    Status = "ignored",
                    ActionTaken = "none"
                });
                MarkProcessed(webhookEvent.EventId, "ignored");
                return;
            }

            UpsertEventStatus(webhookEvent.EventId, "processing");

            // Kiểm tra các quy tắc nghiệp vụ cơ bản:
            // 1. Tần suất gửi (Rate limit)
            var isRateLimited = CountRecentEvents(userId, TimeSpan.FromMinutes(1)) >= 20;
            // 2. Danh sách đen (Blacklist)
            var isBlacklisted = IsUserBlacklisted(userId);
            // 3. Nội dung độc hại/lừa đảo (Malicious/Scam link, số điện thoại zalo...)
            var isMalicious = IsMaliciousOrScamContent(content);
            // 4. Spam thông thường (chứa từ cấm như mien phi, khuyen mai soc,...)
            var isSpam = isMalicious || IsSimpleSpam(content);
            var intent = "spam";
            var sentiment = "trung_tinh";

            // ==========================================
            // BƯỚC 6: Sử dụng Gemini AI để phân tích Ý định (Intent) và Thái độ (Sentiment)
            // (Chỉ phân tích nếu sự kiện không bị coi là spam hoặc rate limit hoặc blacklist từ đầu)
            // ==========================================
            if (!isSpam && !isRateLimited && !isBlacklisted)
            {
                (intent, sentiment) = await AnalyzeWithAiAsync(content, cancellationToken);
            }

            // ==========================================
            // BƯỚC 7: Đưa ra quyết định kiểm duyệt (Decide Action) dựa trên kết quả phân tích
            // ==========================================
            var action = DecideAction(userId, content, intent, sentiment, isSpam, isRateLimited, isBlacklisted, isMalicious);
            if (action.ShouldBlacklist)
            {
                UpsertBlacklist(userId, action.ReviewReason ?? "spam_repeat");
            }

            if (action.QueueForReview)
            {
                EnqueueManualReview(webhookEvent.EventId, userId, action.ReviewReason ?? "manual_review");
            }

            // Lưu log sự kiện vào cơ sở dữ liệu
            SaveEventLog(new EventProcessingLog
            {
                EventId = webhookEvent.EventId,
                UserId = userId,
                Content = content,
                Intent = intent,
                Sentiment = sentiment,
                Status = action.Status,
                ActionTaken = action.ActionType,
                ReviewReason = action.ReviewReason
            });

            MarkProcessed(webhookEvent.EventId, action.Status);

            // ==========================================
            // BƯỚC 8: Nếu hành động yêu cầu tự động phản hồi (auto_reply), 
            // tiến hành tạo một lệnh phản hồi ReplyCommand.
            // ==========================================
            var command = CreateReplyCommand(webhookEvent, action);
            if (command is not null)
            {
                // ==========================================
                // BƯỚC 9: Publish ReplyCommand vào Kafka topic 'reply_commands'.
                // Backend API Service sẽ tiêu thụ (consume) topic này để thực hiện phản hồi trên trang.
                // ==========================================
                await _replyCommandPublisher.PublishAsync([command], cancellationToken);
                _logger.LogInformation("Published command {CommandId} for event {EventId}.", command.CommandId, webhookEvent.EventId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing raw event {EventId}.", webhookEvent?.EventId);
            if (!string.IsNullOrWhiteSpace(webhookEvent?.EventId))
            {
                UpsertEventStatus(webhookEvent.EventId, "failed");
            }
        }
    }

    private ModerationDecision DecideAction(
        string userId,
        string content,
        string intent,
        string sentiment,
        bool isSpam,
        bool isRateLimited,
        bool isBlacklisted,
        bool isMalicious)
    {
        if (isRateLimited)
        {
            return new ModerationDecision("none", "pending_review", null, "rate_limited", true);
        }

        if (isBlacklisted)
        {
            if (isSpam || isMalicious)
            {
                return new ModerationDecision("hide_comment", "pending_review", null, "blacklisted_user_activity", true);
            }

            return new ModerationDecision("alert_admin", "pending_review", null, "blacklisted_user", true);
        }

        if (isMalicious)
        {
            return new ModerationDecision("hide_comment", "pending_review", null, "malicious_link_or_scam", true);
        }

        if (isSpam || intent == "spam")
        {
            if (CountRecentSpamActions(userId, TimeSpan.FromHours(24)) >= 2)
            {
                return new ModerationDecision("blacklist", "blacklisted", null, "spam_repeat_24h", true, true);
            }

            return new ModerationDecision("hide_comment", "moderated", null, "spam_detected");
        }

        if (intent == "hoi_gia")
        {
            return new ModerationDecision("auto_reply", "processed", "Shop xin gui bang gia va thong tin tu van cho ban ngay.");
        }

        if (sentiment == "tieu_cuc")
        {
            return new ModerationDecision("auto_reply", "processed", "Shop rat xin loi vi trai nghiem chua tot. Ben minh se ho tro ngay.");
        }

        if (sentiment == "tich_cuc")
        {
            return new ModerationDecision("auto_reply", "processed", "Cam on ban da ung ho shop. Ben minh rat vui khi nhan duoc phan hoi tich cuc.");
        }

        if (content.Contains("gia", StringComparison.OrdinalIgnoreCase))
        {
            return new ModerationDecision("auto_reply", "processed", "Ban vui long de lai san pham can hoi gia, shop se phan hoi ngay.");
        }

        return new ModerationDecision("none", "processed", null);
    }

    private ReplyCommand? CreateReplyCommand(NormalizedWebhookEvent webhookEvent, ModerationDecision action)
    {
        if (action.ActionType == "none")
        {
            return null;
        }

        return new ReplyCommand
        {
            CommandId = Guid.NewGuid().ToString("N"),
            EventId = webhookEvent.EventId,
            PageId = webhookEvent.PageId ?? "unknown-page",
            UserId = webhookEvent.Actor?.Id ?? "unknown-user",
            CommentId = webhookEvent.Target?.Id,
            PostId = webhookEvent.Target?.ParentId,
            ActionType = action.ActionType,
            ReplyMessage = action.ReplyMessage,
            IdempotencyKey = $"{webhookEvent.EventId}:{action.ActionType}",
            Status = action.Status,
            Metadata =
            {
                ["eventType"] = webhookEvent.EventType,
                ["sourceEventId"] = webhookEvent.SourceEventId,
                ["pageName"] = webhookEvent.PageName
            }
        };
    }

    private bool IsSimpleSpam(string content)
    {
        var lowerContent = content.ToLowerInvariant();
        if (lowerContent.Contains("http://") || lowerContent.Contains("https://") || lowerContent.Contains("bit.ly"))
        {
            return true;
        }

        string[] forbiddenWords = ["scam", "lua dao", "nhan qua", "mien phi", "khuyen mai soc"];
        return forbiddenWords.Any(word => lowerContent.Contains(word));
    }

    private static bool IsMaliciousOrScamContent(string content)
    {
        var lowerContent = content.ToLowerInvariant();
        string[] suspiciousKeywords =
        [
            "http://",
            "https://",
            "bit.ly",
            "tinyurl",
            "t.me/",
            "telegram",
            "lua dao",
            "scam",
            "nhan qua mien phi",
            "trung thuong",
            "nap the",
            "ket ban zalo",
            "bot auto"
        ];

        return suspiciousKeywords.Any(lowerContent.Contains);
    }

    private async Task<(string Intent, string Sentiment)> AnalyzeWithAiAsync(string content, CancellationToken cancellationToken)
    {
        if (IsAiCircuitOpen())
        {
            _logger.LogWarning("AI circuit breaker is open. Fallback to neutral classification.");
            return ("khac", "trung_tinh");
        }

        var apiKey = _configuration["GEMINI_API_KEY"] ?? _aiOptions.ApiKey;
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            return ("khac", "trung_tinh");
        }

        try
        {
            var client = _httpClientFactory.CreateClient(nameof(CoreEventProcessingWorker));

            var requestBody = new
            {
                systemInstruction = new
                {
                    parts = new object[]
                    {
                        new
                        {
                            text = "Respond with valid JSON only. Use this schema: {\"intent\":\"hoi_gia|khieu_nai|khen|spam|khac\",\"sentiment\":\"tich_cuc|tieu_cuc|trung_tinh\"}."
                        }
                    }
                },
                contents = new object[]
                {
                    new
                    {
                        parts = new object[]
                        {
                            new
                            {
                                text = $"Phan tich binh luan sau va tra ve JSON: {content}"
                            }
                        }
                    }
                },
                generationConfig = new
                {
                    temperature = 0,
                    responseMimeType = "application/json"
                }
            };

            using var response = await client.PostAsync(
                $"{_aiOptions.ApiBaseUrl.TrimEnd('/')}/models/{_aiOptions.Model}:generateContent?key={Uri.EscapeDataString(apiKey)}",
                new StringContent(JsonSerializer.Serialize(requestBody), Encoding.UTF8, "application/json"),
                cancellationToken);

            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                RegisterAiFailure();
                _logger.LogWarning("AI provider returned {StatusCode}: {Body}", response.StatusCode, responseBody);
                return ("khac", "trung_tinh");
            }

            using var outerDoc = JsonDocument.Parse(responseBody);
            var candidates = outerDoc.RootElement.GetProperty("candidates");
            if (candidates.GetArrayLength() == 0)
            {
                RegisterAiFailure();
                return ("khac", "trung_tinh");
            }

            var contentNode = candidates[0].GetProperty("content");
            var partsNode = contentNode.GetProperty("parts");
            if (partsNode.GetArrayLength() == 0)
            {
                RegisterAiFailure();
                return ("khac", "trung_tinh");
            }

            var contentJson = partsNode[0].GetProperty("text").GetString();

            if (string.IsNullOrWhiteSpace(contentJson))
            {
                RegisterAiFailure();
                return ("khac", "trung_tinh");
            }

            using var innerDoc = JsonDocument.Parse(contentJson);
            var intent = innerDoc.RootElement.TryGetProperty("intent", out var intentNode)
                ? intentNode.GetString() ?? "khac"
                : "khac";
            var sentiment = innerDoc.RootElement.TryGetProperty("sentiment", out var sentimentNode)
                ? sentimentNode.GetString() ?? "trung_tinh"
                : "trung_tinh";

            ResetAiCircuit();
            return (intent, sentiment);
        }
        catch (Exception ex)
        {
            RegisterAiFailure();
            _logger.LogError(ex, "AI analysis failed.");
            return ("khac", "trung_tinh");
        }
    }

    private bool IsAlreadyProcessed(string eventId)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand("SELECT COUNT(*) FROM ProcessedEvents WHERE event_id = @eventId AND status NOT IN ('received', 'processing')", connection);
            command.Parameters.AddWithValue("@eventId", eventId);
            return Convert.ToInt32(command.ExecuteScalar()) > 0;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to check duplicate event {EventId}.", eventId);
            return false;
        }
    }

    private void MarkProcessed(string eventId, string status)
    {
        try
        {
            UpsertEventStatus(eventId, status);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to mark event {EventId} as processed.", eventId);
        }
    }

    private void UpsertEventStatus(string eventId, string status)
    {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var command = new SqlCommand(@"
            IF EXISTS (SELECT 1 FROM ProcessedEvents WHERE event_id = @eventId)
            BEGIN
                UPDATE ProcessedEvents
                SET status = @status,
                    updated_at = GETDATE()
                WHERE event_id = @eventId
            END
            ELSE
            BEGIN
                INSERT INTO ProcessedEvents (event_id, status, processed_at, updated_at)
                VALUES (@eventId, @status, GETDATE(), GETDATE())
            END", connection);
        command.Parameters.AddWithValue("@eventId", eventId);
        command.Parameters.AddWithValue("@status", status);
        command.ExecuteNonQuery();
    }

    private void SaveEventLog(EventProcessingLog log)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand(@"
                INSERT INTO EventLogs (event_id, user_id, content, intent, sentiment, status, action_taken, review_reason, created_at)
                VALUES (@eventId, @userId, @content, @intent, @sentiment, @status, @actionTaken, @reviewReason, GETDATE())", connection);
            command.Parameters.AddWithValue("@eventId", log.EventId);
            command.Parameters.AddWithValue("@userId", (object?)log.UserId ?? DBNull.Value);
            command.Parameters.AddWithValue("@content", (object?)log.Content ?? DBNull.Value);
            command.Parameters.AddWithValue("@intent", log.Intent);
            command.Parameters.AddWithValue("@sentiment", log.Sentiment);
            command.Parameters.AddWithValue("@status", log.Status);
            command.Parameters.AddWithValue("@actionTaken", log.ActionTaken);
            command.Parameters.AddWithValue("@reviewReason", (object?)log.ReviewReason ?? DBNull.Value);
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to save event log for event {EventId}.", log.EventId);
        }
    }

    private int CountRecentSpamActions(string userId, TimeSpan interval)
    {
        return CountByQuery(@"
            SELECT COUNT(*) FROM EventLogs
            WHERE user_id = @userId
              AND action_taken IN ('hide_comment', 'blacklist')
              AND created_at > DATEADD(second, @seconds, GETDATE())",
            userId,
            interval);
    }

    private int CountRecentEvents(string userId, TimeSpan interval)
    {
        return CountByQuery(@"
            SELECT COUNT(*) FROM EventLogs
            WHERE user_id = @userId
              AND created_at > DATEADD(second, @seconds, GETDATE())",
            userId,
            interval);
    }

    private int CountByQuery(string query, string userId, TimeSpan interval)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@userId", userId);
            command.Parameters.AddWithValue("@seconds", -(int)interval.TotalSeconds);
            return Convert.ToInt32(command.ExecuteScalar());
        }
        catch
        {
            return 0;
        }
    }

    private bool IsUserBlacklisted(string userId)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand("SELECT COUNT(*) FROM BlacklistedUsers WHERE user_id = @userId AND is_active = 1", connection);
            command.Parameters.AddWithValue("@userId", userId);
            return Convert.ToInt32(command.ExecuteScalar()) > 0;
        }
        catch
        {
            return false;
        }
    }

    private void UpsertBlacklist(string userId, string reason)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand(@"
                IF EXISTS (SELECT 1 FROM BlacklistedUsers WHERE user_id = @userId)
                BEGIN
                    UPDATE BlacklistedUsers
                    SET is_active = 1,
                        reason = @reason,
                        updated_at = GETDATE()
                    WHERE user_id = @userId
                END
                ELSE
                BEGIN
                    INSERT INTO BlacklistedUsers (user_id, reason, is_active, created_at, updated_at)
                    VALUES (@userId, @reason, 1, GETDATE(), GETDATE())
                END", connection);
            command.Parameters.AddWithValue("@userId", userId);
            command.Parameters.AddWithValue("@reason", reason);
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to blacklist user {UserId}.", userId);
        }
    }

    private void EnqueueManualReview(string eventId, string userId, string reason)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand(@"
                IF NOT EXISTS (SELECT 1 FROM ManualReviewQueue WHERE event_id = @eventId)
                BEGIN
                    INSERT INTO ManualReviewQueue (review_id, event_id, user_id, reason, status, created_at, updated_at)
                    VALUES (@reviewId, @eventId, @userId, @reason, 'open', GETDATE(), GETDATE())
                END", connection);
            command.Parameters.AddWithValue("@reviewId", Guid.NewGuid().ToString("N"));
            command.Parameters.AddWithValue("@eventId", eventId);
            command.Parameters.AddWithValue("@userId", userId);
            command.Parameters.AddWithValue("@reason", reason);
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to enqueue manual review for event {EventId}.", eventId);
        }
    }

    private bool IsAiCircuitOpen()
    {
        lock (_circuitLock)
        {
            return _aiCircuitOpenedUntil.HasValue && _aiCircuitOpenedUntil.Value > DateTimeOffset.UtcNow;
        }
    }

    private void RegisterAiFailure()
    {
        lock (_circuitLock)
        {
            _aiFailureCount++;
            if (_aiFailureCount >= _aiOptions.CircuitBreakerFailureThreshold)
            {
                _aiCircuitOpenedUntil = DateTimeOffset.UtcNow.AddSeconds(_aiOptions.CircuitBreakerBreakSeconds);
            }
        }
    }

    private void ResetAiCircuit()
    {
        lock (_circuitLock)
        {
            _aiFailureCount = 0;
            _aiCircuitOpenedUntil = null;
        }
    }

    private void EnsureDatabaseObjects()
    {
        try
        {
            EnsureDatabaseCreated();

            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            using var logsCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'EventLogs' AND xtype = 'U')
                BEGIN
                    CREATE TABLE EventLogs (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        event_id VARCHAR(64) NOT NULL,
                        user_id VARCHAR(64) NULL,
                        content NVARCHAR(MAX) NULL,
                        intent VARCHAR(64) NOT NULL,
                        sentiment VARCHAR(64) NOT NULL,
                        status VARCHAR(64) NOT NULL,
                        action_taken VARCHAR(128) NOT NULL,
                        review_reason VARCHAR(128) NULL,
                        created_at DATETIME NOT NULL DEFAULT GETDATE()
                    )
                END", connection);
            logsCommand.ExecuteNonQuery();

            using var eventLogsAlterCommand = new SqlCommand(@"
                IF COL_LENGTH('EventLogs', 'review_reason') IS NULL
                BEGIN
                    ALTER TABLE EventLogs ADD review_reason VARCHAR(128) NULL
                END", connection);
            eventLogsAlterCommand.ExecuteNonQuery();

            using var processedCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'ProcessedEvents' AND xtype = 'U')
                BEGIN
                    CREATE TABLE ProcessedEvents (
                        event_id VARCHAR(64) PRIMARY KEY,
                        status VARCHAR(64) NOT NULL,
                        processed_at DATETIME NOT NULL DEFAULT GETDATE(),
                        updated_at DATETIME NOT NULL DEFAULT GETDATE()
                    )
                END", connection);
            processedCommand.ExecuteNonQuery();

            using var processedEventsAlterCommand = new SqlCommand(@"
                IF COL_LENGTH('ProcessedEvents', 'updated_at') IS NULL
                BEGIN
                    ALTER TABLE ProcessedEvents ADD updated_at DATETIME NOT NULL DEFAULT GETDATE()
                END", connection);
            processedEventsAlterCommand.ExecuteNonQuery();

            using var manualReviewCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'ManualReviewQueue' AND xtype = 'U')
                BEGIN
                    CREATE TABLE ManualReviewQueue (
                        review_id VARCHAR(64) PRIMARY KEY,
                        event_id VARCHAR(64) NOT NULL,
                        user_id VARCHAR(64) NULL,
                        reason VARCHAR(128) NOT NULL,
                        status VARCHAR(64) NOT NULL,
                        created_at DATETIME NOT NULL DEFAULT GETDATE(),
                        updated_at DATETIME NOT NULL DEFAULT GETDATE()
                    )
                END", connection);
            manualReviewCommand.ExecuteNonQuery();

            using var blacklistCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'BlacklistedUsers' AND xtype = 'U')
                BEGIN
                    CREATE TABLE BlacklistedUsers (
                        user_id VARCHAR(64) PRIMARY KEY,
                        reason VARCHAR(128) NOT NULL,
                        is_active BIT NOT NULL DEFAULT 1,
                        created_at DATETIME NOT NULL DEFAULT GETDATE(),
                        updated_at DATETIME NOT NULL DEFAULT GETDATE()
                    )
                END", connection);
            blacklistCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Database bootstrap for core-service failed.");
        }
    }

    private void EnsureDatabaseCreated()
    {
        var builder = new SqlConnectionStringBuilder(_connectionString);
        var databaseName = builder.InitialCatalog;

        if (string.IsNullOrWhiteSpace(databaseName))
        {
            return;
        }

        builder.InitialCatalog = "master";

        using var connection = new SqlConnection(builder.ConnectionString);
        connection.Open();
        using var command = new SqlCommand($@"
            IF DB_ID(N'{databaseName.Replace("'", "''")}') IS NULL
            BEGIN
                CREATE DATABASE [{databaseName}]
            END", connection);
        command.ExecuteNonQuery();
    }
}
