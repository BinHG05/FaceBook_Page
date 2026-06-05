using System.Net;
using System.Text.Json;
using BaiKT4.BackendApi.Options;
using BaiKT4.Contracts.Models;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace BaiKT4.BackendApi.Services;

public sealed class CommandDispatchWorker : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ILogger<CommandDispatchWorker> _logger;
    private readonly KafkaOptions _kafkaOptions;
    private readonly IConfiguration _configuration;
    private readonly IFacebookGraphService _facebookGraphService;
    private readonly ISendFailedPublisher _sendFailedPublisher;
    private readonly string _connectionString;

    public CommandDispatchWorker(
        ILogger<CommandDispatchWorker> logger,
        IOptions<KafkaOptions> kafkaOptions,
        IConfiguration configuration,
        IFacebookGraphService facebookGraphService,
        ISendFailedPublisher sendFailedPublisher)
    {
        _logger = logger;
        _kafkaOptions = kafkaOptions.Value;
        _configuration = configuration;
        _facebookGraphService = facebookGraphService;
        _sendFailedPublisher = sendFailedPublisher;
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

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        
        // ==========================================
        // LUỒNG 1 & LUỒNG 2: 
        // Đăng ký nhận tin nhắn từ cả 2 topic:
        // 1. reply_commands (các lệnh phản hồi mới)
        // 2. send_retry (các lệnh phản hồi được Retry Service đẩy lại sau khi hết thời gian chờ/delay)
        // ==========================================
        consumer.Subscribe([_kafkaOptions.ReplyCommandsTopic, _kafkaOptions.SendRetryTopic]);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);
                if (consumeResult?.Message?.Value is null)
                {
                    continue;
                }

                var command = JsonSerializer.Deserialize<ReplyCommand>(consumeResult.Message.Value, JsonOptions);
                if (command is null)
                {
                    continue;
                }

                await ProcessCommandAsync(command, stoppingToken);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }

    private async Task ProcessCommandAsync(ReplyCommand command, CancellationToken cancellationToken)
    {
        try
        {
            UpsertCommandStatus(command, "received");

            // Chống lặp lệnh (Idempotency) dựa trên IdempotencyKey
            if (IsAlreadyHandled(command.IdempotencyKey))
            {
                UpsertCommandStatus(command, "duplicate");
                _logger.LogInformation("Skip duplicate command {CommandId}.", command.CommandId);
                return;
            }

            UpsertCommandStatus(command, "processing");

            // ==========================================
            // LUỒNG 1: BƯỚC 10: Dispatch Command - Gửi phản hồi thực tế lên trang Facebook.
            // ==========================================
            await _facebookGraphService.DispatchCommandAsync(command, cancellationToken);

            var successStatus = GetSuccessStatus(command.ActionType);
            SaveHandledCommand(command, successStatus);
            UpsertCommandStatus(command, successStatus);
            UpdateEventStatus(command.EventId, successStatus);

            _logger.LogInformation("Dispatched command {CommandId} successfully.", command.CommandId);
        }
        catch (Exception ex)
        {
            // ==========================================
            // LUỒNG 2: LUỒNG GẶP LỖI (Failed flow)
            // BƯỚC 1: Bắt lỗi khi việc gửi lên Facebook Graph API thất bại (ví dụ: mất kết nối, API rate limit...)
            // ==========================================
            _logger.LogError(ex, "Failed to dispatch command {CommandId}.", command.CommandId);
            UpsertCommandStatus(command, "failed", ex.Message);
            UpdateEventStatus(command.EventId, "failed");

            // Xác định xem lỗi này có thể thử lại được không (Retryable)
            // Ví dụ: Lỗi 401 (Unauth) không thể thử lại ngay mà không đổi token, 
            // nhưng lỗi mạng hoặc 429 (Too Many Requests), 5xx (Lỗi Server Facebook) thì có thể thử lại.
            var isRetryable = IsRetryable(ex);

            // Tạo sự kiện lỗi SendFailedEvent chứa Command cũ
            var sendFailedEvent = new SendFailedEvent
            {
                FailureId = Guid.NewGuid().ToString("N"),
                Command = command,
                FailureReason = ex.Message,
                FailureType = ClassifyFailureType(ex),
                RetryCount = command.RetryCount,
                Retryable = isRetryable
            };

            // ==========================================
            // LUỒNG 2: BƯỚC 2: Gửi sự kiện thất bại này vào Kafka topic 'send_failed'
            // Retry Service sẽ tiêu thụ (consume) topic này để quyết định có Retry hay đưa vào Dead Letter.
            // ==========================================
            await _sendFailedPublisher.PublishAsync(sendFailedEvent, cancellationToken);
        }
    }

    private bool IsAlreadyHandled(string idempotencyKey)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand("SELECT COUNT(*) FROM ProcessedCommands WHERE idempotency_key = @key AND status NOT IN ('received', 'processing', 'failed')", connection);
            command.Parameters.AddWithValue("@key", idempotencyKey);
            return Convert.ToInt32(command.ExecuteScalar()) > 0;
        }
        catch
        {
            return false;
        }
    }

    private void SaveHandledCommand(ReplyCommand command, string status)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var sql = new SqlCommand(@"
                IF NOT EXISTS (SELECT 1 FROM ProcessedCommands WHERE idempotency_key = @key)
                BEGIN
                    INSERT INTO ProcessedCommands (command_id, event_id, idempotency_key, action_type, retry_count, status, processed_at, updated_at)
                    VALUES (@commandId, @eventId, @key, @actionType, @retryCount, @status, GETDATE(), GETDATE())
                END
                ELSE
                BEGIN
                    UPDATE ProcessedCommands
                    SET status = @status,
                        retry_count = @retryCount,
                        updated_at = GETDATE(),
                        last_error = NULL
                    WHERE idempotency_key = @key
                END", connection);
            sql.Parameters.AddWithValue("@commandId", command.CommandId);
            sql.Parameters.AddWithValue("@eventId", command.EventId);
            sql.Parameters.AddWithValue("@key", command.IdempotencyKey);
            sql.Parameters.AddWithValue("@actionType", command.ActionType);
            sql.Parameters.AddWithValue("@retryCount", command.RetryCount);
            sql.Parameters.AddWithValue("@status", status);
            sql.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to persist idempotency key for command {CommandId}.", command.CommandId);
        }
    }

    private void UpsertCommandStatus(ReplyCommand command, string status, string? lastError = null)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var sql = new SqlCommand(@"
                IF EXISTS (SELECT 1 FROM ProcessedCommands WHERE idempotency_key = @key)
                BEGIN
                    UPDATE ProcessedCommands
                    SET status = @status,
                        retry_count = @retryCount,
                        last_error = @lastError,
                        updated_at = GETDATE()
                    WHERE idempotency_key = @key
                END
                ELSE
                BEGIN
                    INSERT INTO ProcessedCommands (command_id, event_id, idempotency_key, action_type, retry_count, status, last_error, processed_at, updated_at)
                    VALUES (@commandId, @eventId, @key, @actionType, @retryCount, @status, @lastError, GETDATE(), GETDATE())
                END", connection);
            sql.Parameters.AddWithValue("@commandId", command.CommandId);
            sql.Parameters.AddWithValue("@eventId", command.EventId);
            sql.Parameters.AddWithValue("@key", command.IdempotencyKey);
            sql.Parameters.AddWithValue("@actionType", command.ActionType);
            sql.Parameters.AddWithValue("@retryCount", command.RetryCount);
            sql.Parameters.AddWithValue("@status", status);
            sql.Parameters.AddWithValue("@lastError", (object?)lastError ?? DBNull.Value);
            sql.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update status for command {CommandId}.", command.CommandId);
        }
    }

    private void UpdateEventStatus(string eventId, string status)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var sql = new SqlCommand(@"
                IF EXISTS (SELECT 1 FROM ProcessedEvents WHERE event_id = @eventId)
                BEGIN
                    UPDATE ProcessedEvents
                    SET status = @status,
                        updated_at = GETDATE()
                    WHERE event_id = @eventId
                END", connection);
            sql.Parameters.AddWithValue("@eventId", eventId);
            sql.Parameters.AddWithValue("@status", status);
            sql.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update event status for {EventId}.", eventId);
        }
    }

    private static string GetSuccessStatus(string actionType)
    {
        return actionType switch
        {
            "auto_reply" => "replied",
            "hide_comment" => "moderated",
            "blacklist" => "blacklisted",
            "alert_admin" => "pending_review",
            _ => "processed"
        };
    }

    private static bool IsRetryable(Exception ex)
    {
        return ex switch
        {
            HttpRequestException httpRequestException when httpRequestException.StatusCode is null => true,
            HttpRequestException httpRequestException when httpRequestException.StatusCode is HttpStatusCode.RequestTimeout => true,
            HttpRequestException httpRequestException when httpRequestException.StatusCode is HttpStatusCode.TooManyRequests => true,
            HttpRequestException httpRequestException when (int?)httpRequestException.StatusCode >= 500 => true,
            _ => false
        };
    }

    private static string ClassifyFailureType(Exception ex)
    {
        return ex switch
        {
            HttpRequestException httpRequestException when httpRequestException.StatusCode is HttpStatusCode.Unauthorized => "invalid_token",
            HttpRequestException httpRequestException when httpRequestException.StatusCode is HttpStatusCode.TooManyRequests => "rate_limited",
            HttpRequestException httpRequestException when (int?)httpRequestException.StatusCode >= 500 => "facebook_server_error",
            HttpRequestException => "http_error",
            InvalidOperationException => "circuit_breaker",
            _ => "unknown_error"
        };
    }

    private void EnsureDatabaseObjects()
    {
        try
        {
            EnsureDatabaseCreated();

            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'ProcessedCommands' AND xtype = 'U')
                BEGIN
                    CREATE TABLE ProcessedCommands (
                        command_id VARCHAR(64) NOT NULL,
                        event_id VARCHAR(64) NULL,
                        idempotency_key VARCHAR(128) PRIMARY KEY,
                        action_type VARCHAR(64) NOT NULL,
                        retry_count INT NOT NULL DEFAULT 0,
                        status VARCHAR(64) NOT NULL DEFAULT 'received',
                        last_error NVARCHAR(MAX) NULL,
                        processed_at DATETIME NOT NULL DEFAULT GETDATE(),
                        updated_at DATETIME NOT NULL DEFAULT GETDATE()
                    )
                END", connection);
            command.ExecuteNonQuery();

            using var alterCommand = new SqlCommand(@"
                IF COL_LENGTH('ProcessedCommands', 'event_id') IS NULL
                BEGIN
                    ALTER TABLE ProcessedCommands ADD event_id VARCHAR(64) NULL
                END
                IF COL_LENGTH('ProcessedCommands', 'retry_count') IS NULL
                BEGIN
                    ALTER TABLE ProcessedCommands ADD retry_count INT NOT NULL DEFAULT 0
                END
                IF COL_LENGTH('ProcessedCommands', 'status') IS NULL
                BEGIN
                    ALTER TABLE ProcessedCommands ADD status VARCHAR(64) NOT NULL DEFAULT 'received'
                END
                IF COL_LENGTH('ProcessedCommands', 'last_error') IS NULL
                BEGIN
                    ALTER TABLE ProcessedCommands ADD last_error NVARCHAR(MAX) NULL
                END
                IF COL_LENGTH('ProcessedCommands', 'updated_at') IS NULL
                BEGIN
                    ALTER TABLE ProcessedCommands ADD updated_at DATETIME NOT NULL DEFAULT GETDATE()
                END
                IF OBJECT_ID('ProcessedEvents', 'U') IS NOT NULL AND COL_LENGTH('ProcessedEvents', 'updated_at') IS NULL
                BEGIN
                    ALTER TABLE ProcessedEvents ADD updated_at DATETIME NOT NULL DEFAULT GETDATE()
                END", connection);
            alterCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Database bootstrap for backend-api failed.");
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
