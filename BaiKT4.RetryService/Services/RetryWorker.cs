using System.Text.Json;
using BaiKT4.Contracts.Models;
using BaiKT4.RetryService.Options;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace BaiKT4.RetryService.Services;

public sealed class RetryWorker : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ILogger<RetryWorker> _logger;
    private readonly KafkaOptions _kafkaOptions;
    private readonly IRetryPublisher _retryPublisher;

    public RetryWorker(
        ILogger<RetryWorker> logger,
        IOptions<KafkaOptions> kafkaOptions,
        IRetryPublisher retryPublisher)
    {
        _logger = logger;
        _kafkaOptions = kafkaOptions.Value;
        _retryPublisher = retryPublisher;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        // ==========================================
        // LUỒNG 2: BƯỚC 3: Khởi tạo Kafka Consumer tiêu thụ (consume) topic 'send_failed'
        // ==========================================
        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(_kafkaOptions.SendFailedTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);
                if (consumeResult?.Message?.Value is null)
                {
                    continue;
                }

                var failedEvent = JsonSerializer.Deserialize<SendFailedEvent>(consumeResult.Message.Value, JsonOptions);
                if (failedEvent is null)
                {
                    continue;
                }

                await HandleFailedEventAsync(failedEvent, stoppingToken);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Invalid send_failed payload. The message will be skipped.");
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

    private async Task HandleFailedEventAsync(SendFailedEvent failedEvent, CancellationToken cancellationToken)
    {
        var nextRetryCount = failedEvent.RetryCount + 1;

        // ==========================================
        // LUỒNG 2: BƯỚC 4: Kiểm tra điều kiện đưa vào Dead Letter Queue (DLQ).
        // Nếu lỗi KHÔNG THỂ THỬ LẠI (ví dụ: sai API Token) HOẶC số lần thử lại vượt quá MaxRetryCount (ví dụ: 3 lần)
        // ==========================================
        if (!failedEvent.Retryable || nextRetryCount > _kafkaOptions.MaxRetryCount)
        {
            var deadLetterEvent = new DeadLetterEvent
            {
                DeadLetterId = Guid.NewGuid().ToString("N"),
                Command = failedEvent.Command,
                Reason = failedEvent.FailureReason,
                RetryCount = failedEvent.RetryCount,
                Metadata =
                {
                    ["failureType"] = failedEvent.FailureType,
                    ["retryable"] = failedEvent.Retryable
                }
            };

            // Gửi sang Kafka topic 'dead_letter'
            await _retryPublisher.PublishDeadLetterAsync(deadLetterEvent, cancellationToken);
            _logger.LogWarning("Moved command {CommandId} to dead_letter after {RetryCount} retries.", failedEvent.Command.CommandId, failedEvent.RetryCount);
            return;
        }

        // ==========================================
        // LUỒNG 2: BƯỚC 5: Xử lý Thử lại (Retry) với độ trễ lũy thừa (Exponential Backoff Delay)
        // delay = 2 ^ RetryCount giây (lần 1 delay 1s, lần 2 delay 2s, lần 3 delay 4s...)
        // ==========================================
        var delay = TimeSpan.FromSeconds(Math.Pow(2, failedEvent.RetryCount));
        _logger.LogInformation("Scheduling retry {RetryCount} for command {CommandId} after {DelaySeconds}s.", nextRetryCount, failedEvent.Command.CommandId, delay.TotalSeconds);
        
        // Trì hoãn xử lý trước khi gửi tin nhắn thử lại
        await Task.Delay(delay, cancellationToken);

        // Tạo command mới với RetryCount được tăng lên
        var retryCommand = failedEvent.Command with
        {
            RetryCount = nextRetryCount
        };

        // ==========================================
        // LUỒNG 2: BƯỚC 6: Gửi lại Command vào Kafka topic 'send_retry'.
        // Backend API Service đang consume 'send_retry' nên sẽ nhận được và tiến hành thực thi lại lệnh này.
        // ==========================================
        await _retryPublisher.PublishRetryAsync(retryCommand, cancellationToken);
    }
}
