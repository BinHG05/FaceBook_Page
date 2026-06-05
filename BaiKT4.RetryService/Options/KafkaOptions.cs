namespace BaiKT4.RetryService.Options;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9094";

    public string GroupId { get; set; } = "retry-service-group";

    public string RestProxyBaseUrl { get; set; } = "http://localhost:8082";

    public string SendFailedTopic { get; set; } = "send_failed";

    public string SendRetryTopic { get; set; } = "send_retry";

    public string DeadLetterTopic { get; set; } = "dead_letter";

    public int MaxRetryCount { get; set; } = 3;
}
