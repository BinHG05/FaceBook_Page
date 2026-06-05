namespace BaiKT4.BackendApi.Options;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9094";

    public string GroupId { get; set; } = "backend-api-group";

    public string RestProxyBaseUrl { get; set; } = "http://localhost:8082";

    public string ReplyCommandsTopic { get; set; } = "reply_commands";

    public string SendRetryTopic { get; set; } = "send_retry";

    public string SendFailedTopic { get; set; } = "send_failed";
}
