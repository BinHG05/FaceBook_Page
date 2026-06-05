namespace BaiKT4.CoreService.Options;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9094";

    public string GroupId { get; set; } = "core-service-group";

    public string RestProxyBaseUrl { get; set; } = "http://localhost:8082";

    public string RawEventsTopic { get; set; } = "raw_events";

    public string ReplyCommandsTopic { get; set; } = "reply_commands";
}
