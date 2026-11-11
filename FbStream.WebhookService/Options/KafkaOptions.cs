namespace FbStream.WebhookService.Options;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string RestProxyBaseUrl { get; set; } = "http://localhost:8082";

    public string TopicName { get; set; } = "raw_events";

    public string ClientId { get; set; } = "fbstream-webhook-service";
}
