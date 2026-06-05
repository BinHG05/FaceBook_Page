using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using BaiKT4.WebhookService.Models;
using BaiKT4.WebhookService.Options;
using Microsoft.Extensions.Options;

namespace BaiKT4.WebhookService.Services;

public sealed class KafkaEventPublisher : IKafkaEventPublisher
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly KafkaOptions _options;

    public KafkaEventPublisher(IHttpClientFactory httpClientFactory, IOptions<KafkaOptions> options)
    {
        _httpClientFactory = httpClientFactory;
        _options = options.Value;
    }

    public async Task PublishAsync(IEnumerable<NormalizedWebhookEvent> events, CancellationToken cancellationToken)
    {
        var records = events.Select(webhookEvent => new
        {
            key = ToBase64(webhookEvent.PageId ?? webhookEvent.SourceEventId ?? webhookEvent.EventId),
            value = ToBase64(JsonSerializer.Serialize(webhookEvent, JsonOptions))
        });

        var requestBody = JsonSerializer.Serialize(new { records }, JsonOptions);
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{_options.RestProxyBaseUrl.TrimEnd('/')}/topics/{Uri.EscapeDataString(_options.TopicName)}");

        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.kafka.v2+json"));
        request.Content = new StringContent(requestBody, Encoding.UTF8, "application/vnd.kafka.binary.v2+json");

        var client = _httpClientFactory.CreateClient(nameof(KafkaEventPublisher));
        using var response = await client.SendAsync(request, cancellationToken);
        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"Kafka REST Proxy publish failed with status {(int)response.StatusCode}: {responseBody}");
        }
    }

    private static string ToBase64(string value)
    {
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(value));
    }
}
