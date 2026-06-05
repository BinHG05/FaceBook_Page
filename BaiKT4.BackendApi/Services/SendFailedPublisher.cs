using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using BaiKT4.BackendApi.Options;
using BaiKT4.Contracts.Models;
using Microsoft.Extensions.Options;

namespace BaiKT4.BackendApi.Services;

public sealed class SendFailedPublisher : ISendFailedPublisher
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly KafkaOptions _options;

    public SendFailedPublisher(IHttpClientFactory httpClientFactory, IOptions<KafkaOptions> options)
    {
        _httpClientFactory = httpClientFactory;
        _options = options.Value;
    }

    public async Task PublishAsync(SendFailedEvent sendFailedEvent, CancellationToken cancellationToken)
    {
        var record = new
        {
            key = ToBase64(sendFailedEvent.Command.CommandId),
            value = ToBase64(JsonSerializer.Serialize(sendFailedEvent, JsonOptions))
        };

        var requestBody = JsonSerializer.Serialize(new { records = new[] { record } }, JsonOptions);
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{_options.RestProxyBaseUrl.TrimEnd('/')}/topics/{Uri.EscapeDataString(_options.SendFailedTopic)}");

        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.kafka.v2+json"));
        request.Content = new StringContent(requestBody, Encoding.UTF8, "application/vnd.kafka.binary.v2+json");

        var client = _httpClientFactory.CreateClient(nameof(SendFailedPublisher));
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
