using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using FbStream.Contracts.Models;
using FbStream.Contracts.Validation;
using FbStream.CoreService.Options;
using Microsoft.Extensions.Options;

namespace FbStream.CoreService.Services;

public sealed class ReplyCommandPublisher : IReplyCommandPublisher
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly KafkaOptions _options;

    public ReplyCommandPublisher(IHttpClientFactory httpClientFactory, IOptions<KafkaOptions> options)
    {
        _httpClientFactory = httpClientFactory;
        _options = options.Value;
    }

    public async Task PublishAsync(IEnumerable<ReplyCommand> commands, CancellationToken cancellationToken)
    {
        var commandList = commands.ToList();

        // 🛡️ Data Contract & Schema Enforcement on Write
        foreach (var command in commandList)
        {
            ContractValidator.Validate(command);
        }

        var records = commandList.Select(command => new
        {
            key = ToBase64(command.CommandId),
            value = ToBase64(JsonSerializer.Serialize(command, JsonOptions))
        });

        var requestBody = JsonSerializer.Serialize(new { records }, JsonOptions);
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{_options.RestProxyBaseUrl.TrimEnd('/')}/topics/{Uri.EscapeDataString(_options.ReplyCommandsTopic)}");

        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.kafka.v2+json"));
        request.Content = new StringContent(requestBody, Encoding.UTF8, "application/vnd.kafka.binary.v2+json");

        var client = _httpClientFactory.CreateClient(nameof(ReplyCommandPublisher));
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
