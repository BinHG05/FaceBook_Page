using System.Text;
using BaiKT4.WebhookService.Options;
using BaiKT4.WebhookService.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace BaiKT4.WebhookService.Controllers;

[ApiController]
[Route("webhook")]
public sealed class WebhookController : ControllerBase
{
    private readonly FacebookWebhookOptions _options;
    private readonly IFacebookSignatureValidator _signatureValidator;
    private readonly IFacebookWebhookNormalizer _normalizer;
    private readonly IKafkaEventPublisher _publisher;
    private readonly ILogger<WebhookController> _logger;

    public WebhookController(
        IOptions<FacebookWebhookOptions> options,
        IFacebookSignatureValidator signatureValidator,
        IFacebookWebhookNormalizer normalizer,
        IKafkaEventPublisher publisher,
        ILogger<WebhookController> logger)
    {
        _options = options.Value;
        _signatureValidator = signatureValidator;
        _normalizer = normalizer;
        _publisher = publisher;
        _logger = logger;
    }

    [HttpGet]
    public IActionResult Verify(
        [FromQuery(Name = "hub.mode")] string? mode,
        [FromQuery(Name = "hub.verify_token")] string? verifyToken,
        [FromQuery(Name = "hub.challenge")] string? challenge)
    {
        if (!string.Equals(mode, "subscribe", StringComparison.OrdinalIgnoreCase))
        {
            return BadRequest(new { error = "Invalid hub.mode. Expected 'subscribe'." });
        }

        if (string.IsNullOrWhiteSpace(_options.VerifyToken) || !string.Equals(verifyToken, _options.VerifyToken, StringComparison.Ordinal))
        {
            return Unauthorized(new { error = "Verify token is invalid." });
        }

        return Content(challenge ?? string.Empty, "text/plain", Encoding.UTF8);
    }

    [HttpPost]
    public async Task<IActionResult> Receive(CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(Request.Body, Encoding.UTF8);
        var rawPayload = await reader.ReadToEndAsync(cancellationToken);
        var signatureHeader = Request.Headers["X-Hub-Signature-256"].FirstOrDefault();

        if (!_signatureValidator.IsSignatureValid(rawPayload, signatureHeader))
        {
            return Unauthorized(new { error = "Signature validation failed." });
        }

        var normalizedEvents = _normalizer.Normalize(rawPayload);
        if (normalizedEvents.Count == 0)
        {
            _logger.LogInformation("Webhook received but no supported event could be normalized.");
            return Ok(new
            {
                received = true,
                publishedCount = 0,
                message = "No supported Facebook event found in payload."
            });
        }

        await _publisher.PublishAsync(normalizedEvents, cancellationToken);

        _logger.LogInformation("Published {Count} event(s) to Kafka topic raw_events.", normalizedEvents.Count);

        return Ok(new
        {
            received = true,
            publishedCount = normalizedEvents.Count,
            normalizedEvents = normalizedEvents.Select(x => new
            {
                x.EventId,
                x.EventType,
                x.SourceEventId,
                x.PageId
            })
        });
    }
}
