using BaiKT4.BackendApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace BaiKT4.BackendApi.Controllers;

[ApiController]
[Route("")]
public sealed class FacebookProxyController : ControllerBase
{
    private readonly IFacebookGraphService _facebookGraphService;

    public FacebookProxyController(IFacebookGraphService facebookGraphService)
    {
        _facebookGraphService = facebookGraphService;
    }

    [HttpGet("posts")]
    public async Task<IActionResult> GetPosts(CancellationToken cancellationToken)
    {
        try
        {
            var response = await _facebookGraphService.GetPostsAsync(cancellationToken);
            return Ok(new { success = true, data = response });
        }
        catch (Exception ex)
        {
            return StatusCode(StatusCodes.Status502BadGateway, new
            {
                success = false,
                error = "facebook_api_error",
                message = ex.Message
            });
        }
    }

    [HttpPost("post")]
    public async Task<IActionResult> CreatePost([FromBody] CreatePostRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.Message))
        {
            return BadRequest(new
            {
                success = false,
                error = "invalid_request",
                message = "Message is required."
            });
        }

        try
        {
            var response = await _facebookGraphService.CreatePostAsync(request.Message, cancellationToken);
            return Ok(new { success = true, data = response });
        }
        catch (Exception ex)
        {
            return StatusCode(StatusCodes.Status502BadGateway, new
            {
                success = false,
                error = "facebook_api_error",
                message = ex.Message
            });
        }
    }

    [HttpGet("comments")]
    public async Task<IActionResult> GetComments([FromQuery] string postId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(postId))
        {
            return BadRequest(new
            {
                success = false,
                error = "invalid_request",
                message = "postId is required."
            });
        }

        try
        {
            var response = await _facebookGraphService.GetCommentsAsync(postId, cancellationToken);
            return Ok(new { success = true, data = response });
        }
        catch (Exception ex)
        {
            return StatusCode(StatusCodes.Status502BadGateway, new
            {
                success = false,
                error = "facebook_api_error",
                message = ex.Message
            });
        }
    }
}

public sealed record CreatePostRequest(string Message);
