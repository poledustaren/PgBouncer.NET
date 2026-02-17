using Microsoft.AspNetCore.Mvc;
using PgBouncer.Core.Authentication;
using PgBouncer.Core.Configuration;

namespace PgBouncer.Server.Controllers;

[ApiController]
[Route("api/admin")]
public class AdminController : ControllerBase
{
    private readonly UserRegistry _userRegistry;
    private readonly PgBouncerConfig _config;

    public AdminController(UserRegistry userRegistry, PgBouncerConfig config)
    {
        _userRegistry = userRegistry;
        _config = config;
    }

    [HttpPost("reload-users")]
    public IActionResult ReloadUsers()
    {
        _userRegistry.Load(_config.Auth.AuthFile);
        return Ok("Users reloaded");
    }
}
