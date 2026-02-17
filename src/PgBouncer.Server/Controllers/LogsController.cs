using Microsoft.AspNetCore.Mvc;

namespace PgBouncer.Server.Controllers;

[ApiController]
[Route("api/logs")]
public class LogsController : ControllerBase
{
    [HttpGet]
    public IActionResult GetLogs()
    {
        var logDir = Path.Combine(AppContext.BaseDirectory, "logs");
        if (!Directory.Exists(logDir))
            return Ok(Array.Empty<string>());

        var files = Directory.GetFiles(logDir, "log_*.txt")
            .Select(Path.GetFileName)
            .OrderByDescending(f => f)
            .ToList();

        return Ok(files);
    }

    [HttpGet("{filename}")]
    public IActionResult GetLogContent(string filename)
    {
        if (string.IsNullOrEmpty(filename) || filename.Contains("/") || filename.Contains("\\"))
            return BadRequest("Invalid filename");

        var path = Path.Combine(AppContext.BaseDirectory, "logs", filename);
        if (!System.IO.File.Exists(path))
            return NotFound();

        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        return File(fs, "text/plain");
    }
}
