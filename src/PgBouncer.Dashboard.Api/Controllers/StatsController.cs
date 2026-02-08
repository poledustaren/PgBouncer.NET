using Microsoft.AspNetCore.Mvc;
using PgBouncer.Core.Pooling;

namespace PgBouncer.Dashboard.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class StatsController : ControllerBase
{
    private readonly PoolManager _poolManager;
    private readonly ILogger<StatsController> _logger;

    public StatsController(PoolManager poolManager, ILogger<StatsController> logger)
    {
        _poolManager = poolManager;
        _logger = logger;
    }

    /// <summary>
    /// Получить статистику всех пулов
    /// </summary>
    [HttpGet]
    public ActionResult<IEnumerable<PoolStats>> GetAllStats()
    {
        var stats = _poolManager.GetAllStats();
        return Ok(stats);
    }

    /// <summary>
    /// Получить статистику конкретного пула
    /// </summary>
    [HttpGet("{database}/{username}")]
    public ActionResult<PoolStats> GetPoolStats(string database, string username)
    {
        var stats = _poolManager.GetStats(database, username);
        
        if (stats == null)
            return NotFound($"Пул для {database}/{username} не найден");

        return Ok(stats);
    }

    /// <summary>
    /// Общая статистика
    /// </summary>
    [HttpGet("summary")]
    public ActionResult<object> GetSummary()
    {
        var allStats = _poolManager.GetAllStats().ToList();
        
        return Ok(new
        {
            TotalPools = allStats.Count,
            TotalConnections = allStats.Sum(s => s.TotalConnections),
            ActiveConnections = allStats.Sum(s => s.ActiveConnections),
            IdleConnections = allStats.Sum(s => s.IdleConnections),
            Pools = allStats
        });
    }

    /// <summary>
    /// Перезагрузить конфигурацию
    /// </summary>
    [HttpPost("reload")]
    public ActionResult Reload()
    {
        _logger.LogInformation("Запрос на перезагрузку конфигурации");
        // TODO: реализовать hot-reload
        return Ok(new { Message = "Конфигурация перезагружена" });
    }
}
