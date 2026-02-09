using PgBouncer.Core.Configuration;

namespace PgBouncer.Tests;

/// <summary>
/// Тесты конфигурации PgBouncer
/// </summary>
public class ConfigurationTests
{
    [Fact]
    public void DefaultConfig_ShouldHaveValidDefaults()
    {
        // Arrange & Act
        var config = new PgBouncerConfig();
        
        // Assert
        config.Server.Port.Should().Be(6432);
        config.Pool.MaxPoolSize.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Backend_ShouldHaveDefaultValues()
    {
        // Arrange & Act
        var config = new PgBouncerConfig();
        
        // Assert
        config.Backend.Host.Should().Be("localhost");
        config.Backend.Port.Should().Be(5432);
    }

    [Fact]
    public void PoolConfig_ShouldHaveSensibleDefaults()
    {
        // Arrange & Act
        var config = new PoolConfig();
        
        // Assert
        config.MinPoolSize.Should().BeGreaterThanOrEqualTo(0);
        config.MaxPoolSize.Should().BeGreaterThan(config.MinPoolSize);
        config.MaxTotalConnections.Should().BeGreaterThanOrEqualTo(config.MaxPoolSize);
    }
}
