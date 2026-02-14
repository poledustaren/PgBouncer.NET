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
        config.ListenPort.Should().Be(6442);
        config.Pool.MaxSize.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Backend_ShouldHaveDefaultValues()
    {
        // Arrange & Act
        var config = new PgBouncerConfig();

        // Assert
        config.Backend.Host.Should().Be("127.0.0.1");
        config.Backend.Port.Should().Be(5437);
    }

    [Fact]
    public void PoolConfig_ShouldHaveSensibleDefaults()
    {
        // Arrange & Act
        var config = new PoolConfig();

        // Assert
        config.MinSize.Should().BeGreaterThanOrEqualTo(0);
        config.MaxSize.Should().BeGreaterThan(config.MinSize);
        config.MaxTotalConnections.Should().BeGreaterThanOrEqualTo(config.MaxSize);
    }

    [Fact]
    public void AuthConfig_ShouldDefaultToPassthrough()
    {
        // Arrange & Act
        var config = new AuthConfig();

        // Assert
        config.Type.Should().Be("passthrough");
    }

    [Fact]
    public void PoolingMode_ShouldDefaultToTransaction()
    {
        // Arrange & Act
        var config = new PoolConfig();

        // Assert
        config.Mode.Should().Be(PoolingMode.Transaction);
    }

    [Fact]
    public void UsePipelinesArchitecture_ShouldDefaultToFalse()
    {
        // Arrange & Act
        var config = new PoolConfig();

        // Assert
        config.UsePipelinesArchitecture.Should().BeFalse();
    }

    [Fact]
    public void UsePipelinesArchitecture_ShouldBeConfigurable()
    {
        // Arrange
        var config = new PoolConfig();

        // Act
        config.UsePipelinesArchitecture = true;

        // Assert
        config.UsePipelinesArchitecture.Should().BeTrue();
    }

    [Fact]
    public void FullConfig_ShouldSupportPipelinesToggle()
    {
        // Arrange & Act
        var config = new PgBouncerConfig
        {
            Pool = { UsePipelinesArchitecture = true }
        };

        // Assert
        config.Pool.UsePipelinesArchitecture.Should().BeTrue();
    }
}
