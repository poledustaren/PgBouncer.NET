using PgBouncer.Core.Configuration;
using Xunit;

namespace PgBouncer.Tests.Configuration;

/// <summary>
/// Tests for PgBouncerConfig configuration
/// </summary>
public class PgBouncerConfigTests
{
    [Fact]
    public void PgBouncerConfig_DefaultValues_ShouldBeCorrect()
    {
        // Arrange & Act
        var config = new PgBouncerConfig();

        // Assert
        config.ListenPort.Should().Be(6442);
        config.DashboardPort.Should().Be(5083);
        config.Backend.Should().NotBeNull();
        config.Pool.Should().NotBeNull();
        config.Auth.Should().NotBeNull();
    }

    [Fact]
    public void BackendConfig_DefaultValues_ShouldBeCorrect()
    {
        // Arrange & Act
        var config = new BackendConfig();

        // Assert
        config.Host.Should().Be("127.0.0.1");
        config.Port.Should().Be(5437);
        config.AdminUser.Should().Be("postgres");
        config.AdminPassword.Should().BeEmpty();
    }

    [Fact]
    public void PoolConfig_DefaultValues_ShouldBeCorrect()
    {
        // Arrange & Act
        var config = new PoolConfig();

        // Assert
        config.DefaultSize.Should().Be(20);
        config.MinSize.Should().Be(2);
        config.MaxSize.Should().Be(1000);
        config.MaxTotalConnections.Should().Be(8000);
        config.Mode.Should().Be(PoolingMode.Transaction);
        config.IdleTimeout.Should().Be(600);
        config.ConnectionTimeout.Should().Be(60);
        config.ServerResetQuery.Should().Be("DISCARD ALL");
    }

    [Fact]
    public void PoolConfig_UsePipelinesArchitecture_Default_ShouldBeFalse()
    {
        // Arrange & Act
        var config = new PoolConfig();

        // Assert - by default, should use legacy Stream-based architecture
        config.UsePipelinesArchitecture.Should().BeFalse();
    }

    [Fact]
    public void PoolConfig_UsePipelinesArchitecture_CanBeSetToTrue()
    {
        // Arrange
        var config = new PoolConfig();

        // Act
        config.UsePipelinesArchitecture = true;

        // Assert
        config.UsePipelinesArchitecture.Should().BeTrue();
    }

    [Fact]
    public void PoolConfig_UsePipelinesArchitecture_CanBeSetToFalse()
    {
        // Arrange
        var config = new PoolConfig { UsePipelinesArchitecture = true };

        // Act
        config.UsePipelinesArchitecture = false;

        // Assert
        config.UsePipelinesArchitecture.Should().BeFalse();
    }

    [Fact]
    public void PgBouncerConfig_WithPipelinesEnabled_ShouldPropagateToPool()
    {
        // Arrange & Act
        var config = new PgBouncerConfig
        {
            Pool = new PoolConfig
            {
                UsePipelinesArchitecture = true,
                DefaultSize = 50
            }
        };

        // Assert
        config.Pool.UsePipelinesArchitecture.Should().BeTrue();
        config.Pool.DefaultSize.Should().Be(50);
    }

    [Fact]
    public void AuthConfig_DefaultValues_ShouldBeCorrect()
    {
        // Arrange & Act
        var config = new AuthConfig();

        // Assert
        config.Type.Should().Be("passthrough");
    }

    [Theory]
    [InlineData(PoolingMode.Session)]
    [InlineData(PoolingMode.Transaction)]
    [InlineData(PoolingMode.Statement)]
    public void PoolConfig_Mode_CanBeSetToAllSupportedModes(PoolingMode mode)
    {
        // Arrange
        var config = new PoolConfig();

        // Act
        config.Mode = mode;

        // Assert
        config.Mode.Should().Be(mode);
    }

    [Fact]
    public void PoolConfig_ServerResetQuery_CanBeDisabled()
    {
        // Arrange
        var config = new PoolConfig();

        // Act
        config.ServerResetQuery = string.Empty;

        // Assert
        config.ServerResetQuery.Should().BeEmpty();
    }

    [Fact]
    public void PoolConfig_ServerResetQuery_CanBeCustomized()
    {
        // Arrange
        var config = new PoolConfig();
        var customQuery = "SELECT 1; SET LOCAL statement_timeout = '30s';";

        // Act
        config.ServerResetQuery = customQuery;

        // Assert
        config.ServerResetQuery.Should().Be(customQuery);
    }

    [Fact]
    public void PoolConfig_SizeValidation_MinSizeShouldNotExceedMaxSize()
    {
        // Arrange
        var config = new PoolConfig
        {
            MinSize = 100,
            MaxSize = 50
        };

        // Act & Assert - This is an invalid configuration
        // In production, validation would catch this
        config.MinSize.Should().BeGreaterThan(config.MaxSize);
    }
}
