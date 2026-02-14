using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Tests.Fixtures;
using PgBouncer.Tests.Protocol;
using Xunit;
using Xunit.Abstractions;

namespace PgBouncer.Tests.Integration;

/// <summary>
/// End-to-end integration tests for System.IO.Pipelines architecture.
/// Tests PipelinesClientSession with BackendConnection for zero-allocation I/O.
/// </summary>
[Collection("PgBouncerIntegration")]
public class PipelinesArchitectureIntegrationTests
{
    private readonly PgBouncerTestFixture _fixture;
    private readonly ITestOutputHelper _output;

    public PipelinesArchitectureIntegrationTests(PgBouncerTestFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task BackendConnection_SocketCommunication_ShouldWork()
    {
        // Arrange
        var (clientSocket, serverSocket) = CreateSocketPair();

        try
        {
            // Act - Create BackendConnection and send data
            var logger = CreateLogger();
            var backend = new BackendConnection(serverSocket, "testdb", "testuser", logger);

            // Write using the Writer (PipeWriter)
            var testData = new byte[] { 1, 2, 3, 4, 5 };
            await backend.Writer.WriteAsync(testData);
            await backend.Writer.FlushAsync();

            // Read from client socket to verify
            await using var clientStream = new NetworkStream(clientSocket);
            var readBuffer = new byte[10];
            var bytesRead = await clientStream.ReadAsync(readBuffer, 0, readBuffer.Length);

            // Assert - Data should be received
            Assert.Equal(testData.Length, bytesRead);
            Assert.Equal(testData, readBuffer[..bytesRead]);
        }
        finally
        {
            CleanupSockets(clientSocket, serverSocket);
        }
    }

    [Fact]
    public void PgProtocolHelpers_CreateStartupMessage_ShouldHaveCorrectFormat()
    {
        // Act
        var message = PgProtocolHelpers.CreateStartupMessage("testuser", "testdb", "testapp");

        // Assert - Verify message structure
        Assert.True(message.Length >= 4, "Message should have at least 4 bytes for length");

        // Read length
        int offset = 0;
        int length = PgProtocolHelpers.ReadInt32_BE(message, ref offset);
        Assert.Equal(message.Length, length);

        // Read protocol version
        int protocolVersion = PgProtocolHelpers.ReadInt32_BE(message, ref offset);
        Assert.Equal(0x00030000, protocolVersion); // Protocol version 3.0

        // Read parameters
        var parameters = new Dictionary<string, string>();
        while (offset < message.Length - 1)
        {
            string key = PgProtocolHelpers.ReadPgString(message, ref offset);
            if (string.IsNullOrEmpty(key)) break;
            string value = PgProtocolHelpers.ReadPgString(message, ref offset);
            parameters[key] = value;
        }

        Assert.Equal("testuser", parameters["user"]);
        Assert.Equal("testdb", parameters["database"]);
        Assert.Equal("testapp", parameters["application_name"]);
    }

    [Fact]
    public void PgProtocolHelpers_CreateQueryMessage_ShouldHaveCorrectFormat()
    {
        // Act
        var message = PgProtocolHelpers.CreateQueryMessage("SELECT 1");

        // Assert
        Assert.Equal((byte)'Q', message[0]); // Query message type

        // Read length
        int offset = 1;
        int length = PgProtocolHelpers.ReadInt32_BE(message, ref offset);
        Assert.Equal(message.Length - 1, length);

        // Read query string
        string query = PgProtocolHelpers.ReadPgString(message, ref offset);
        Assert.Equal("SELECT 1", query);
    }

    [Fact]
    public void PgProtocolHelpers_CreateSSLRequest_ShouldHaveCorrectFormat()
    {
        // Act
        var message = PgProtocolHelpers.CreateSSLRequest();

        // Assert
        Assert.Equal(8, message.Length);

        int offset = 0;
        Assert.Equal(8, PgProtocolHelpers.ReadInt32_BE(message, ref offset));
        Assert.Equal(0x04D2162F, PgProtocolHelpers.ReadInt32_BE(message, ref offset)); // SSLRequest code
    }

    [Fact]
    public void PgProtocolHelpers_CreateRowDescription_ShouldWork()
    {
        // Act
        var message = PgProtocolHelpers.CreateRowDescription(
            ("id", 0, 1, 23, 4, -1, 0),   // int4
            ("name", 0, 2, 25, -1, -1, 0)  // text
        );

        // Assert
        Assert.Equal((byte)'T', message[0]); // RowDescription message type
    }

    [Fact]
    public void PgProtocolHelpers_CreateDataRow_ShouldWork()
    {
        // Act
        var message = PgProtocolHelpers.CreateDataRow(
            new[] { "1" },
            new[] { "test" }
        );

        // Assert
        Assert.Equal((byte)'D', message[0]); // DataRow message type
    }

    [Fact]
    public void PgProtocolHelpers_CreateCommandComplete_ShouldWork()
    {
        // Act
        var message = PgProtocolHelpers.CreateCommandComplete("SELECT 1");

        // Assert
        Assert.Equal((byte)'C', message[0]); // CommandComplete message type
    }

    [Fact]
    public void PgProtocolHelpers_CreateReadyForQuery_ShouldWork()
    {
        // Act
        var message = PgProtocolHelpers.CreateReadyForQuery('I');

        // Assert
        Assert.Equal((byte)'Z', message[0]); // ReadyForQuery message type
        Assert.Equal(5, message.Length);
    }

    [Fact]
    public void PgProtocolHelpers_CreateAuthenticationOk_ShouldWork()
    {
        // Act
        var message = PgProtocolHelpers.CreateAuthenticationOk();

        // Assert
        Assert.Equal((byte)'R', message[0]); // Authentication message type
    }

    #region Helper Methods

    private (Socket Client, Socket Server) CreateSocketPair()
    {
        var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        listener.Listen(1);

        var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        client.Connect(listener.LocalEndPoint!);

        var server = listener.Accept();
        listener.Close();

        return (client, server);
    }

    private void CleanupSockets(params Socket[] sockets)
    {
        foreach (var socket in sockets)
        {
            try
            {
                socket.Shutdown(SocketShutdown.Both);
                socket.Close();
                socket.Dispose();
            }
            catch { }
        }
    }

    private ILogger CreateLogger()
    {
        return new TestLogger(_output);
    }

    #endregion

    #region Mock Classes

    private class TestLogger : ILogger
    {
        private readonly ITestOutputHelper _output;

        public TestLogger(ITestOutputHelper output)
        {
            _output = output;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull
        {
            return null!;
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            _output.WriteLine($"[{logLevel}] {formatter(state, exception)}");
        }
    }

    #endregion
}
