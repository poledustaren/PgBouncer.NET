using System.Buffers;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Server;
using Xunit;

namespace PgBouncer.Tests.Client;

/// <summary>
/// Tests for PipelinesClientSession - System.IO.Pipelines based client connection handler
/// </summary>
public class PipelinesClientSessionTests : IAsyncDisposable
{
    private readonly List<Socket> _socketsToCleanup = new();
    private readonly List<PipelinesClientSession> _sessionsToCleanup = new();

    public async ValueTask DisposeAsync()
    {
        foreach (var session in _sessionsToCleanup)
        {
            try
            {
                await session.DisposeAsync();
            }
            catch { }
        }

        foreach (var socket in _socketsToCleanup)
        {
            try
            {
                socket.Shutdown(SocketShutdown.Both);
                socket.Close();
            }
            catch { }
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Creates a pair of connected sockets for testing
    /// </summary>
    private (Socket Client, Socket Server) CreateSocketPair()
    {
        var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        listener.Listen(1);

        var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        client.Connect(listener.LocalEndPoint!);

        var server = listener.Accept();
        listener.Close();

        _socketsToCleanup.Add(client);
        _socketsToCleanup.Add(server);

        return (client, server);
    }

    /// <summary>
    /// Mock IConnectionPool for testing
    /// </summary>
    private class MockConnectionPool : IConnectionPool
    {
        public readonly List<IServerConnection> AcquiredConnections = new();
        public readonly List<IServerConnection> ReleasedConnections = new();
        public int AcquireCallCount { get; private set; }
        public int ReleaseCallCount { get; private set; }
        public IServerConnection? ConnectionToReturn { get; set; }
        public Exception? ExceptionToThrow { get; set; }
        private readonly TaskCompletionSource<IServerConnection?> _acquireTcs = new();

        public Task<IServerConnection> AcquireAsync(CancellationToken cancellationToken)
        {
            AcquireCallCount++;
            if (ExceptionToThrow != null)
            {
                throw ExceptionToThrow;
            }

            if (ConnectionToReturn != null)
            {
                AcquiredConnections.Add(ConnectionToReturn);
                _acquireTcs.TrySetResult(ConnectionToReturn);
                return Task.FromResult(ConnectionToReturn);
            }

            var tcs = new TaskCompletionSource<IServerConnection?>();
            cancellationToken.Register(() => tcs.TrySetCanceled());
            return tcs.Task;
        }

        public void Release(IServerConnection connection)
        {
            ReleaseCallCount++;
            ReleasedConnections.Add(connection);
        }

        public PoolStats GetStats() => new PoolStats
        {
            Database = "testdb",
            Username = "testuser",
            TotalConnections = AcquiredConnections.Count - ReleasedConnections.Count,
            ActiveConnections = AcquiredConnections.Count - ReleasedConnections.Count,
            IdleConnections = 0,
            MaxConnections = 100
        };

        public Task InitializeAsync(int minConnections, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public void RecordSuccess(Guid connectionId) { }

        public void RecordFailure(Guid connectionId) { }

        public void Dispose() { }
    }

    /// <summary>
    /// Creates a test configuration
    /// </summary>
    private PgBouncerConfig CreateTestConfig(PoolingMode mode = PoolingMode.Transaction)
    {
        return new PgBouncerConfig
        {
            Pool = new PoolConfig
            {
                Mode = mode,
                DefaultSize = 10,
                MaxSize = 100
            },
            Backend = new BackendConfig
            {
                Host = "127.0.0.1",
                Port = 5432,
                AdminPassword = "test"
            }
        };
    }

    /// <summary>
    /// Creates a mock logger
    /// </summary>
    private ILogger CreateMockLogger()
    {
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
        return loggerFactory.CreateLogger<PipelinesClientSessionTests>();
    }

    [Fact]
    public void Constructor_ShouldInitializeProperties()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var config = CreateTestConfig();
        var pool = new MockConnectionPool();
        var logger = CreateMockLogger();
        var sessionInfo = new SessionInfo { Id = Guid.NewGuid(), StartedAt = DateTime.UtcNow };

        // Act
        var session = new PipelinesClientSession(server, config, pool, logger, sessionInfo);
        _sessionsToCleanup.Add(session);

        // Assert
        session.IsAuthenticated.Should().BeFalse();
        session.IsDisposed.Should().BeFalse();
    }

    [Fact]
    public async Task DisposeAsync_ShouldCleanupResources()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var config = CreateTestConfig();
        var pool = new MockConnectionPool();
        var logger = CreateMockLogger();
        var sessionInfo = new SessionInfo { Id = Guid.NewGuid(), StartedAt = DateTime.UtcNow };

        var session = new PipelinesClientSession(server, config, pool, logger, sessionInfo);

        // Act
        await session.DisposeAsync();

        // Assert
        session.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task RunAsync_WithSessionPooling_ShouldAcquireBackendOnce()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var (backendClient, backendServer) = CreateSocketPair();

        var config = CreateTestConfig(PoolingMode.Session);
        var pool = new MockConnectionPool();

        // Create a backend connection to return from pool
        var backendConnection = new BackendConnection(backendServer, "testdb", "testuser");
        pool.ConnectionToReturn = backendConnection;

        var logger = CreateMockLogger();
        var sessionInfo = new SessionInfo { Id = Guid.NewGuid(), StartedAt = DateTime.UtcNow };

        var session = new PipelinesClientSession(server, config, pool, logger, sessionInfo);
        _sessionsToCleanup.Add(session);

        // Send StartupMessage
        var startupMsg = CreateStartupMessage("testdb", "testuser");
        await client.SendAsync(new ArraySegment<byte>(startupMsg), SocketFlags.None);

        // Act - wait a bit for processing
        await Task.Delay(100);

        // Assert
        pool.AcquireCallCount.Should().Be(1); // Should only acquire once for session
        sessionInfo.State.Should().Be(SessionState.Active);
    }

    [Fact]
    public async Task ReleaseBackendAsync_ShouldReturnConnectionToPool()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var (backendClient, backendServer) = CreateSocketPair();

        var config = CreateTestConfig(PoolingMode.Transaction);
        var pool = new MockConnectionPool();

        var backendConnection = new BackendConnection(backendServer, "testdb", "testuser");
        pool.ConnectionToReturn = backendConnection;

        var logger = CreateMockLogger();
        var sessionInfo = new SessionInfo { Id = Guid.NewGuid(), StartedAt = DateTime.UtcNow };

        var session = new PipelinesClientSession(server, config, pool, logger, sessionInfo);
        _sessionsToCleanup.Add(session);

        // Send StartupMessage
        var startupMsg = CreateStartupMessage("testdb", "testuser");
        await client.SendAsync(new ArraySegment<byte>(startupMsg), SocketFlags.None);
        await Task.Delay(100);

        // Act - dispose session to trigger backend release
        await session.DisposeAsync();

        // Assert
        pool.ReleaseCallCount.Should().Be(1);
        pool.ReleasedConnections.Should().Contain(backendConnection);
    }

    // Helper methods

    private byte[] CreateStartupMessage(string database, string user)
    {
        using var ms = new MemoryStream();

        // Length (will be updated later)
        ms.Write(new byte[4]);

        // Protocol version 3.0
        var versionBytes = BitConverter.GetBytes(196608);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(versionBytes);
        ms.Write(versionBytes);

        // Parameters
        WriteParameter(ms, "database", database);
        WriteParameter(ms, "user", user);

        // Terminator
        ms.WriteByte(0);

        // Update length
        var length = (int)ms.Length;
        var lengthBytes = BitConverter.GetBytes(length);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(lengthBytes);

        var buffer = ms.ToArray();
        Buffer.BlockCopy(lengthBytes, 0, buffer, 0, 4);

        return buffer;
    }

    private byte[] CreateQueryMessage(string sql)
    {
        var sqlBytes = System.Text.Encoding.UTF8.GetBytes(sql);
        var result = new byte[5 + sqlBytes.Length + 1]; // 'Q' + length + sql + null terminator

        result[0] = (byte)'Q';
        var length = 4 + sqlBytes.Length + 1;
        result[1] = (byte)(length >> 24);
        result[2] = (byte)(length >> 16);
        result[3] = (byte)(length >> 8);
        result[4] = (byte)length;

        Array.Copy(sqlBytes, 0, result, 5, sqlBytes.Length);
        result[5 + sqlBytes.Length] = 0;

        return result;
    }

    private static void WriteParameter(MemoryStream ms, string key, string value)
    {
        var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
        var valueBytes = System.Text.Encoding.UTF8.GetBytes(value);
        ms.Write(keyBytes);
        ms.WriteByte(0);
        ms.Write(valueBytes);
        ms.WriteByte(0);
    }
}
