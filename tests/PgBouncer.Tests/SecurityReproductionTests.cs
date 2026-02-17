using System.Net.Sockets;
using PgBouncer.Core.Pooling;
using FluentAssertions;
using System.Buffers.Binary;
using Xunit;
using System.Threading.Tasks;

namespace PgBouncer.Tests;

public class SecurityReproductionTests : IDisposable
{
    private readonly Socket _listener;
    private readonly Socket _client;
    private readonly Socket _server;

    public SecurityReproductionTests()
    {
        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listener.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        _listener.Listen(1);

        _client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _client.Connect(_listener.LocalEndPoint!);

        _server = _listener.Accept();
    }

    public void Dispose()
    {
        _client.Dispose();
        _server.Dispose();
        _listener.Dispose();
    }

    [Fact]
    public async Task ConnectAndAuthenticateAsync_WithLongPassword_ShouldNotThrow()
    {
        // Arrange
        var connection = new BackendConnection(_server, "testdb", "testuser");
        var longPassword = new string('a', 300);

        // Start the authentication process in a task
        var authTask = connection.ConnectAndAuthenticateAsync(longPassword);

        // Server side: receive startup message
        var buffer = new byte[1024];
        var received = await _client.ReceiveAsync(buffer, SocketFlags.None);
        received.Should().BeGreaterThan(0);

        // Server side: send AuthenticationCleartextPassword (R, len=8, code=3)
        var authRequest = new byte[9];
        authRequest[0] = (byte)'R';
        BinaryPrimitives.WriteInt32BigEndian(authRequest.AsSpan(1), 8);
        BinaryPrimitives.WriteInt32BigEndian(authRequest.AsSpan(5), 3);
        await _client.SendAsync(authRequest, SocketFlags.None);

        // Act & Assert
        // This should not throw ArgumentException due to buffer overflow
        Func<Task> act = async () => await authTask;
        await act.Should().NotThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task WriteStartupMessage_WithLongUsername_ShouldNotThrow()
    {
        // Arrange
        var longUser = new string('a', 300);
        var longDb = new string('b', 300);
        var connection = new BackendConnection(_server, longDb, longUser);

        // Act
        // WriteStartupMessage is private, but called by ConnectAndAuthenticateAsync
        var authTask = connection.ConnectAndAuthenticateAsync("password");

        // Assert
        Func<Task> act = async () => await authTask;
        // It might throw because the server socket might be closed or something,
        // but we specifically want to see if it throws because of stackalloc buffer size.
        // If it throws ArgumentException from Encoding.UTF8.GetBytes, that's what we want to catch.
        await act.Should().NotThrowAsync<ArgumentException>();
    }
}
