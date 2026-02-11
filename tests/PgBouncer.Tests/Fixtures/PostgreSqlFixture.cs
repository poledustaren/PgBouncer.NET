using Testcontainers.PostgreSql;

namespace PgBouncer.Tests.Fixtures;

public class PostgreSqlFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres;
    
    public PostgreSqlFixture()
    {
        _postgres = new PostgreSqlBuilder()
            .WithDatabase("testdb")
            .WithUsername("postgres")
            .WithPassword("testpass123")
            .Build();
    }
    
    public string ConnectionString => _postgres.GetConnectionString();
    
    public Task InitializeAsync() => _postgres.StartAsync();
    
    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();
}
