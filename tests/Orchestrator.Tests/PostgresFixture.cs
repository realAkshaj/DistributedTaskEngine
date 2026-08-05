using Dte.Orchestrator.Persistence;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Dte.Orchestrator.Tests;

public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("dte")
        .WithUsername("dte")
        .WithPassword("dte")
        .Build();

    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        DataSource = new NpgsqlDataSourceBuilder(_container.GetConnectionString()).Build();

        var sqlPath = Path.Combine(AppContext.BaseDirectory, "Persistence", "schema.sql");
        var sql = await File.ReadAllTextAsync(sqlPath);
        await using var conn = await DataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }

    public async Task ResetAsync()
    {
        await using var conn = await DataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            "TRUNCATE task_events, tasks, job_batches, workers RESTART IDENTITY CASCADE", conn);
        await cmd.ExecuteNonQueryAsync();
    }
}
