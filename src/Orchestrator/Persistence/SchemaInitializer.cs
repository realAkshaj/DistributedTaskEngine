using Npgsql;

namespace Dte.Orchestrator.Persistence;

public sealed class SchemaInitializer(NpgsqlDataSource dataSource, ILogger<SchemaInitializer> log)
{
    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var sqlPath = Path.Combine(AppContext.BaseDirectory, "Persistence", "schema.sql");
        var sql = await File.ReadAllTextAsync(sqlPath, ct);

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);

        log.LogInformation("Schema initialized");
    }
}
