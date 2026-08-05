using Dte.Orchestrator.Api;
using Dte.Orchestrator.Persistence;
using Npgsql;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((ctx, cfg) => cfg
    .ReadFrom.Configuration(ctx.Configuration)
    .WriteTo.Console());

var connString = builder.Configuration.GetConnectionString("Default")
    ?? throw new InvalidOperationException("ConnectionStrings:Default is required");

builder.Services.AddSingleton(_ => new NpgsqlDataSourceBuilder(connString).Build());
builder.Services.AddScoped<ITaskRepository, TaskRepository>();
builder.Services.AddSingleton<SchemaInitializer>();

var app = builder.Build();

await app.Services.GetRequiredService<SchemaInitializer>().InitializeAsync();

app.MapGet("/health/live",  () => Results.Ok(new { status = "live" }));
app.MapGet("/health/ready", async (NpgsqlDataSource ds) =>
{
    try
    {
        await using var conn = await ds.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        await cmd.ExecuteScalarAsync();
        return Results.Ok(new { status = "ready" });
    }
    catch (Exception ex)
    {
        return Results.Json(new { status = "unready", error = ex.Message }, statusCode: 503);
    }
});

app.MapJobs();

app.Run();

public partial class Program { }
