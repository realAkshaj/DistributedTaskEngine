using Dte.Orchestrator.Persistence;

namespace Dte.Orchestrator.Scheduling;

public sealed class LeaseReaperService(
    WorkerRegistry registry,
    IServiceScopeFactory scopes,
    ILogger<LeaseReaperService> log) : BackgroundService
{
    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        log.LogInformation("Lease reaper started (tick={Tick})", TickInterval);

        using var timer = new PeriodicTimer(TickInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try { await TickAsync(stoppingToken); }
            catch (Exception ex) { log.LogError(ex, "Reaper tick failed"); }
        }
    }

    private async Task TickAsync(CancellationToken ct)
    {
        using var scope = scopes.CreateScope();
        var repo = scope.ServiceProvider.GetRequiredService<ITaskRepository>();

        var reaped = await repo.ReapExpiredLeasesAsync(ct);
        if (reaped.Count == 0) return;

        foreach (var r in reaped) registry.ReleaseSlot(r.WorkerId);

        log.LogInformation("Reaped {Count} expired leases ({Dead} dead-lettered)",
            reaped.Count, reaped.Count(r => r.DeadLettered));
    }
}
