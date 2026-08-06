using Dte.Orchestrator.Persistence;
using Dte.V1;
using Google.Protobuf;

namespace Dte.Orchestrator.Scheduling;

public sealed class SchedulerService(
    WorkerRegistry registry,
    IServiceScopeFactory scopes,
    ILogger<SchedulerService> log) : BackgroundService
{
    public static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan TickInterval = TimeSpan.FromMilliseconds(500);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        log.LogInformation("Scheduler started (tick={Tick}, lease={Lease})",
            TickInterval, LeaseDuration);

        using var timer = new PeriodicTimer(TickInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try { await TickAsync(stoppingToken); }
            catch (Exception ex) { log.LogError(ex, "Scheduler tick failed"); }
        }
    }

    private async Task TickAsync(CancellationToken ct)
    {
        var workers = registry.Snapshot();
        var totalSlots = workers.Sum(w => w.AvailableSlots);
        if (totalSlots == 0) return;

        using var scope = scopes.CreateScope();
        var repo = scope.ServiceProvider.GetRequiredService<ITaskRepository>();

        var pending = await repo.GetPendingAsync(totalSlots, ct);
        if (pending.Count == 0) return;

        var reservations = new List<(TaskRecordSnapshot Task, WorkerConnection Worker)>();
        foreach (var task in pending)
        {
            var worker = workers
                .Where(w => w.JobTypes.Contains(task.JobType) && w.AvailableSlots > 0)
                .OrderByDescending(w => w.AvailableSlots)
                .FirstOrDefault();
            if (worker is null) continue;
            if (!worker.TryReserve()) continue;
            reservations.Add((TaskRecordSnapshot.From(task), worker));
        }

        if (reservations.Count == 0) return;

        var pairs = reservations.Select(r => (r.Task.Id, r.Worker.Id)).ToList();
        IReadOnlyList<(Guid TaskId, Guid WorkerId)> assigned;
        try
        {
            assigned = await repo.AssignBatchAsync(pairs, LeaseDuration, ct);
        }
        catch
        {
            foreach (var (_, w) in reservations) w.Release();
            throw;
        }

        var assignedSet = assigned.ToHashSet();
        foreach (var (task, worker) in reservations)
        {
            if (!assignedSet.Contains((task.Id, worker.Id)))
            {
                worker.Release();
                continue;
            }

            var sent = worker.TrySend(new OrchestratorMessage
            {
                Assignment = new Assignment
                {
                    TaskId = task.Id.ToString(),
                    JobType = task.JobType,
                    Payload = ByteString.CopyFromUtf8(task.Payload),
                    Attempt = task.Attempts + 1,
                    LeaseDurationMs = (long)LeaseDuration.TotalMilliseconds
                }
            });
            if (!sent) worker.Release();
        }

        log.LogDebug("Scheduler tick: assigned {Count} tasks across {Workers} workers",
            assigned.Count, workers.Count);
    }

    private sealed record TaskRecordSnapshot(Guid Id, string JobType, string Payload, int Attempts)
    {
        public static TaskRecordSnapshot From(Domain.TaskRecord t) =>
            new(t.Id, t.JobType, t.Payload, t.Attempts);
    }
}
