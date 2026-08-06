using Dapper;
using Dte.Orchestrator.Domain;
using Dte.Orchestrator.Persistence;
using Xunit;

namespace Dte.Orchestrator.Tests;

public sealed class TaskRepositoryTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture _pg;
    private readonly TaskRepository _repo;

    public TaskRepositoryTests(PostgresFixture pg)
    {
        _pg = pg;
        _repo = new TaskRepository(pg.DataSource);
    }

    public Task InitializeAsync() => _pg.ResetAsync();
    public Task DisposeAsync()    => Task.CompletedTask;

    [Fact]
    public async Task Enqueue_CreatesPendingRow_WithZeroAttempts()
    {
        var batchId = await _repo.CreateBatchAsync("test");
        var ids = await _repo.EnqueueAsync(batchId, [new NewTask("noop", "{}")]);

        var t = await _repo.GetAsync(ids[0]);
        Assert.NotNull(t);
        Assert.Equal(JobStatus.Pending, t!.Status);
        Assert.Equal(0, t.Attempts);
        Assert.Equal(batchId, t.BatchId);
    }

    [Fact]
    public async Task Enqueue_LogsPendingEvent()
    {
        var batchId = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(batchId, [new NewTask("noop", "{}")]);

        var events = await EventsFor(ids[0]);
        Assert.Single(events);
        Assert.Null(events[0].from_status);
        Assert.Equal("Pending", events[0].to_status);
    }

    [Fact]
    public async Task Enqueue_BatchShareBatchId()
    {
        var batchId = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(batchId, [
            new NewTask("a", "{}"),
            new NewTask("b", "{}"),
            new NewTask("c", "{}"),
        ]);

        Assert.Equal(3, ids.Count);
        Assert.Equal(3, ids.Distinct().Count());
        foreach (var id in ids)
        {
            var t = await _repo.GetAsync(id);
            Assert.Equal(batchId, t!.BatchId);
        }
    }

    [Fact]
    public async Task Cancel_PendingTask_TransitionsToCancelled()
    {
        var batchId = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(batchId, [new NewTask("noop", "{}")]);

        var outcome = await _repo.CancelAsync(ids[0]);
        Assert.True(outcome.Cancelled);
        Assert.Null(outcome.PreviouslyAssignedWorker);

        var t = await _repo.GetAsync(ids[0]);
        Assert.Equal(JobStatus.Cancelled, t!.Status);
        Assert.NotNull(t.FinishedAt);
    }

    [Fact]
    public async Task Cancel_LogsTransitionEvent()
    {
        var batchId = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(batchId, [new NewTask("noop", "{}")]);

        await _repo.CancelAsync(ids[0]);

        var events = await EventsFor(ids[0]);
        Assert.Equal(2, events.Count);
        Assert.Equal("Pending", events[1].from_status);
        Assert.Equal("Cancelled", events[1].to_status);
    }

    [Fact]
    public async Task Cancel_UnknownId_ReturnsFalse()
    {
        var outcome = await _repo.CancelAsync(Guid.NewGuid());
        Assert.False(outcome.Cancelled);
    }

    [Fact]
    public async Task Cancel_TerminalTask_ReturnsFalse_NoNewEvent()
    {
        var batchId = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(batchId, [new NewTask("noop", "{}")]);

        await _repo.CancelAsync(ids[0]);
        var second = await _repo.CancelAsync(ids[0]);

        Assert.False(second.Cancelled);
        var events = await EventsFor(ids[0]);
        Assert.Equal(2, events.Count);
    }

    [Fact]
    public async Task Query_FiltersByStatus()
    {
        var batchId = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(batchId, [
            new NewTask("a", "{}"), new NewTask("b", "{}"), new NewTask("c", "{}"),
        ]);
        await _repo.CancelAsync(ids[0]);

        var pending   = await _repo.QueryAsync(batchId, JobStatus.Pending, 10);
        var cancelled = await _repo.QueryAsync(batchId, JobStatus.Cancelled, 10);

        Assert.Equal(2, pending.Count);
        Assert.Single(cancelled);
    }

    // ----- scheduling ops -----

    [Fact]
    public async Task GetPending_ReturnsInPriorityOrder()
    {
        var b = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(b, [
            new NewTask("a", "{}", Priority: 1),
            new NewTask("b", "{}", Priority: 9),
            new NewTask("c", "{}", Priority: 5),
        ]);

        var pending = await _repo.GetPendingAsync(10);
        Assert.Equal(3, pending.Count);
        Assert.Equal("b", pending[0].JobType);
        Assert.Equal("c", pending[1].JobType);
        Assert.Equal("a", pending[2].JobType);
    }

    [Fact]
    public async Task Assign_MoveTaskToAssigned_WithLease()
    {
        var b = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(b, [new NewTask("x", "{}")]);
        var worker = Guid.NewGuid();

        var assigned = await _repo.AssignBatchAsync([(ids[0], worker)], TimeSpan.FromSeconds(30));
        Assert.Single(assigned);

        var t = await _repo.GetAsync(ids[0]);
        Assert.Equal(JobStatus.Assigned, t!.Status);
        Assert.Equal(worker, t.AssignedWorkerId);
        Assert.NotNull(t.LeaseExpiresAt);
    }

    [Fact]
    public async Task Assign_SkipsTasksNotPending()
    {
        var b = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(b, [new NewTask("x", "{}")]);
        await _repo.CancelAsync(ids[0]);

        var assigned = await _repo.AssignBatchAsync([(ids[0], Guid.NewGuid())], TimeSpan.FromSeconds(30));
        Assert.Empty(assigned);
    }

    [Fact]
    public async Task MarkCompleted_HappyPath()
    {
        var b = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(b, [new NewTask("x", "{}")]);
        var worker = Guid.NewGuid();
        await _repo.AssignBatchAsync([(ids[0], worker)], TimeSpan.FromSeconds(30));
        await _repo.MarkStartedAsync(ids[0], worker);

        var ok = await _repo.MarkCompletedAsync(ids[0], worker, "{\"n\":1}");
        Assert.True(ok);

        var t = await _repo.GetAsync(ids[0]);
        Assert.Equal(JobStatus.Success, t!.Status);
        Assert.NotNull(t.FinishedAt);
    }

    [Fact]
    public async Task MarkCompleted_RejectsStaleWorker()
    {
        var b = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(b, [new NewTask("x", "{}")]);
        var actual = Guid.NewGuid();
        var stale = Guid.NewGuid();
        await _repo.AssignBatchAsync([(ids[0], actual)], TimeSpan.FromSeconds(30));

        var ok = await _repo.MarkCompletedAsync(ids[0], stale, "{}");
        Assert.False(ok);

        var t = await _repo.GetAsync(ids[0]);
        Assert.Equal(JobStatus.Assigned, t!.Status);
    }

    [Fact]
    public async Task MarkFailed_RequeuesWhenAttemptsRemain()
    {
        var b = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(b, [new NewTask("x", "{}", MaxAttempts: 3)]);
        var w = Guid.NewGuid();
        await _repo.AssignBatchAsync([(ids[0], w)], TimeSpan.FromSeconds(30));

        var outcome = await _repo.MarkFailedAsync(ids[0], w, "boom");
        Assert.Equal(CompletionOutcome.Requeued, outcome);

        var t = await _repo.GetAsync(ids[0]);
        Assert.Equal(JobStatus.Pending, t!.Status);
        Assert.Equal(1, t.Attempts);
        Assert.Null(t.AssignedWorkerId);
    }

    [Fact]
    public async Task MarkFailed_DeadLettersWhenAttemptsExhausted()
    {
        var b = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(b, [new NewTask("x", "{}", MaxAttempts: 1)]);
        var w = Guid.NewGuid();
        await _repo.AssignBatchAsync([(ids[0], w)], TimeSpan.FromSeconds(30));

        var outcome = await _repo.MarkFailedAsync(ids[0], w, "boom");
        Assert.Equal(CompletionOutcome.DeadLettered, outcome);

        var t = await _repo.GetAsync(ids[0]);
        Assert.Equal(JobStatus.DeadLettered, t!.Status);
        Assert.Equal("boom", t.Error);
    }

    [Fact]
    public async Task Reap_MovesExpiredAssignmentsBackToPending()
    {
        var b = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(b, [new NewTask("x", "{}")]);
        var w = Guid.NewGuid();
        await _repo.AssignBatchAsync([(ids[0], w)], TimeSpan.FromMilliseconds(1));
        await Task.Delay(50);

        var reaped = await _repo.ReapExpiredLeasesAsync();
        Assert.Single(reaped);
        Assert.Equal(w, reaped[0].WorkerId);
        Assert.False(reaped[0].DeadLettered);

        var t = await _repo.GetAsync(ids[0]);
        Assert.Equal(JobStatus.Pending, t!.Status);
        Assert.Equal(1, t.Attempts);
    }

    private async Task<List<EventRow>> EventsFor(Guid taskId)
    {
        await using var conn = await _pg.DataSource.OpenConnectionAsync();
        var rows = await conn.QueryAsync<EventRow>(
            "SELECT from_status, to_status, at FROM task_events WHERE task_id = @taskId ORDER BY at",
            new { taskId });
        return rows.ToList();
    }

    private sealed record EventRow(string? from_status, string to_status, DateTime at);
}
