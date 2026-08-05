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

        var ok = await _repo.CancelAsync(ids[0]);
        Assert.True(ok);

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
        var ok = await _repo.CancelAsync(Guid.NewGuid());
        Assert.False(ok);
    }

    [Fact]
    public async Task Cancel_TerminalTask_ReturnsFalse_NoNewEvent()
    {
        var batchId = await _repo.CreateBatchAsync(null);
        var ids = await _repo.EnqueueAsync(batchId, [new NewTask("noop", "{}")]);

        await _repo.CancelAsync(ids[0]);
        var second = await _repo.CancelAsync(ids[0]);

        Assert.False(second);
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
