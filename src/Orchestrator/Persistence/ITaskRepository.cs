using Dte.Orchestrator.Domain;

namespace Dte.Orchestrator.Persistence;

public interface ITaskRepository
{
    Task<Guid> CreateBatchAsync(string? submitter, CancellationToken ct = default);

    Task<IReadOnlyList<Guid>> EnqueueAsync(
        Guid batchId,
        IReadOnlyList<NewTask> tasks,
        CancellationToken ct = default);

    Task<TaskRecord?> GetAsync(Guid taskId, CancellationToken ct = default);

    Task<IReadOnlyList<TaskRecord>> QueryAsync(
        Guid? batchId,
        JobStatus? status,
        int limit,
        CancellationToken ct = default);

    Task<bool> CancelAsync(Guid taskId, CancellationToken ct = default);
}

public sealed record NewTask(
    string JobType,
    string Payload,
    short Priority = 5,
    int MaxAttempts = 3,
    int? EstimatedRuntimeMs = null);
