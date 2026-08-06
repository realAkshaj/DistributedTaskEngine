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

    Task<CancelOutcome> CancelAsync(Guid taskId, CancellationToken ct = default);

    Task<IReadOnlyList<TaskRecord>> GetPendingAsync(int limit, CancellationToken ct = default);

    Task<IReadOnlyList<(Guid TaskId, Guid WorkerId)>> AssignBatchAsync(
        IReadOnlyList<(Guid TaskId, Guid WorkerId)> pairs,
        TimeSpan leaseDuration,
        CancellationToken ct = default);

    Task<bool> MarkStartedAsync(Guid taskId, Guid workerId, CancellationToken ct = default);

    Task<bool> MarkCompletedAsync(Guid taskId, Guid workerId, string result, CancellationToken ct = default);

    Task<CompletionOutcome> MarkFailedAsync(Guid taskId, Guid workerId, string error, CancellationToken ct = default);

    Task<bool> RenewLeaseAsync(Guid taskId, Guid workerId, TimeSpan leaseDuration, CancellationToken ct = default);

    Task<IReadOnlyList<ReapedTask>> ReapExpiredLeasesAsync(CancellationToken ct = default);
}

public sealed record NewTask(
    string JobType,
    string Payload,
    short Priority = 5,
    int MaxAttempts = 3,
    int? EstimatedRuntimeMs = null);

public sealed record CancelOutcome(bool Cancelled, Guid? PreviouslyAssignedWorker);

public sealed record ReapedTask(Guid TaskId, Guid WorkerId, JobStatus PreviousStatus, bool DeadLettered);

public enum CompletionOutcome { Requeued, DeadLettered, Ignored }
