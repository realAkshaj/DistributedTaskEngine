namespace Dte.Orchestrator.Domain;

public sealed record TaskEvent(
    long Id,
    Guid TaskId,
    JobStatus? FromStatus,
    JobStatus ToStatus,
    Guid? WorkerId,
    DateTime At,
    string? Detail);
