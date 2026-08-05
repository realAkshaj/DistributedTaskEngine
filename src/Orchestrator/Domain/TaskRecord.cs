namespace Dte.Orchestrator.Domain;

public sealed class TaskRecord
{
    public Guid Id { get; init; }
    public Guid BatchId { get; init; }
    public string JobType { get; init; } = "";
    public string Payload { get; init; } = "{}";
    public short Priority { get; init; }
    public JobStatus Status { get; set; }
    public int Attempts { get; set; }
    public int MaxAttempts { get; init; }
    public Guid? AssignedWorkerId { get; set; }
    public DateTime? LeaseExpiresAt { get; set; }
    public int? EstimatedRuntimeMs { get; init; }
    public DateTime CreatedAt { get; init; }
    public DateTime? StartedAt { get; set; }
    public DateTime? FinishedAt { get; set; }
    public string? Result { get; set; }
    public string? Error { get; set; }
}
