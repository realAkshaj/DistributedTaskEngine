namespace Dte.Orchestrator.Domain;

public enum JobStatus
{
    Pending,
    Assigned,
    Processing,
    Success,
    Failed,
    DeadLettered,
    Cancelled
}

public static class JobStatusExtensions
{
    public static bool IsTerminal(this JobStatus status) =>
        status is JobStatus.Success or JobStatus.Failed
               or JobStatus.DeadLettered or JobStatus.Cancelled;
}
