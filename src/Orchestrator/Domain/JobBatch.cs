namespace Dte.Orchestrator.Domain;

public sealed record JobBatch(Guid Id, DateTime SubmittedAt, string? Submitter);
