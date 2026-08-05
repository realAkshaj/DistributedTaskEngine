using System.Text.Json;
using Dte.Orchestrator.Domain;

namespace Dte.Orchestrator.Api.Dto;

public sealed record JobResponse(
    Guid Id,
    Guid BatchId,
    string JobType,
    JsonElement Payload,
    string Status,
    int Attempts,
    int MaxAttempts,
    Guid? AssignedWorkerId,
    DateTime CreatedAt,
    DateTime? StartedAt,
    DateTime? FinishedAt,
    JsonElement? Result,
    string? Error)
{
    public static JobResponse From(TaskRecord t) => new(
        t.Id, t.BatchId, t.JobType,
        JsonDocument.Parse(t.Payload).RootElement,
        t.Status.ToString(),
        t.Attempts, t.MaxAttempts,
        t.AssignedWorkerId,
        t.CreatedAt, t.StartedAt, t.FinishedAt,
        t.Result is null ? null : JsonDocument.Parse(t.Result).RootElement,
        t.Error);
}
