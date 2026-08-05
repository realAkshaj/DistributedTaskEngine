using System.Text.Json;

namespace Dte.Orchestrator.Api.Dto;

public sealed record SubmitJobsRequest(List<JobSpec> Jobs, string? Submitter = null);

public sealed record JobSpec(
    string JobType,
    JsonElement Payload,
    short Priority = 5,
    int MaxAttempts = 3,
    int? EstimatedRuntimeMs = null);

public sealed record SubmitJobsResponse(Guid BatchId, List<Guid> JobIds);
