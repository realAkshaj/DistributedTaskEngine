using Dte.Orchestrator.Api.Dto;
using Dte.Orchestrator.Domain;
using Dte.Orchestrator.Persistence;

namespace Dte.Orchestrator.Api;

public static class JobsEndpoints
{
    public static IEndpointRouteBuilder MapJobs(this IEndpointRouteBuilder app)
    {
        var g = app.MapGroup("/api/v1/jobs");

        g.MapPost("/", Submit);
        g.MapGet("/{id:guid}", GetOne);
        g.MapGet("/", Query);
        g.MapPost("/{id:guid}/cancel", Cancel);

        return app;
    }

    static async Task<IResult> Submit(SubmitJobsRequest req, ITaskRepository repo, CancellationToken ct)
    {
        if (req.Jobs is null || req.Jobs.Count == 0)
            return Results.BadRequest(new { error = "jobs must be non-empty" });

        var batchId = await repo.CreateBatchAsync(req.Submitter, ct);
        var newTasks = req.Jobs
            .Select(j => new NewTask(
                j.JobType, j.Payload.GetRawText(), j.Priority, j.MaxAttempts, j.EstimatedRuntimeMs))
            .ToList();

        var ids = await repo.EnqueueAsync(batchId, newTasks, ct);
        return Results.Ok(new SubmitJobsResponse(batchId, ids.ToList()));
    }

    static async Task<IResult> GetOne(Guid id, ITaskRepository repo, CancellationToken ct)
    {
        var t = await repo.GetAsync(id, ct);
        return t is null ? Results.NotFound() : Results.Ok(JobResponse.From(t));
    }

    static async Task<IResult> Query(
        Guid? batchId, string? status, int? limit,
        ITaskRepository repo, CancellationToken ct)
    {
        JobStatus? parsed = null;
        if (!string.IsNullOrEmpty(status))
        {
            if (!Enum.TryParse<JobStatus>(status, ignoreCase: true, out var s))
                return Results.BadRequest(new { error = $"unknown status '{status}'" });
            parsed = s;
        }

        var rows = await repo.QueryAsync(batchId, parsed, limit ?? 100, ct);
        return Results.Ok(rows.Select(JobResponse.From));
    }

    static async Task<IResult> Cancel(Guid id, ITaskRepository repo, CancellationToken ct)
    {
        var ok = await repo.CancelAsync(id, ct);
        return ok ? Results.NoContent() : Results.Conflict(new { error = "task missing or already terminal" });
    }
}
