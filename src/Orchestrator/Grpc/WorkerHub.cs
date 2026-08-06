using Dte.Orchestrator.Persistence;
using Dte.Orchestrator.Scheduling;
using Dte.V1;
using Google.Protobuf;
using Grpc.Core;

namespace Dte.Orchestrator.Grpc;

public sealed class WorkerHub(
    WorkerRegistry registry,
    ITaskRepository repo,
    ILogger<WorkerHub> log) : TaskDispatch.TaskDispatchBase
{
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(30);

    public override async Task Stream(
        IAsyncStreamReader<WorkerMessage> requestStream,
        IServerStreamWriter<OrchestratorMessage> responseStream,
        ServerCallContext context)
    {
        var ct = context.CancellationToken;

        if (!await requestStream.MoveNext(ct))
        {
            log.LogWarning("Worker stream closed before Hello");
            return;
        }

        var hello = requestStream.Current.Hello;
        if (hello is null)
        {
            log.LogWarning("First worker message was not Hello: {Kind}", requestStream.Current.KindCase);
            return;
        }

        if (!Guid.TryParse(hello.WorkerId, out var workerId))
        {
            log.LogWarning("Invalid worker_id in Hello: {Id}", hello.WorkerId);
            return;
        }

        var conn = new WorkerConnection(workerId, hello.MaxParallel, hello.JobTypes.ToHashSet());
        if (!registry.TryAdd(conn))
        {
            log.LogWarning("Worker {Id} already registered; rejecting duplicate stream", workerId);
            conn.Dispose();
            return;
        }

        log.LogInformation("Worker {Id} connected: parallel={P}, types=[{T}]",
            workerId, hello.MaxParallel, string.Join(",", hello.JobTypes));

        await responseStream.WriteAsync(new OrchestratorMessage
        {
            Welcome = new Welcome
            {
                ServerTimeUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                HeartbeatIntervalMs = 5000
            }
        }, ct);

        var pumpTask = PumpOutbound(conn, responseStream, ct);

        try
        {
            await foreach (var msg in requestStream.ReadAllAsync(ct))
            {
                await HandleAsync(conn, msg, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            log.LogWarning(ex, "Worker {Id} stream error", workerId);
        }
        finally
        {
            registry.TryRemove(workerId, out _);
            conn.Dispose();
            try { await pumpTask; } catch { /* pump already torn down */ }
            log.LogInformation("Worker {Id} disconnected", workerId);
        }
    }

    private static async Task PumpOutbound(
        WorkerConnection conn,
        IServerStreamWriter<OrchestratorMessage> writer,
        CancellationToken ct)
    {
        try
        {
            await foreach (var msg in conn.Outbound.ReadAllAsync(ct))
            {
                await writer.WriteAsync(msg, ct);
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task HandleAsync(WorkerConnection conn, WorkerMessage msg, CancellationToken ct)
    {
        switch (msg.KindCase)
        {
            case WorkerMessage.KindOneofCase.Heartbeat:
                break;

            case WorkerMessage.KindOneofCase.Started:
                if (Guid.TryParse(msg.Started.TaskId, out var startedId))
                    await repo.MarkStartedAsync(startedId, conn.Id, ct);
                break;

            case WorkerMessage.KindOneofCase.Completed:
                if (Guid.TryParse(msg.Completed.TaskId, out var doneId))
                {
                    var result = msg.Completed.Result.IsEmpty ? "{}" : msg.Completed.Result.ToStringUtf8();
                    var accepted = await repo.MarkCompletedAsync(doneId, conn.Id, result, ct);
                    if (accepted) conn.Release();
                    else log.LogInformation("Duplicate/late completion for {Task} from {Worker}", doneId, conn.Id);
                }
                break;

            case WorkerMessage.KindOneofCase.Failed:
                if (Guid.TryParse(msg.Failed.TaskId, out var failedId))
                {
                    var outcome = await repo.MarkFailedAsync(failedId, conn.Id, msg.Failed.Error, ct);
                    if (outcome != CompletionOutcome.Ignored) conn.Release();
                }
                break;

            case WorkerMessage.KindOneofCase.LeaseRenew:
                if (Guid.TryParse(msg.LeaseRenew.TaskId, out var renewId))
                    await repo.RenewLeaseAsync(renewId, conn.Id, LeaseDuration, ct);
                break;

            case WorkerMessage.KindOneofCase.Hello:
                log.LogWarning("Unexpected second Hello from {Worker}", conn.Id);
                break;
        }
    }
}
