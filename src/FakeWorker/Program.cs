using System.Threading.Channels;
using Dte.V1;
using Grpc.Core;
using Grpc.Net.Client;

var workerId = Environment.GetEnvironmentVariable("DTE_WORKER_ID") is { } wid && Guid.TryParse(wid, out var g)
    ? g : Guid.NewGuid();
var maxParallel = int.TryParse(Environment.GetEnvironmentVariable("DTE_MAX_PARALLEL"), out var mp) ? mp : 4;
var jobTypes = (Environment.GetEnvironmentVariable("DTE_JOB_TYPES") ?? "graph.bfs,string.suffix,noop")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var address = Environment.GetEnvironmentVariable("DTE_ORCHESTRATOR") ?? "http://localhost:5001";
var minMs = int.TryParse(Environment.GetEnvironmentVariable("DTE_MIN_RUN_MS"), out var mn) ? mn : 100;
var maxMs = int.TryParse(Environment.GetEnvironmentVariable("DTE_MAX_RUN_MS"), out var mx) ? mx : 500;

Console.WriteLine($"[fake-worker] id={workerId} parallel={maxParallel} types=[{string.Join(",", jobTypes)}] -> {address}");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

using var channel = GrpcChannel.ForAddress(address);
var client = new TaskDispatch.TaskDispatchClient(channel);
using var call = client.Stream(cancellationToken: cts.Token);

await call.RequestStream.WriteAsync(new WorkerMessage
{
    Hello = new Hello
    {
        WorkerId = workerId.ToString(),
        Version = "fake-0.1",
        MaxParallel = maxParallel,
        JobTypes = { jobTypes }
    }
});

var inbox = Channel.CreateUnbounded<Assignment>(new UnboundedChannelOptions { SingleReader = false });
var outbox = call.RequestStream;
var outboxLock = new SemaphoreSlim(1, 1);

async Task SendAsync(WorkerMessage msg)
{
    await outboxLock.WaitAsync(cts.Token);
    try { await outbox.WriteAsync(msg, cts.Token); }
    finally { outboxLock.Release(); }
}

var rng = new Random();
var executors = Enumerable.Range(0, maxParallel).Select(_ => Task.Run(async () =>
{
    await foreach (var a in inbox.Reader.ReadAllAsync(cts.Token))
    {
        try
        {
            await SendAsync(new WorkerMessage { Started = new TaskStarted { TaskId = a.TaskId } });
            var delay = rng.Next(minMs, maxMs + 1);
            await Task.Delay(delay, cts.Token);
            await SendAsync(new WorkerMessage
            {
                Completed = new TaskCompleted
                {
                    TaskId = a.TaskId,
                    Result = Google.Protobuf.ByteString.CopyFromUtf8($"{{\"ok\":true,\"ms\":{delay}}}"),
                    Metrics = new ExecutionMetrics { WallMs = delay }
                }
            });
        }
        catch (OperationCanceledException) { break; }
        catch (Exception ex)
        {
            await SendAsync(new WorkerMessage
            {
                Failed = new TaskFailed { TaskId = a.TaskId, Error = ex.Message }
            });
        }
    }
})).ToArray();

var heartbeat = Task.Run(async () =>
{
    while (!cts.IsCancellationRequested)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
            await SendAsync(new WorkerMessage { Heartbeat = new Heartbeat() });
        }
        catch (OperationCanceledException) { break; }
    }
});

try
{
    await foreach (var msg in call.ResponseStream.ReadAllAsync(cts.Token))
    {
        switch (msg.KindCase)
        {
            case OrchestratorMessage.KindOneofCase.Welcome:
                Console.WriteLine($"[fake-worker] welcome (server_time={msg.Welcome.ServerTimeUnixMs}, hb={msg.Welcome.HeartbeatIntervalMs}ms)");
                break;
            case OrchestratorMessage.KindOneofCase.Assignment:
                await inbox.Writer.WriteAsync(msg.Assignment, cts.Token);
                break;
            case OrchestratorMessage.KindOneofCase.Shutdown:
                Console.WriteLine("[fake-worker] shutdown requested");
                cts.Cancel();
                break;
        }
    }
}
catch (OperationCanceledException) { }
catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { }
catch (Exception ex)
{
    Console.Error.WriteLine($"[fake-worker] stream error: {ex.Message}");
}
finally
{
    inbox.Writer.TryComplete();
    try { await call.RequestStream.CompleteAsync(); } catch { }
    await Task.WhenAll(executors);
    cts.Cancel();
    try { await heartbeat; } catch { }
}

Console.WriteLine("[fake-worker] exit");
