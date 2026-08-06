using System.Threading.Channels;
using Dte.V1;

namespace Dte.Orchestrator.Scheduling;

public sealed class WorkerConnection : IDisposable
{
    private readonly Channel<OrchestratorMessage> _outbound =
        Channel.CreateUnbounded<OrchestratorMessage>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    private readonly CancellationTokenSource _cts = new();
    private int _availableSlots;

    public WorkerConnection(Guid id, int maxParallel, IReadOnlySet<string> jobTypes)
    {
        Id = id;
        MaxParallel = maxParallel;
        JobTypes = jobTypes;
        _availableSlots = maxParallel;
    }

    public Guid Id { get; }
    public int MaxParallel { get; }
    public IReadOnlySet<string> JobTypes { get; }
    public int AvailableSlots => Volatile.Read(ref _availableSlots);
    public ChannelReader<OrchestratorMessage> Outbound => _outbound.Reader;
    public CancellationToken ShutdownToken => _cts.Token;

    public bool TryReserve()
    {
        while (true)
        {
            var current = Volatile.Read(ref _availableSlots);
            if (current <= 0) return false;
            if (Interlocked.CompareExchange(ref _availableSlots, current - 1, current) == current)
                return true;
        }
    }

    public void Release()
    {
        while (true)
        {
            var current = Volatile.Read(ref _availableSlots);
            if (current >= MaxParallel) return;
            if (Interlocked.CompareExchange(ref _availableSlots, current + 1, current) == current)
                return;
        }
    }

    public bool TrySend(OrchestratorMessage msg) => _outbound.Writer.TryWrite(msg);

    public void Dispose()
    {
        _outbound.Writer.TryComplete();
        _cts.Cancel();
        _cts.Dispose();
    }
}
