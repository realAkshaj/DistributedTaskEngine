using System.Collections.Concurrent;

namespace Dte.Orchestrator.Scheduling;

public sealed class WorkerRegistry
{
    private readonly ConcurrentDictionary<Guid, WorkerConnection> _byId = new();

    public bool TryAdd(WorkerConnection conn) => _byId.TryAdd(conn.Id, conn);

    public bool TryRemove(Guid id, out WorkerConnection? removed)
    {
        var ok = _byId.TryRemove(id, out var c);
        removed = c;
        return ok;
    }

    public WorkerConnection? Get(Guid id) => _byId.GetValueOrDefault(id);

    public IReadOnlyList<WorkerConnection> Snapshot() => _byId.Values.ToList();

    public void ReleaseSlot(Guid workerId)
    {
        if (_byId.TryGetValue(workerId, out var conn)) conn.Release();
    }
}
