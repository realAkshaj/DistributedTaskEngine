using System.Data;
using System.Diagnostics.CodeAnalysis;
using Dapper;
using Dte.Orchestrator.Domain;
using Npgsql;
using NpgsqlTypes;

namespace Dte.Orchestrator.Persistence;

public sealed class TaskRepository(NpgsqlDataSource dataSource) : ITaskRepository
{
    static TaskRepository()
    {
        DefaultTypeMap.MatchNamesWithUnderscores = true;
        SqlMapper.AddTypeHandler(new JobStatusHandler());
    }

    public async Task<Guid> CreateBatchAsync(string? submitter, CancellationToken ct = default)
    {
        var id = Guid.NewGuid();
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(
            "INSERT INTO job_batches (id, submitter) VALUES (@id, @submitter)",
            new { id, submitter },
            cancellationToken: ct));
        return id;
    }

    public async Task<IReadOnlyList<Guid>> EnqueueAsync(
        Guid batchId,
        IReadOnlyList<NewTask> tasks,
        CancellationToken ct = default)
    {
        if (tasks.Count == 0) return Array.Empty<Guid>();

        var ids = new Guid[tasks.Count];
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        for (var i = 0; i < tasks.Count; i++)
        {
            var t = tasks[i];
            var id = Guid.NewGuid();
            ids[i] = id;

            await using var cmd = new NpgsqlCommand(
                @"INSERT INTO tasks
                    (id, batch_id, job_type, payload, priority, status,
                     attempts, max_attempts, estimated_runtime_ms)
                  VALUES
                    (@id, @batch_id, @job_type, @payload::jsonb, @priority, 'Pending',
                     0, @max_attempts, @estimated_runtime_ms)",
                conn, (NpgsqlTransaction)tx);
            cmd.Parameters.AddWithValue("id", id);
            cmd.Parameters.AddWithValue("batch_id", batchId);
            cmd.Parameters.AddWithValue("job_type", t.JobType);
            cmd.Parameters.Add(new NpgsqlParameter("payload", NpgsqlDbType.Jsonb) { Value = t.Payload });
            cmd.Parameters.AddWithValue("priority", t.Priority);
            cmd.Parameters.AddWithValue("max_attempts", t.MaxAttempts);
            cmd.Parameters.AddWithValue("estimated_runtime_ms",
                (object?)t.EstimatedRuntimeMs ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);

            await LogEventAsync(conn, (NpgsqlTransaction)tx, id, null, JobStatus.Pending, null, null, ct);
        }

        await tx.CommitAsync(ct);
        return ids;
    }

    public async Task<TaskRecord?> GetAsync(Guid taskId, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<TaskRecord>(new CommandDefinition(
            "SELECT * FROM tasks WHERE id = @taskId",
            new { taskId },
            cancellationToken: ct));
    }

    public async Task<IReadOnlyList<TaskRecord>> QueryAsync(
        Guid? batchId,
        JobStatus? status,
        int limit,
        CancellationToken ct = default)
    {
        var sql = "SELECT * FROM tasks WHERE 1=1";
        if (batchId.HasValue) sql += " AND batch_id = @batchId";
        if (status.HasValue)  sql += " AND status = @status";
        sql += " ORDER BY created_at DESC LIMIT @limit";

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<TaskRecord>(new CommandDefinition(
            sql,
            new { batchId, status = status?.ToString(), limit },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<CancelOutcome> CancelAsync(Guid taskId, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var current = await conn.QuerySingleOrDefaultAsync<LockedRow>(new CommandDefinition(
            "SELECT status, assigned_worker_id AS assignedworkerid, attempts, max_attempts AS maxattempts FROM tasks WHERE id = @taskId FOR UPDATE",
            new { taskId }, tx, cancellationToken: ct));

        if (current is null) { await tx.RollbackAsync(ct); return new CancelOutcome(false, null); }

        var from = Enum.Parse<JobStatus>(current.Status);
        if (from.IsTerminal()) { await tx.RollbackAsync(ct); return new CancelOutcome(false, null); }

        await conn.ExecuteAsync(new CommandDefinition(
            @"UPDATE tasks
              SET status = 'Cancelled', finished_at = now(),
                  assigned_worker_id = NULL, lease_expires_at = NULL
              WHERE id = @taskId",
            new { taskId }, tx, cancellationToken: ct));

        await LogEventAsync(conn, (NpgsqlTransaction)tx, taskId, from, JobStatus.Cancelled, null, null, ct);
        await tx.CommitAsync(ct);
        return new CancelOutcome(true, current.AssignedWorkerId);
    }

    public async Task<IReadOnlyList<TaskRecord>> GetPendingAsync(int limit, CancellationToken ct = default)
    {
        if (limit <= 0) return Array.Empty<TaskRecord>();
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<TaskRecord>(new CommandDefinition(
            @"SELECT * FROM tasks
              WHERE status = 'Pending'
              ORDER BY priority DESC, created_at ASC
              LIMIT @limit",
            new { limit }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<IReadOnlyList<(Guid TaskId, Guid WorkerId)>> AssignBatchAsync(
        IReadOnlyList<(Guid TaskId, Guid WorkerId)> pairs,
        TimeSpan leaseDuration,
        CancellationToken ct = default)
    {
        if (pairs.Count == 0) return Array.Empty<(Guid, Guid)>();

        var success = new List<(Guid, Guid)>(pairs.Count);
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        foreach (var (taskId, workerId) in pairs)
        {
            var affected = await conn.ExecuteAsync(new CommandDefinition(
                @"UPDATE tasks
                  SET status = 'Assigned',
                      assigned_worker_id = @workerId,
                      lease_expires_at = now() + @lease
                  WHERE id = @taskId AND status = 'Pending'",
                new { taskId, workerId, lease = leaseDuration }, tx, cancellationToken: ct));
            if (affected == 0) continue;
            await LogEventAsync(conn, (NpgsqlTransaction)tx, taskId, JobStatus.Pending, JobStatus.Assigned, workerId, null, ct);
            success.Add((taskId, workerId));
        }

        await tx.CommitAsync(ct);
        return success;
    }

    public async Task<bool> MarkStartedAsync(Guid taskId, Guid workerId, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var affected = await conn.ExecuteAsync(new CommandDefinition(
            @"UPDATE tasks
              SET status = 'Processing', started_at = now()
              WHERE id = @taskId AND assigned_worker_id = @workerId AND status = 'Assigned'",
            new { taskId, workerId }, tx, cancellationToken: ct));
        if (affected == 0) { await tx.RollbackAsync(ct); return false; }

        await LogEventAsync(conn, (NpgsqlTransaction)tx, taskId, JobStatus.Assigned, JobStatus.Processing, workerId, null, ct);
        await tx.CommitAsync(ct);
        return true;
    }

    public async Task<bool> MarkCompletedAsync(Guid taskId, Guid workerId, string result, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var current = await conn.QuerySingleOrDefaultAsync<LockedRow>(new CommandDefinition(
            "SELECT status, assigned_worker_id AS assignedworkerid, attempts, max_attempts AS maxattempts FROM tasks WHERE id = @taskId FOR UPDATE",
            new { taskId }, tx, cancellationToken: ct));

        if (current is null || current.AssignedWorkerId != workerId
            || (current.Status != "Assigned" && current.Status != "Processing"))
        {
            await tx.RollbackAsync(ct);
            return false;
        }

        var prev = Enum.Parse<JobStatus>(current.Status);
        await conn.ExecuteAsync(new CommandDefinition(
            @"UPDATE tasks
              SET status = 'Success', result = @result::jsonb, finished_at = now()
              WHERE id = @taskId",
            new { taskId, result }, tx, cancellationToken: ct));

        await LogEventAsync(conn, (NpgsqlTransaction)tx, taskId, prev, JobStatus.Success, workerId, null, ct);
        await tx.CommitAsync(ct);
        return true;
    }

    public async Task<CompletionOutcome> MarkFailedAsync(Guid taskId, Guid workerId, string error, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var current = await conn.QuerySingleOrDefaultAsync<LockedRow>(new CommandDefinition(
            "SELECT status, assigned_worker_id AS assignedworkerid, attempts, max_attempts AS maxattempts FROM tasks WHERE id = @taskId FOR UPDATE",
            new { taskId }, tx, cancellationToken: ct));

        if (current is null || current.AssignedWorkerId != workerId
            || (current.Status != "Assigned" && current.Status != "Processing"))
        {
            await tx.RollbackAsync(ct);
            return CompletionOutcome.Ignored;
        }

        var prev = Enum.Parse<JobStatus>(current.Status);
        var deadLetter = current.Attempts + 1 >= current.MaxAttempts;

        if (deadLetter)
        {
            await conn.ExecuteAsync(new CommandDefinition(
                @"UPDATE tasks
                  SET status = 'DeadLettered', attempts = attempts + 1, finished_at = now(),
                      assigned_worker_id = NULL, lease_expires_at = NULL, error = @error
                  WHERE id = @taskId",
                new { taskId, error }, tx, cancellationToken: ct));
            await LogEventAsync(conn, (NpgsqlTransaction)tx, taskId, prev, JobStatus.DeadLettered, workerId, null, ct);
            await tx.CommitAsync(ct);
            return CompletionOutcome.DeadLettered;
        }
        else
        {
            await conn.ExecuteAsync(new CommandDefinition(
                @"UPDATE tasks
                  SET status = 'Pending', attempts = attempts + 1,
                      assigned_worker_id = NULL, lease_expires_at = NULL, error = @error
                  WHERE id = @taskId",
                new { taskId, error }, tx, cancellationToken: ct));
            await LogEventAsync(conn, (NpgsqlTransaction)tx, taskId, prev, JobStatus.Pending, workerId, null, ct);
            await tx.CommitAsync(ct);
            return CompletionOutcome.Requeued;
        }
    }

    public async Task<bool> RenewLeaseAsync(Guid taskId, Guid workerId, TimeSpan leaseDuration, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var affected = await conn.ExecuteAsync(new CommandDefinition(
            @"UPDATE tasks
              SET lease_expires_at = now() + @lease
              WHERE id = @taskId AND assigned_worker_id = @workerId
                    AND status IN ('Assigned', 'Processing')",
            new { taskId, workerId, lease = leaseDuration }, cancellationToken: ct));
        return affected > 0;
    }

    public async Task<IReadOnlyList<ReapedTask>> ReapExpiredLeasesAsync(CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var expired = (await conn.QueryAsync<ExpiredRow>(new CommandDefinition(
            @"SELECT id, assigned_worker_id AS assignedworkerid, status, attempts, max_attempts AS maxattempts
              FROM tasks
              WHERE status IN ('Assigned', 'Processing') AND lease_expires_at < now()
              FOR UPDATE SKIP LOCKED",
            transaction: tx, cancellationToken: ct))).ToList();

        var reaped = new List<ReapedTask>(expired.Count);
        foreach (var e in expired)
        {
            var prev = Enum.Parse<JobStatus>(e.Status);
            var deadLetter = e.Attempts + 1 >= e.MaxAttempts;
            var next = deadLetter ? JobStatus.DeadLettered : JobStatus.Pending;

            if (deadLetter)
            {
                await conn.ExecuteAsync(new CommandDefinition(
                    @"UPDATE tasks
                      SET status = 'DeadLettered', attempts = attempts + 1, finished_at = now(),
                          assigned_worker_id = NULL, lease_expires_at = NULL,
                          error = 'lease expired; retry budget exhausted'
                      WHERE id = @id",
                    new { id = e.Id }, tx, cancellationToken: ct));
            }
            else
            {
                await conn.ExecuteAsync(new CommandDefinition(
                    @"UPDATE tasks
                      SET status = 'Pending', attempts = attempts + 1,
                          assigned_worker_id = NULL, lease_expires_at = NULL
                      WHERE id = @id",
                    new { id = e.Id }, tx, cancellationToken: ct));
            }

            await LogEventAsync(conn, (NpgsqlTransaction)tx, e.Id, prev, next, e.AssignedWorkerId, "\"lease_expired\"", ct);
            reaped.Add(new ReapedTask(e.Id, e.AssignedWorkerId, prev, deadLetter));
        }

        await tx.CommitAsync(ct);
        return reaped;
    }

    static async Task LogEventAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx,
        Guid taskId, JobStatus? from, JobStatus to,
        Guid? workerId, string? detail, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            @"INSERT INTO task_events (task_id, from_status, to_status, worker_id, detail)
              VALUES (@task_id, @from_status, @to_status, @worker_id, @detail::jsonb)",
            conn, tx);
        cmd.Parameters.AddWithValue("task_id", taskId);
        cmd.Parameters.AddWithValue("from_status", (object?)from?.ToString() ?? DBNull.Value);
        cmd.Parameters.AddWithValue("to_status", to.ToString());
        cmd.Parameters.AddWithValue("worker_id", (object?)workerId ?? DBNull.Value);
        cmd.Parameters.Add(new NpgsqlParameter("detail", NpgsqlDbType.Jsonb)
            { Value = (object?)detail ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private sealed class JobStatusHandler : SqlMapper.TypeHandler<JobStatus>
    {
        public override JobStatus Parse(object value) => Enum.Parse<JobStatus>((string)value);
        public override void SetValue(IDbDataParameter p, [AllowNull] JobStatus value) =>
            p.Value = value.ToString();
    }

    private sealed record LockedRow(string Status, Guid? AssignedWorkerId, int Attempts, int MaxAttempts);
    private sealed record ExpiredRow(Guid Id, Guid AssignedWorkerId, string Status, int Attempts, int MaxAttempts);
}
