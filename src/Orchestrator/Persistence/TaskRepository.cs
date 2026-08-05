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

    public async Task<bool> CancelAsync(Guid taskId, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var current = await conn.QuerySingleOrDefaultAsync<string?>(new CommandDefinition(
            "SELECT status FROM tasks WHERE id = @taskId FOR UPDATE",
            new { taskId }, tx, cancellationToken: ct));

        if (current is null) { await tx.RollbackAsync(ct); return false; }

        var from = Enum.Parse<JobStatus>(current);
        if (from.IsTerminal()) { await tx.RollbackAsync(ct); return false; }

        await conn.ExecuteAsync(new CommandDefinition(
            @"UPDATE tasks
              SET status = 'Cancelled', finished_at = now()
              WHERE id = @taskId",
            new { taskId }, tx, cancellationToken: ct));

        await LogEventAsync(conn, (NpgsqlTransaction)tx, taskId, from, JobStatus.Cancelled, null, null, ct);

        await tx.CommitAsync(ct);
        return true;
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
}
