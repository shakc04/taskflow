import Database from 'better-sqlite3';
import { Job, JobStatus } from '../core/job';
import { RetryPolicy } from '../core/retry';

export interface DeadLetterEntry extends Job {
  failure_reason: string;
  moved_at: number;
}

interface DeadLetterRow {
  id: string;
  type: string;
  payload: string;
  priority: number;
  status: JobStatus;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  error: string | null;
  run_at: number | null;
  retry_policy: string | null;
  attempts_remaining: number | null;
  failure_reason: string;
  moved_at: number;
}

/**
 * Holds jobs that exhausted their retries. It shares the jobs database
 * connection so that a move out of the jobs table and into here happens against
 * the same store the rest of the system reads.
 */
export class DeadLetterStore {
  constructor(private readonly db: Database.Database) {
    this.migrate();
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS dlq (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        payload TEXT NOT NULL,
        priority INTEGER NOT NULL,
        status TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        started_at INTEGER,
        finished_at INTEGER,
        error TEXT,
        run_at INTEGER,
        retry_policy TEXT,
        attempts_remaining INTEGER,
        failure_reason TEXT NOT NULL,
        moved_at INTEGER NOT NULL
      );
    `);
  }

  add(job: Job, failureReason: string, movedAt: number): void {
    const statement = this.db.prepare(`
      INSERT INTO dlq (id, type, payload, priority, status, created_at, started_at, finished_at, error,
        run_at, retry_policy, attempts_remaining, failure_reason, moved_at)
      VALUES (@id, @type, @payload, @priority, @status, @created_at, @started_at, @finished_at, @error,
        @run_at, @retry_policy, @attempts_remaining, @failure_reason, @moved_at)
    `);
    statement.run({
      id: job.id,
      type: job.type,
      payload: JSON.stringify(job.payload),
      priority: job.priority,
      status: job.status,
      created_at: job.created_at,
      started_at: job.started_at,
      finished_at: job.finished_at,
      error: job.error,
      run_at: job.run_at ?? job.created_at,
      retry_policy: job.retry_policy == null ? null : JSON.stringify(job.retry_policy),
      attempts_remaining: job.attempts_remaining ?? null,
      failure_reason: failureReason,
      moved_at: movedAt,
    });
  }

  list(): DeadLetterEntry[] {
    const rows = this.db.prepare('SELECT * FROM dlq ORDER BY moved_at ASC').all() as DeadLetterRow[];
    return rows.map((row) => this.fromRow(row));
  }

  getById(id: string): DeadLetterEntry | null {
    const row = this.db.prepare('SELECT * FROM dlq WHERE id = ?').get(id) as DeadLetterRow | undefined;
    if (row === undefined) {
      return null;
    }
    return this.fromRow(row);
  }

  remove(id: string): void {
    this.db.prepare('DELETE FROM dlq WHERE id = ?').run(id);
  }

  private fromRow(row: DeadLetterRow): DeadLetterEntry {
    const parsed: unknown = JSON.parse(row.payload);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error(`dlq entry ${row.id} has a payload that is not an object`);
    }
    const retryPolicy: RetryPolicy | null =
      row.retry_policy === null ? null : (JSON.parse(row.retry_policy) as RetryPolicy);
    const entry: DeadLetterEntry = {
      id: row.id,
      type: row.type,
      payload: parsed as Record<string, unknown>,
      priority: row.priority,
      status: row.status,
      created_at: row.created_at,
      started_at: row.started_at,
      finished_at: row.finished_at,
      error: row.error,
      run_at: row.run_at ?? row.created_at,
      retry_policy: retryPolicy,
      failure_reason: row.failure_reason,
      moved_at: row.moved_at,
    };
    if (row.attempts_remaining !== null) {
      entry.attempts_remaining = row.attempts_remaining;
    }
    return entry;
  }
}
