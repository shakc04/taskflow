import Database from 'better-sqlite3';
import { Job, JobStatus } from '../core/job';
import { RetryPolicy } from '../core/retry';

/**
 * Shape of a row as SQLite hands it back. Payload and retry_policy are stored
 * as JSON strings and booleans/nulls map onto the column types below, so this
 * is a separate type from Job and gets converted on read.
 */
interface JobRow {
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
}

export class JobStore {
  private readonly db: Database.Database;

  constructor(filename: string) {
    this.db = new Database(filename);
    // WAL lets reads from the HTTP layer run while a worker writes a status
    // transition, which avoids blocking the API on background work.
    this.db.pragma('journal_mode = WAL');
    this.migrate();
  }

  /**
   * Hands the underlying connection to the dead letter store so both tables
   * live in one database. This matters for an in-memory database, where each
   * separate connection would otherwise see its own private schema.
   */
  getConnection(): Database.Database {
    return this.db;
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
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
        attempts_remaining INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
      CREATE INDEX IF NOT EXISTS idx_jobs_run_at ON jobs (run_at);
    `);
    // A database written by an earlier version has the jobs table already, and
    // CREATE TABLE IF NOT EXISTS will not add the newer columns to it, so each
    // one is added here when it is missing.
    this.ensureColumn('run_at', 'INTEGER');
    this.ensureColumn('retry_policy', 'TEXT');
    this.ensureColumn('attempts_remaining', 'INTEGER');
  }

  private ensureColumn(name: string, type: string): void {
    const columns = this.db.prepare('PRAGMA table_info(jobs)').all() as Array<{ name: string }>;
    if (!columns.some((column) => column.name === name)) {
      this.db.exec(`ALTER TABLE jobs ADD COLUMN ${name} ${type}`);
    }
  }

  insert(job: Job): void {
    const statement = this.db.prepare(`
      INSERT INTO jobs (id, type, payload, priority, status, created_at, started_at, finished_at, error,
        run_at, retry_policy, attempts_remaining)
      VALUES (@id, @type, @payload, @priority, @status, @created_at, @started_at, @finished_at, @error,
        @run_at, @retry_policy, @attempts_remaining)
    `);
    statement.run(this.toRow(job));
  }

  update(job: Job): void {
    const statement = this.db.prepare(`
      UPDATE jobs
      SET type = @type, payload = @payload, priority = @priority, status = @status,
          created_at = @created_at, started_at = @started_at, finished_at = @finished_at, error = @error,
          run_at = @run_at, retry_policy = @retry_policy, attempts_remaining = @attempts_remaining
      WHERE id = @id
    `);
    statement.run(this.toRow(job));
  }

  getById(id: string): Job | null {
    const row = this.db.prepare('SELECT * FROM jobs WHERE id = ?').get(id) as JobRow | undefined;
    if (row === undefined) {
      return null;
    }
    return this.fromRow(row);
  }

  list(status?: JobStatus): Job[] {
    const rows =
      status === undefined
        ? (this.db.prepare('SELECT * FROM jobs ORDER BY created_at ASC').all() as JobRow[])
        : (this.db
            .prepare('SELECT * FROM jobs WHERE status = ? ORDER BY created_at ASC')
            .all(status) as JobRow[]);
    return rows.map((row) => this.fromRow(row));
  }

  loadPending(): Job[] {
    return this.list('pending');
  }

  /**
   * Pending jobs whose run_at has arrived. A run_at left null by an older row
   * counts as due so that jobs predating deferred scheduling still run.
   */
  loadDue(now: number): Job[] {
    const rows = this.db
      .prepare(
        `SELECT * FROM jobs
         WHERE status = 'pending' AND (run_at IS NULL OR run_at <= ?)
         ORDER BY priority ASC, created_at ASC`,
      )
      .all(now) as JobRow[];
    return rows.map((row) => this.fromRow(row));
  }

  delete(id: string): void {
    this.db.prepare('DELETE FROM jobs WHERE id = ?').run(id);
  }

  close(): void {
    this.db.close();
  }

  private toRow(job: Job): JobRow {
    return {
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
    };
  }

  private fromRow(row: JobRow): Job {
    const parsed: unknown = JSON.parse(row.payload);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error(`job ${row.id} has a payload that is not an object`);
    }
    const retryPolicy: RetryPolicy | null =
      row.retry_policy === null ? null : (JSON.parse(row.retry_policy) as RetryPolicy);
    const job: Job = {
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
    };
    if (row.attempts_remaining !== null) {
      job.attempts_remaining = row.attempts_remaining;
    }
    return job;
  }
}
