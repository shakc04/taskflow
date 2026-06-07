import Database from 'better-sqlite3';
import { Job, JobStatus } from '../core/job';

/**
 * Shape of a row as SQLite hands it back. Payload is stored as a JSON string
 * and booleans/nulls map onto the column types below, so this is a separate
 * type from Job and gets converted on read.
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
        error TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
    `);
  }

  insert(job: Job): void {
    const statement = this.db.prepare(`
      INSERT INTO jobs (id, type, payload, priority, status, created_at, started_at, finished_at, error)
      VALUES (@id, @type, @payload, @priority, @status, @created_at, @started_at, @finished_at, @error)
    `);
    statement.run(this.toRow(job));
  }

  update(job: Job): void {
    const statement = this.db.prepare(`
      UPDATE jobs
      SET type = @type, payload = @payload, priority = @priority, status = @status,
          created_at = @created_at, started_at = @started_at, finished_at = @finished_at, error = @error
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
    };
  }

  private fromRow(row: JobRow): Job {
    const parsed: unknown = JSON.parse(row.payload);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error(`job ${row.id} has a payload that is not an object`);
    }
    return {
      id: row.id,
      type: row.type,
      payload: parsed as Record<string, unknown>,
      priority: row.priority,
      status: row.status,
      created_at: row.created_at,
      started_at: row.started_at,
      finished_at: row.finished_at,
      error: row.error,
    };
  }
}
