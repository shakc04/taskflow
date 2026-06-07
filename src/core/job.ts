export type JobStatus = 'pending' | 'running' | 'done' | 'failed';

export interface Job {
  id: string;
  type: string;
  payload: Record<string, unknown>;
  /** Lower number wins. The caller decides what the scale means for their domain. */
  priority: number;
  status: JobStatus;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  error: string | null;
}
