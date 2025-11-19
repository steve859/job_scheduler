
CREATE KEYSPACE IF NOT EXISTS scheduler
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

USE scheduler;

CREATE TABLE IF NOT EXISTS jobs (
  job_id text PRIMARY KEY,
  schedule text,
  payload text,
  max_duration_seconds int,
  created_at timestamp
);

CREATE TABLE IF NOT EXISTS job_history (
  job_id text,
  run_id uuid,
  worker_id text,
  fencing_token bigint,
  start_at timestamp,
  end_at timestamp,
  status text,
  PRIMARY KEY ((job_id), run_id)
);

CREATE TABLE IF NOT EXISTS locks (
  resource text PRIMARY KEY,
  holder text,
  fencing_token bigint,
  expire_at timestamp,
  metadata map<text,text>
);

CREATE TABLE IF NOT EXISTS lock_seq (
  resource text PRIMARY KEY,
  last_seq bigint
);
