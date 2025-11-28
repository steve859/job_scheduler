
/* =====================================================================
   KEYSPACE
   ===================================================================== */
CREATE KEYSPACE IF NOT EXISTS scheduler
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

USE scheduler;

/* =====================================================================
   LEGACY SIMPLE JOBS TABLE (KEEP FOR BACKWARDS COMPAT / DEMO)
   Will be superseded by jobs_sharded below.
   ===================================================================== */
CREATE TABLE IF NOT EXISTS jobs (
  job_id text PRIMARY KEY,
  schedule text,
  payload text,
  max_duration_seconds int,
  created_at timestamp
);

/* =====================================================================
   JOB HISTORY (IMMUTABLE EVENT LOG PER JOB RUN)
   ===================================================================== */
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

/* =====================================================================
   DISTRIBUTED LOCKS (CP LAYER)
   DO NOT USE TTL ON ROW; USE expire_at TIMESTAMP FIELD
   Added epoch for takeover sequencing and TWCS compaction
   ===================================================================== */
CREATE TABLE IF NOT EXISTS locks (
  resource_id text PRIMARY KEY,
  holder text,
  fencing_token bigint,
  expire_at timestamp,
  epoch bigint,
  metadata map<text,text>
) WITH compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'MINUTES',
    'compaction_window_size': '1'
  }
  AND gc_grace_seconds = 60;

/* =====================================================================
   SEQUENCE (UNSHARDED) - MAY BE PHASED OUT BY SHARDED SEQ
   ===================================================================== */
CREATE TABLE IF NOT EXISTS lock_seq (
  resource text PRIMARY KEY,
  last_seq bigint
);

/* =====================================================================
    Lock sequence sharded to reduce contention (Phase 1 optimization path)
    ===================================================================== */
CREATE TABLE IF NOT EXISTS lock_seq_shard (
   resource text,
   shard_id int,
   last_seq bigint,
   PRIMARY KEY ((resource), shard_id)
);

/* =====================================================================
   SHARDED SEQUENCE FOR HOT RESOURCES (resource hashed to shard_id)
   ===================================================================== */
CREATE TABLE IF NOT EXISTS lock_seq_shard (
  resource text,
  shard_id int,
  last_seq bigint,
  PRIMARY KEY ((resource), shard_id)
);

/* =====================================================================
   SHARDED JOB QUEUE (AP LAYER)
   Partition: bucket_id; clustering for priority: scheduled_time then job_id
   status: pending|running|completed|failed|canceled
   attempts: retry counter
   ===================================================================== */
CREATE TABLE IF NOT EXISTS jobs_sharded (
  bucket_id int,
  scheduled_time timestamp,
  job_id uuid,
  payload text,
  status text,
  attempts int,
  PRIMARY KEY ((bucket_id), scheduled_time, job_id)
) WITH default_time_to_live = 0;  /* explicit; jobs cleaned by process */

/* =====================================================================
    PENDING QUEUE VIEW PER BUCKET (to avoid ALLOW FILTERING)
    - Enqueue writes here and jobs_sharded
    - Pollers read from here (<= now) and delete when starting
    ===================================================================== */
CREATE TABLE IF NOT EXISTS jobs_pending_by_bucket (
   bucket_id int,
   scheduled_time timestamp,
   job_id uuid,
   payload text,
   PRIMARY KEY ((bucket_id), scheduled_time, job_id)
);

/* =====================================================================
   MIGRATION NOTES
   ---------------------------------------------------------------------
   If upgrading from previous schema:
   1. ALTER TABLE locks RENAME resource TO resource_id; (only if old column name was 'resource')
      Cassandra does not support RENAME for primary key column in-place; if existing table differs
      you must: CREATE new table locks_new AS SELECT ..., then DROP old & RENAME.
      Since we use IF NOT EXISTS here, manual migration may be required if old structure differs.
   2. Add epoch column manually if existing locks table lacks it:
        ALTER TABLE locks ADD epoch bigint;
   3. Apply compaction settings:
        ALTER TABLE locks WITH compaction = {
          'class': 'TimeWindowCompactionStrategy',
          'compaction_window_unit': 'MINUTES',
          'compaction_window_size': '1'
        } AND gc_grace_seconds = 60;
   4. Populate initial shards for hot resources (optional):
        INSERT INTO lock_seq_shard (resource, shard_id, last_seq) VALUES ('inventory:SKU123', 0, 1);
   5. Begin using jobs_sharded for new scheduling logic; retain jobs for legacy simple jobs until deprecated.
   ===================================================================== */
