CREATE TABLE replay_execution (
    job_id varchar(36) PRIMARY KEY REFERENCES replay_jobs(id),
    requested boolean NOT NULL DEFAULT false, actor_document text NOT NULL,
    authorized_until bigint NOT NULL, lease_owner varchar(36), lease_until bigint NOT NULL DEFAULT 0,
    pause_requested boolean NOT NULL DEFAULT false, released boolean NOT NULL DEFAULT false,
    anchor text, next_position integer NOT NULL DEFAULT 0,
    published integer NOT NULL DEFAULT 0, skipped integer NOT NULL DEFAULT 0,
    batch_count integer NOT NULL DEFAULT 0, next_eligible bigint NOT NULL DEFAULT 0,
    status_detail text, first_published_at bigint, last_receipt_at bigint,
    recovery_boundary bigint, started_at bigint NOT NULL, resume_state varchar(40)
);
CREATE TABLE replay_receipt_projection (
    job_id varchar(36) NOT NULL REFERENCES replay_jobs(id), batch_index integer NOT NULL,
    document text NOT NULL, PRIMARY KEY(job_id,batch_index)
);
-- Synthetic authority is separate from the projection, just as Kafka is in live mode.
CREATE TABLE demo_outputs (
    id bigserial PRIMARY KEY, job_id varchar(36) NOT NULL, manifest_position integer NOT NULL,
    topic text NOT NULL, partition_id integer NOT NULL, document text NOT NULL,
    UNIQUE(job_id,manifest_position)
);
CREATE TABLE demo_receipts (
    id bigserial PRIMARY KEY, job_id varchar(36) NOT NULL, batch_index integer NOT NULL,
    document text NOT NULL, UNIQUE(job_id,batch_index)
);
