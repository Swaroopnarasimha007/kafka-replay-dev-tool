CREATE TABLE replay_mapping_heads (
    id varchar(36) PRIMARY KEY, current_version integer NOT NULL
);
CREATE TABLE replay_mappings (
    id varchar(36) NOT NULL REFERENCES replay_mapping_heads(id), version integer NOT NULL,
    definition text NOT NULL, PRIMARY KEY(id, version)
);
CREATE TABLE replay_previews (
    id varchar(36) PRIMARY KEY, owner_name text NOT NULL,
    mapping_id varchar(36) NOT NULL, mapping_version integer NOT NULL,
    document text NOT NULL,
    FOREIGN KEY(mapping_id, mapping_version) REFERENCES replay_mappings(id, version)
);
CREATE TABLE replay_jobs (
    id varchar(36) PRIMARY KEY, preview_id varchar(36) NOT NULL REFERENCES replay_previews(id),
    owner_name text NOT NULL, idempotency_key varchar(200) NOT NULL UNIQUE,
    request_hash varchar(64) NOT NULL, state varchar(40) NOT NULL,
    document text NOT NULL, approved_at timestamp with time zone NOT NULL DEFAULT now()
);
CREATE TABLE replay_events (
    id bigserial PRIMARY KEY, job_id varchar(36) NOT NULL REFERENCES replay_jobs(id),
    event_type varchar(40) NOT NULL, actor text NOT NULL,
    at timestamp with time zone NOT NULL DEFAULT now(),
    UNIQUE(job_id, event_type)
);
