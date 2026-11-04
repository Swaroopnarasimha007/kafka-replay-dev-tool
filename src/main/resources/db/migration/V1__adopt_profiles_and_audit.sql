CREATE TABLE IF NOT EXISTS cluster_profiles (
    id varchar(36) PRIMARY KEY, name text NOT NULL, bootstrap text NOT NULL,
    cluster_id text NOT NULL, secret text NOT NULL
);
CREATE TABLE IF NOT EXISTS browse_audit (
    id bigserial PRIMARY KEY, at timestamp with time zone DEFAULT now(),
    actor text, action text, coordinates text
);
