-- Job Dependencies (Section 14.1 Stretch Goal)
-- Jobs with unmet dependencies start as 'blocked' and become 'pending' when all dependencies succeed.

CREATE TABLE IF NOT EXISTS job_dependencies (
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    depends_on_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, depends_on_id)
);

CREATE INDEX IF NOT EXISTS job_deps_depends_idx ON job_dependencies (depends_on_id);

