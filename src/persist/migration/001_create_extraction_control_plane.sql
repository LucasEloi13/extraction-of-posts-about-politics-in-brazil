CREATE TABLE IF NOT EXISTS extraction_jobs (
    job_id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    total_keywords INTEGER NOT NULL,
    per_keyword_limit INTEGER NOT NULL,
    total_fetched INTEGER NOT NULL DEFAULT 0,
    total_inserted INTEGER NOT NULL DEFAULT 0,
    total_duplicates INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS extraction_job_tasks (
    job_id UUID NOT NULL REFERENCES extraction_jobs(job_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    keyword TEXT NOT NULL,
    status TEXT NOT NULL,
    fetched_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    PRIMARY KEY (job_id, source, keyword)
);
