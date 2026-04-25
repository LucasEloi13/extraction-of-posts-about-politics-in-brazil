from __future__ import annotations

import os
from dataclasses import dataclass
from uuid import UUID, uuid4

from psycopg import Connection, connect


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> PostgresConfig:
        return cls(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            database=os.environ["POSTGRES_DB"],
        )

class PostgresExtractionControlPlane:
    def __init__(self, config: PostgresConfig) -> None:
        self._connection: Connection = connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            dbname=config.database,
            autocommit=False,
        )

    def close(self) -> None:
        self._connection.close()

    def setup(self) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
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
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS extraction_job_tasks (
                    job_id UUID NOT NULL REFERENCES extraction_jobs(job_id) ON DELETE CASCADE,
                    extraction_name TEXT NOT NULL,
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
                """
            )
            cursor.execute(
                """
                ALTER TABLE extraction_job_tasks
                ADD COLUMN IF NOT EXISTS extraction_name TEXT;
                """
            )
        self._connection.commit()

    def start_job(self, *, total_keywords: int, per_keyword_limit: int) -> UUID:
        job_id = uuid4()
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO extraction_jobs (job_id, status, total_keywords, per_keyword_limit)
                VALUES (%s, 'running', %s, %s);
                """,
                (job_id, total_keywords, per_keyword_limit),
            )
        self._connection.commit()
        return job_id

    def start_task(self, *, job_id: UUID, source: str, keyword: str) -> str:
        extraction_name = str(uuid4())
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO extraction_job_tasks (job_id, extraction_name, source, keyword, status)
                VALUES (%s, %s, %s, %s, 'running')
                ON CONFLICT (job_id, source, keyword)
                DO UPDATE SET
                    extraction_name = EXCLUDED.extraction_name,
                    status = EXCLUDED.status,
                    started_at = NOW(),
                    finished_at = NULL,
                    error_message = NULL,
                    fetched_count = 0,
                    inserted_count = 0,
                    duplicate_count = 0;
                """,
                (job_id, extraction_name, source, keyword),
            )
        self._connection.commit()
        return extraction_name

    def finish_task(
        self,
        *,
        job_id: UUID,
        source: str,
        keyword: str,
        status: str,
        fetched_count: int,
        inserted_count: int,
        duplicate_count: int,
        error_message: str | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE extraction_job_tasks
                SET
                    status = %s,
                    fetched_count = %s,
                    inserted_count = %s,
                    duplicate_count = %s,
                    finished_at = NOW(),
                    error_message = %s
                WHERE job_id = %s AND source = %s AND keyword = %s;
                """,
                (
                    status,
                    fetched_count,
                    inserted_count,
                    duplicate_count,
                    error_message,
                    job_id,
                    source,
                    keyword,
                ),
            )
        self._connection.commit()

    def finish_job(
        self,
        *,
        job_id: UUID,
        status: str,
        total_fetched: int,
        total_inserted: int,
        total_duplicates: int,
        error_message: str | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE extraction_jobs
                SET
                    status = %s,
                    total_fetched = %s,
                    total_inserted = %s,
                    total_duplicates = %s,
                    finished_at = NOW(),
                    error_message = %s
                WHERE job_id = %s;
                """,
                (
                    status,
                    total_fetched,
                    total_inserted,
                    total_duplicates,
                    error_message,
                    job_id,
                ),
            )
        self._connection.commit()
