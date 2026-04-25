from __future__ import annotations

import argparse
import logging
import sys
from json import JSONDecodeError
from pathlib import Path
from urllib.error import HTTPError, URLError
from abc import ABC, abstractmethod
from typing import Iterable

from dotenv import load_dotenv

try:
    from .models import ExtractedPost
    from ..persist.postgres_control_plane import (
        PostgresExtractionControlPlane,
        PostgresConfig,
    )
    from ..persist.s3_raw_posts import RawPostsS3Persister
except ImportError:
    current_dir = Path(__file__).resolve().parent
    src_dir = current_dir.parent
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    from models import ExtractedPost
    from persist.postgres_control_plane import (
        PostgresExtractionControlPlane,
        PostgresConfig,
    )
    from persist.s3_raw_posts import RawPostsS3Persister

from playwright.async_api import Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


class BaseFetcher(ABC):
    @property
    @abstractmethod
    def source_name(self) -> str:
        ...

    @abstractmethod
    def fetch_posts(self, query: str, limit: int = 10) -> list[ExtractedPost]:
        ...


def format_post(post: ExtractedPost) -> str:
    lines: list[str] = [
        f"fonte={post.source}",
        f"post_id={post.post_id}",
        f"autor={post.author}",
        f"usuario={post.author_username or 'desconhecido'}",
        f"data={post.created_at.isoformat() if post.created_at else 'desconhecida'}",
        f"texto={post.text}",
        f"url={post.url}",
    ]

    optional_fields: Iterable[tuple[str, object | None]] = (
        ("titulo", post.title),
        ("subreddit", post.subreddit),
        ("score", post.score),
        ("comentarios", post.num_comments),
        ("respostas", post.num_replies),
        ("retweets", post.num_reposts),
        ("curtidas", post.num_likes),
        ("cid", post.content_id),
        ("uri", post.raw_uri),
        ("nsfw", post.nsfw),
    )
    lines.extend(f"{name}={value}" for name, value in optional_fields if value is not None)
    lines.append(f"coletado_em={post.collected_at.isoformat()}")
    return "\n".join(lines)


def load_keywords(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de keywords não encontrado: {path}")

    keywords = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not keywords:
        raise ValueError(f"Arquivo de keywords sem conteúdo: {path}")
    return keywords


def run_extraction(*, keywords_path: Path, limit: int, cookies_path: Path) -> int:
    logger = logging.getLogger(__name__)
    keywords = load_keywords(keywords_path)
    load_dotenv(".env")

    control_plane = PostgresExtractionControlPlane(PostgresConfig.from_env())
    s3_persister = RawPostsS3Persister()
    control_plane.setup()

    try:
        from .reddit import RedditFetcher
        from .blusky import BlueskyFetcher
        from .x import XFetcher
    except ImportError:
        from reddit import RedditFetcher
        from blusky import BlueskyFetcher
        from x import XFetcher

    fetchers: list[BaseFetcher] = [
        RedditFetcher(sort="new"),
        BlueskyFetcher(),
        XFetcher(cookies_file=str(cookies_path)),
    ]

    job_id = control_plane.start_job(total_keywords=len(keywords), per_keyword_limit=limit)
    logger.info("Job de extração iniciado: %s", job_id)

    total_fetched = 0
    total_inserted = 0
    total_duplicates = 0
    had_errors = False

    try:
        for fetcher in fetchers:
            for keyword in keywords:
                extraction_name = control_plane.start_task(
                    job_id=job_id,
                    source=fetcher.source_name,
                    keyword=keyword,
                )
                fetched_count = 0
                inserted_count = 0
                duplicate_count = 0
                try:
                    posts = fetcher.fetch_posts(query=keyword, limit=limit)
                    fetched_count = len(posts)
                    inserted_count, duplicate_count = s3_persister.persist_posts(
                        source=fetcher.source_name,
                        extraction_name=extraction_name,
                        posts=posts,
                    )
                    control_plane.finish_task(
                        job_id=job_id,
                        source=fetcher.source_name,
                        keyword=keyword,
                        status="completed",
                        fetched_count=fetched_count,
                        inserted_count=inserted_count,
                        duplicate_count=duplicate_count,
                    )
                except (
                    HTTPError,
                    URLError,
                    JSONDecodeError,
                    ValueError,
                    OSError,
                    PlaywrightError,
                    PlaywrightTimeoutError,
                ) as exc:
                    had_errors = True
                    control_plane.finish_task(
                        job_id=job_id,
                        source=fetcher.source_name,
                        keyword=keyword,
                        status="failed",
                        fetched_count=fetched_count,
                        inserted_count=inserted_count,
                        duplicate_count=duplicate_count,
                        error_message=str(exc),
                    )
                    logger.error(
                        "Falha na extração | source=%s keyword=%s erro=%s",
                        fetcher.source_name,
                        keyword,
                        exc,
                    )
                    continue

                total_fetched += fetched_count
                total_inserted += inserted_count
                total_duplicates += duplicate_count
                logger.info(
                    "Extração concluída | source=%s keyword=%s fetched=%d inserted=%d duplicates=%d",
                    fetcher.source_name,
                    keyword,
                    fetched_count,
                    inserted_count,
                    duplicate_count,
                )
    finally:
        final_status = "failed" if had_errors else "completed"
        control_plane.finish_job(
            job_id=job_id,
            status=final_status,
            total_fetched=total_fetched,
            total_inserted=total_inserted,
            total_duplicates=total_duplicates,
            error_message="Uma ou mais tasks falharam." if had_errors else None,
        )
        control_plane.close()

    logger.info(
        "Job finalizado | status=%s fetched=%d inserted=%d duplicates=%d",
        "failed" if had_errors else "completed",
        total_fetched,
        total_inserted,
        total_duplicates,
    )
    return 1 if had_errors else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Orquestrador da extração em múltiplas redes.")
    parser.add_argument("--keywords", default="keywords.txt")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--cookies", default="cookies.json")
    args = parser.parse_args()

    configure_logging()
    return run_extraction(
        keywords_path=Path(args.keywords),
        limit=max(1, min(args.limit, 100)),
        cookies_path=Path(args.cookies),
    )


if __name__ == "__main__":
    raise SystemExit(main())
