from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import sys
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from urllib.error import HTTPError, URLError
from abc import ABC, abstractmethod
from typing import Callable, Iterable

from dotenv import load_dotenv
import yaml

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


@dataclass(frozen=True)
class ExtractionSettings:
    keywords_path: Path
    cookies_path: Path
    max_workers: int


@dataclass(frozen=True)
class ExtractionTaskResult:
    source: str
    keyword: str
    fetched_count: int
    inserted_count: int
    duplicate_count: int
    error_message: str | None = None


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


def load_settings(path: Path) -> ExtractionSettings:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {path}")

    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Formato inválido no arquivo de configuração: {path}")

    max_workers_raw = payload.get("max_workers", 4)
    try:
        max_workers = int(max_workers_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("max_workers deve ser um número inteiro.") from exc
    if max_workers < 1:
        raise ValueError("max_workers deve ser maior ou igual a 1.")

    keywords_value = payload.get("keywords_path", "config/keywords.txt")
    cookies_value = payload.get("cookies_path", "cookies.json")
    return ExtractionSettings(
        keywords_path=Path(str(keywords_value)),
        cookies_path=Path(str(cookies_value)),
        max_workers=max_workers,
    )


def run_extraction(*, keywords_path: Path, limit: int, cookies_path: Path, max_workers: int) -> int:
    logger = logging.getLogger(__name__)
    keywords = load_keywords(keywords_path)
    load_dotenv(".env")

    control_plane = PostgresExtractionControlPlane(PostgresConfig.from_env())
    control_plane.setup()

    try:
        from .reddit import RedditFetcher
        from .blusky import BlueskyFetcher
        from .x import XFetcher
    except ImportError:
        from reddit import RedditFetcher
        from blusky import BlueskyFetcher
        from x import XFetcher

    fetcher_factories: dict[str, Callable[[], BaseFetcher]] = {
        "reddit": lambda: RedditFetcher(sort="new"),
        "bluesky": BlueskyFetcher,
        "x": lambda: XFetcher(cookies_file=str(cookies_path)),
    }

    job_id = control_plane.start_job(total_keywords=len(keywords), per_keyword_limit=limit)
    logger.info("Job de extração iniciado: %s", job_id)

    total_fetched = 0
    total_inserted = 0
    total_duplicates = 0
    had_errors = False

    def extract_task(source: str, keyword: str) -> ExtractionTaskResult:
        worker_control_plane = PostgresExtractionControlPlane(PostgresConfig.from_env())
        worker_persister = RawPostsS3Persister()
        fetcher = fetcher_factories[source]()
        extraction_name = worker_control_plane.start_task(
            job_id=job_id,
            source=source,
            keyword=keyword,
        )
        fetched_count = 0
        inserted_count = 0
        duplicate_count = 0
        try:
            posts = fetcher.fetch_posts(query=keyword, limit=limit)
            fetched_count = len(posts)
            inserted_count, duplicate_count = worker_persister.persist_posts(
                source=source,
                extraction_name=extraction_name,
                posts=posts,
            )
            worker_control_plane.finish_task(
                job_id=job_id,
                source=source,
                keyword=keyword,
                status="completed",
                fetched_count=fetched_count,
                inserted_count=inserted_count,
                duplicate_count=duplicate_count,
            )
            return ExtractionTaskResult(
                source=source,
                keyword=keyword,
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
            error_message = str(exc)
            worker_control_plane.finish_task(
                job_id=job_id,
                source=source,
                keyword=keyword,
                status="failed",
                fetched_count=fetched_count,
                inserted_count=inserted_count,
                duplicate_count=duplicate_count,
                error_message=error_message,
            )
            logger.error(
                "Falha na extração | source=%s keyword=%s erro=%s",
                source,
                keyword,
                error_message,
            )
            return ExtractionTaskResult(
                source=source,
                keyword=keyword,
                fetched_count=fetched_count,
                inserted_count=inserted_count,
                duplicate_count=duplicate_count,
                error_message=error_message,
            )
        finally:
            worker_control_plane.close()

    try:
        tasks: list[tuple[str, str]] = [
            (source, keyword) for source in fetcher_factories for keyword in keywords
        ]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(extract_task, source, keyword): (source, keyword)
                for source, keyword in tasks
            }
            for future in as_completed(future_to_task):
                source, keyword = future_to_task[future]
                try:
                    result = future.result()
                except Exception as exc:
                    had_errors = True
                    logger.error(
                        "Erro inesperado no worker | source=%s keyword=%s erro=%s",
                        source,
                        keyword,
                        exc,
                    )
                    continue

                if result.error_message:
                    had_errors = True
                    continue

                total_fetched += result.fetched_count
                total_inserted += result.inserted_count
                total_duplicates += result.duplicate_count
                logger.info(
                    "Extração concluída | source=%s keyword=%s fetched=%d inserted=%d duplicates=%d",
                    result.source,
                    result.keyword,
                    result.fetched_count,
                    result.inserted_count,
                    result.duplicate_count,
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
    parser.add_argument("--config", default="config/extraction.yml")
    parser.add_argument("--keywords", default=None)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--cookies", default=None)
    parser.add_argument("--workers", type=int, default=None)
    args = parser.parse_args()

    configure_logging()
    settings = load_settings(Path(args.config))

    resolved_keywords_path = Path(args.keywords) if args.keywords else settings.keywords_path
    resolved_cookies_path = Path(args.cookies) if args.cookies else settings.cookies_path
    resolved_workers = args.workers if args.workers is not None else settings.max_workers
    return run_extraction(
        keywords_path=resolved_keywords_path,
        limit=max(1, min(args.limit, 100)),
        cookies_path=resolved_cookies_path,
        max_workers=max(1, resolved_workers),
    )


if __name__ == "__main__":
    raise SystemExit(main())
