from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from json import JSONDecodeError
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from .orchestrator import BaseFetcher, configure_logging, format_post
    from .models import ExtractedPost
except ImportError:
    from orchestrator import BaseFetcher, configure_logging, format_post
    from models import ExtractedPost


LOGGER = logging.getLogger(__name__)


def _safe_int(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


class RedditFetcher(BaseFetcher):
    def __init__(self, sort: str = "new") -> None:
        self.sort = sort

    @property
    def source_name(self) -> str:
        return "reddit"

    def fetch_posts(self, query: str, limit: int = 10) -> list[ExtractedPost]:
        bounded_limit = max(1, min(limit, 100))
        params = {
            "q": query,
            "sort": self.sort,
            "limit": bounded_limit,
            "raw_json": 1,
            "restrict_sr": 0,
            "type": "link",
            "t": "all",
        }
        url = f"https://www.reddit.com/search.json?{urlencode(params)}"
        request = Request(
            url,
            headers={
                "User-Agent": "python:political-posts-script:v1.0 (by /u/eloi13)",
                "Accept": "application/json",
            },
        )
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))

        children = payload.get("data", {}).get("children", [])
        posts: list[ExtractedPost] = []
        for child in children:
            data = child.get("data", {})
            post_id = str(data.get("id", "")).strip()
            if not post_id:
                continue

            created_utc = data.get("created_utc")
            created_at = (
                datetime.fromtimestamp(created_utc, tz=timezone.utc)
                if isinstance(created_utc, (int, float))
                else None
            )
            permalink = str(data.get("permalink", "")).strip()
            full_url = f"https://www.reddit.com{permalink}" if permalink else str(data.get("url", ""))
            title = str(data.get("title", "")).strip()
            body = str(data.get("selftext", "")).strip()

            posts.append(
                ExtractedPost(
                    post_id=post_id,
                    source=self.source_name,
                    author=str(data.get("author", "desconhecido")),
                    author_username=str(data.get("author", "")) or None,
                    created_at=created_at,
                    collected_at=datetime.now(timezone.utc),
                    text=body or title,
                    title=title or None,
                    url=full_url,
                    score=_safe_int(data.get("score")),
                    num_comments=_safe_int(data.get("num_comments")),
                    subreddit=str(data.get("subreddit", "")) or None,
                    nsfw=bool(data["over_18"]) if "over_18" in data else None,
                )
            )
        return posts


def main() -> int:
    parser = argparse.ArgumentParser(description="Extrator Reddit por keyword com schema normalizado.")
    parser.add_argument("keyword", nargs="?", default="lula")
    parser.add_argument("limit", nargs="?", type=int, default=10)
    parser.add_argument("sort", nargs="?", default="new")
    args = parser.parse_args()

    configure_logging()
    fetcher = RedditFetcher(sort=args.sort)

    try:
        posts = fetcher.fetch_posts(query=args.keyword, limit=args.limit)
    except (HTTPError, URLError, JSONDecodeError, ValueError) as exc:
        LOGGER.error("Falha ao consultar Reddit: %s", exc)
        return 1

    if not posts:
        LOGGER.info("Nenhum post encontrado para '%s' (%s).", args.keyword, args.sort)
        return 0

    LOGGER.info("Encontrados %d posts para '%s' (%s).", len(posts), args.keyword, args.sort)
    for idx, post in enumerate(posts, start=1):
        LOGGER.info("[%d]\n%s\n", idx, format_post(post))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
