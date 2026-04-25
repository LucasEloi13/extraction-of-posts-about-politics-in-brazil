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


API_URL = "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"
LOGGER = logging.getLogger(__name__)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _safe_int(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _uri_to_url(uri: str) -> str:
    # Ex.: at://did:plc:abc/app.bsky.feed.post/3lnxyz
    if not uri.startswith("at://"):
        return uri
    parts = uri.split("/")
    if len(parts) < 5:
        return uri
    return f"https://bsky.app/profile/{parts[2]}/post/{parts[4]}"


class BlueskyFetcher(BaseFetcher):
    @property
    def source_name(self) -> str:
        return "bluesky"

    def fetch_posts(self, query: str, limit: int = 10) -> list[ExtractedPost]:
        params = {"q": query, "limit": max(1, min(limit, 100)), "lang": "pt"}
        url = f"{API_URL}?{urlencode(params)}"
        request = Request(url, headers={"User-Agent": "political-posts-script/1.0"})
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))

        raw_posts = payload.get("posts", [])
        posts: list[ExtractedPost] = []
        for item in raw_posts:
            uri = str(item.get("uri", "")).strip()
            if not uri:
                continue

            record = item.get("record", {})
            author = item.get("author", {})
            created_at = _parse_datetime(record.get("createdAt") or item.get("indexedAt"))
            author_handle = str(author.get("handle", "")).strip()
            author_name = str(author.get("displayName", "")).strip() or author_handle or "desconhecido"

            reply_count = _safe_int(item.get("replyCount"))
            repost_count = _safe_int(item.get("repostCount"))

            posts.append(
                ExtractedPost(
                    post_id=uri,
                    source=self.source_name,
                    author=author_name,
                    author_username=author_handle or None,
                    created_at=created_at,
                    collected_at=datetime.now(timezone.utc),
                    text=str(record.get("text", "")).strip(),
                    url=_uri_to_url(uri),
                    num_comments=reply_count,
                    num_replies=reply_count,
                    num_reposts=repost_count,
                    num_likes=_safe_int(item.get("likeCount")),
                    content_id=str(item.get("cid", "")).strip() or None,
                    raw_uri=uri,
                )
            )
        return posts


def main() -> int:
    parser = argparse.ArgumentParser(description="Extrator Bluesky com schema normalizado.")
    parser.add_argument("keyword", nargs="?", default="lula")
    parser.add_argument("limit", nargs="?", type=int, default=10)
    args = parser.parse_args()

    configure_logging()
    fetcher = BlueskyFetcher()
    try:
        posts = fetcher.fetch_posts(query=args.keyword, limit=args.limit)
    except (HTTPError, URLError, JSONDecodeError, ValueError) as exc:
        LOGGER.error("Erro ao consultar a API do Bluesky: %s", exc)
        return 1

    if not posts:
        LOGGER.info("Nenhum post encontrado para a palavra-chave '%s'.", args.keyword)
        return 0

    LOGGER.info("Encontrados %d posts para '%s'.", len(posts), args.keyword)
    for idx, post in enumerate(posts, start=1):
        LOGGER.info("[%d]\n%s\n", idx, format_post(post))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
