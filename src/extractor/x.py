from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

from playwright.async_api import (
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

try:
    from .orchestrator import BaseFetcher, configure_logging, format_post
    from .models import ExtractedPost
except ImportError:
    from orchestrator import BaseFetcher, configure_logging, format_post
    from models import ExtractedPost


LOGGER = logging.getLogger(__name__)
DEFAULT_MAX_POSTS = 15
DEFAULT_OUTPUT_FILE = "posts_politica.json"
DEFAULT_COOKIES_FILE = "cookies.json"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_count(value: str) -> int:
    normalized = value.strip().lower().replace(" ", "").replace(",", ".")
    if not normalized:
        return 0

    multiplier = 1
    if normalized.endswith("mil"):
        multiplier = 1_000
        normalized = normalized[:-3]
    elif normalized.endswith("k"):
        multiplier = 1_000
        normalized = normalized[:-1]
    elif normalized.endswith("mi"):
        multiplier = 1_000_000
        normalized = normalized[:-2]
    elif normalized.endswith("m"):
        multiplier = 1_000_000
        normalized = normalized[:-1]

    try:
        return int(float(normalized) * multiplier)
    except ValueError:
        digits = re.sub(r"[^\d]", "", normalized)
        return int(digits) if digits else 0


def _extract_post_id(link: str, username: str, text: str) -> str:
    match = re.search(r"/status/(\d+)", link)
    if match:
        return match.group(1)
    fallback = _clean_text(text)[:30] or "sem-texto"
    return f"{username or 'desconhecido'}-{fallback}"


def _load_cookies(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if isinstance(data, list):
        raw_cookies = data
    elif isinstance(data, dict) and "cookies" in data and isinstance(data["cookies"], list):
        raw_cookies = data["cookies"]
    else:
        raise ValueError(
            "Formato de cookies não reconhecido. "
            "Esperado: lista de cookies ou objeto com chave 'cookies'."
        )

    normalized_cookies: list[dict] = []
    for cookie_data in raw_cookies:
        domain = str(cookie_data.get("domain", cookie_data.get("Domain", ".x.com")))
        if domain and not domain.startswith("."):
            domain = f".{domain}"

        same_site = str(cookie_data.get("sameSite", cookie_data.get("SameSite", "None"))).capitalize()
        if same_site not in ("Strict", "Lax", "None"):
            same_site = "None"

        normalized_cookie = {
            "name": str(cookie_data.get("name", cookie_data.get("Name", ""))),
            "value": str(cookie_data.get("value", cookie_data.get("Value", ""))),
            "domain": domain,
            "path": str(cookie_data.get("path", cookie_data.get("Path", "/"))),
            "sameSite": same_site,
            "secure": bool(cookie_data.get("secure", cookie_data.get("Secure", False))),
            "httpOnly": bool(cookie_data.get("httpOnly", cookie_data.get("HttpOnly", False))),
        }
        expires = cookie_data.get("expirationDate", cookie_data.get("expires", cookie_data.get("Expires")))
        if expires is not None:
            normalized_cookie["expires"] = int(float(expires))

        normalized_cookies.append(normalized_cookie)

    return normalized_cookies


class XFetcher(BaseFetcher):
    def __init__(self, cookies_file: str = DEFAULT_COOKIES_FILE) -> None:
        self.cookies_path = Path(cookies_file)

    @property
    def source_name(self) -> str:
        return "x"

    def fetch_posts(self, query: str, limit: int = DEFAULT_MAX_POSTS) -> list[ExtractedPost]:
        return asyncio.run(self._fetch_posts_async(query=query, limit=limit))

    async def _fetch_posts_async(self, query: str, limit: int) -> list[ExtractedPost]:
        search_url = f"https://x.com/search?q={quote_plus(query)}&src=typed_query&f=live"
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            try:
                context = await browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1280, "height": 900},
                    locale="pt-BR",
                    timezone_id="America/Sao_Paulo",
                )
                await context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )

                if self.cookies_path.exists():
                    cookies = _load_cookies(self.cookies_path)
                    await context.add_cookies(cookies)
                    LOGGER.info("Cookies carregados: %d", len(cookies))
                else:
                    LOGGER.warning("Arquivo de cookies não encontrado: %s", self.cookies_path)

                page = await context.new_page()
                await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_selector('article[data-testid="tweet"]', timeout=20_000)
                return await self._scroll_and_collect(page=page, limit=limit)
            finally:
                await browser.close()

    async def _scroll_and_collect(self, page, limit: int) -> list[ExtractedPost]:
        collected_posts: list[ExtractedPost] = []
        seen_ids: set[str] = set()
        attempts_without_new = 0
        max_attempts_without_new = 5

        while len(collected_posts) < limit and attempts_without_new < max_attempts_without_new:
            articles = await page.query_selector_all('article[data-testid="tweet"]')
            found_in_round = 0

            for article in articles:
                try:
                    post = await self._extract_post(article)
                except PlaywrightError as exc:
                    LOGGER.warning("Falha ao processar tweet: %s", exc)
                    continue

                if not post or post.post_id in seen_ids:
                    continue

                seen_ids.add(post.post_id)
                collected_posts.append(post)
                found_in_round += 1
                if len(collected_posts) >= limit:
                    break

            attempts_without_new = attempts_without_new + 1 if found_in_round == 0 else 0
            await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
            await asyncio.sleep(2.5)

        return collected_posts

    async def _extract_post(self, article) -> ExtractedPost | None:
        text_element = await article.query_selector('[data-testid="tweetText"]')
        text = _clean_text(await text_element.inner_text()) if text_element else ""
        if not text:
            return None

        user_element = await article.query_selector('[data-testid="User-Name"]')
        user_raw = await user_element.inner_text() if user_element else ""
        user_lines = user_raw.split("\n")
        author_name = user_lines[0].strip() if user_lines else "desconhecido"
        author_username = user_lines[1].strip().lstrip("@") if len(user_lines) > 1 else None

        time_element = await article.query_selector("time")
        created_raw = await time_element.get_attribute("datetime") if time_element else None
        created_at = _parse_datetime(created_raw)

        link_element = await article.query_selector('a[href*="/status/"]')
        link = await link_element.get_attribute("href") if link_element else ""
        if link and not link.startswith("http"):
            link = f"https://x.com{link}"

        metrics: dict[str, int] = {"reply": 0, "retweet": 0, "like": 0}
        for metric in metrics:
            metric_root = await article.query_selector(f'[data-testid="{metric}"]')
            if not metric_root:
                continue
            metric_label = await metric_root.query_selector("span[data-testid]")
            metric_text = await metric_label.inner_text() if metric_label else "0"
            metrics[metric] = _parse_count(metric_text)

        return ExtractedPost(
            post_id=_extract_post_id(link=link, username=author_username or "", text=text),
            source=self.source_name,
            author=author_name or "desconhecido",
            author_username=author_username,
            created_at=created_at,
            collected_at=datetime.now(timezone.utc),
            text=text,
            url=link,
            num_comments=metrics["reply"],
            num_replies=metrics["reply"],
            num_reposts=metrics["retweet"],
            num_likes=metrics["like"],
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Extrator X com schema normalizado.")
    parser.add_argument("keyword", nargs="?", default="lula")
    parser.add_argument("limit", nargs="?", type=int, default=DEFAULT_MAX_POSTS)
    parser.add_argument("--cookies", default=DEFAULT_COOKIES_FILE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE)
    args = parser.parse_args()

    configure_logging()
    fetcher = XFetcher(cookies_file=args.cookies)
    try:
        posts = fetcher.fetch_posts(query=args.keyword, limit=args.limit)
    except (PlaywrightTimeoutError, PlaywrightError, ValueError, OSError) as exc:
        LOGGER.error("Erro ao coletar posts do X: %s", exc)
        return 1

    output_path = Path(args.output)
    payload = [post.to_dict() for post in posts]
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    LOGGER.info("%d posts coletados e salvos em '%s'.", len(posts), output_path)
    for idx, post in enumerate(posts[:5], start=1):
        LOGGER.info("[%d]\n%s\n", idx, format_post(post))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
