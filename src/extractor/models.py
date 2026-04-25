from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class ExtractedPost:
    post_id: str
    source: str
    author: str
    created_at: datetime | None
    collected_at: datetime
    text: str
    url: str
    title: str | None = None
    author_username: str | None = None
    score: int | None = None
    num_comments: int | None = None
    num_likes: int | None = None
    num_reposts: int | None = None
    num_replies: int | None = None
    subreddit: str | None = None
    content_id: str | None = None
    nsfw: bool | None = None
    raw_uri: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "post_id": self.post_id,
            "source": self.source,
            "author": self.author,
            "author_username": self.author_username,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "collected_at": self.collected_at.isoformat(),
            "text": self.text,
            "title": self.title,
            "url": self.url,
            "score": self.score,
            "num_comments": self.num_comments,
            "num_likes": self.num_likes,
            "num_reposts": self.num_reposts,
            "num_replies": self.num_replies,
            "subreddit": self.subreddit,
            "content_id": self.content_id,
            "nsfw": self.nsfw,
            "raw_uri": self.raw_uri,
        }
