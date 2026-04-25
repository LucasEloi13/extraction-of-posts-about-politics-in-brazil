from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256

import boto3

try:
    from ..extractor.models import ExtractedPost
except ImportError:
    from extractor.models import ExtractedPost


@dataclass(frozen=True)
class S3Config:
    bucket: str
    raw_prefix: str = "raw"
    region: str | None = None

    @classmethod
    def from_env(cls) -> S3Config:
        return cls(
            bucket=os.getenv("S3_BUCKET_NAME", "023546157022-posts"),
            raw_prefix=os.getenv("S3_RAW_PREFIX", "raw"),
            region=os.getenv("AWS_REGION"),
        )


class RawPostsS3Persister:
    def __init__(self, config: S3Config | None = None) -> None:
        resolved_config = config or S3Config.from_env()
        self._bucket = resolved_config.bucket
        self._raw_prefix = resolved_config.raw_prefix.strip("/") or "raw"
        self._s3_client = boto3.client("s3", region_name=resolved_config.region)

    def _build_raw_s3_key(self, *, source: str, post: ExtractedPost) -> str:
        partition_date: datetime = post.collected_at
        object_id = post.post_id.strip()
        if not object_id:
            object_id = sha256(post.url.encode("utf-8")).hexdigest()
        safe_object_id = re.sub(r"[^a-zA-Z0-9._-]+", "_", object_id)
        return (
            f"{self._raw_prefix}/"
            f"source={source}/"
            f"year={partition_date.year:04d}/"
            f"month={partition_date.month:02d}/"
            f"day={partition_date.day:02d}/"
            f"{safe_object_id}.json"
        )

    def persist_posts(self, *, source: str, posts: list[ExtractedPost]) -> tuple[int, int]:
        uploaded_count = 0
        duplicates_count = 0
        for post in posts:
            if not post.url:
                duplicates_count += 1
                continue
            s3_key = self._build_raw_s3_key(source=source, post=post)
            body = json.dumps(post.to_dict(), ensure_ascii=False).encode("utf-8")
            self._s3_client.put_object(
                Bucket=self._bucket,
                Key=s3_key,
                Body=body,
                ContentType="application/json",
            )
            uploaded_count += 1
        return uploaded_count, duplicates_count
