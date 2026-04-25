from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone

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

    def _build_raw_s3_key(
        self,
        *,
        source: str,
        partition_date: datetime,
        extraction_name: str,
    ) -> str:
        safe_extraction_name = re.sub(r"[^a-zA-Z0-9._-]+", "_", extraction_name.strip())
        return (
            f"{self._raw_prefix}/"
            f"source={source}/"
            f"year={partition_date.year:04d}/"
            f"month={partition_date.month:02d}/"
            f"day={partition_date.day:02d}/"
            f"{safe_extraction_name}.jsonl"
        )

    def persist_posts(
        self,
        *,
        source: str,
        extraction_name: str,
        posts: list[ExtractedPost],
    ) -> tuple[int, int]:
        if posts:
            partition_date = posts[0].collected_at
        else:
            partition_date = datetime.now(timezone.utc)
        s3_key = self._build_raw_s3_key(
            source=source,
            partition_date=partition_date,
            extraction_name=extraction_name,
        )
        lines = [json.dumps(post.to_dict(), ensure_ascii=False) for post in posts]
        body = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
        self._s3_client.put_object(
            Bucket=self._bucket,
            Key=s3_key,
            Body=body,
            ContentType="application/jsonl",
        )
        return len(posts), 0
