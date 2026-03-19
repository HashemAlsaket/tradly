from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone


REQUIRED_NEWS_BUCKETS = (
    "core_semis",
    "us_macro",
    "sector_context",
    "event_reserve",
)
OPTIONAL_NEWS_BUCKETS = (
    "healthcare_core",
    "asia_semis",
    "asia_macro",
)
ALL_NEWS_BUCKETS = REQUIRED_NEWS_BUCKETS + OPTIONAL_NEWS_BUCKETS
NEWS_WATERMARK_SOURCE = "news_events_marketaux_bucket"


@dataclass
class NewsBucketHealth:
    bucket: str
    required: bool
    status: str
    last_attempt_status: str | None
    last_attempt_utc: str | None
    last_success_utc: str | None
    watermark_utc: str | None
    new_events_upserted_last_attempt: int
    detail: str


def _age_seconds(value: datetime | None, now_utc: datetime) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return max(0, int((now_utc - value).total_seconds()))


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat()


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        (table_name,),
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def load_news_bucket_health(conn, *, request_date_local: date, now_utc: datetime, max_age_sec: int) -> list[NewsBucketHealth]:
    watermarks: dict[str, datetime] = {}
    if _table_exists(conn, "pipeline_watermarks"):
        watermark_rows = conn.execute(
            """
            SELECT scope_key, watermark_ts_utc
            FROM pipeline_watermarks
            WHERE source_name = ?
              AND scope_key IN ({placeholders})
            """.format(placeholders=",".join("?" for _ in ALL_NEWS_BUCKETS)),
            [NEWS_WATERMARK_SOURCE, *ALL_NEWS_BUCKETS],
        ).fetchall()
        watermarks = {str(bucket): ts for bucket, ts in watermark_rows if ts is not None}

    pull_rows = []
    if _table_exists(conn, "news_pull_usage"):
        pull_rows = conn.execute(
            """
            SELECT bucket, response_status, new_events_upserted, created_at_utc
            FROM news_pull_usage
            WHERE request_date_utc = ?
              AND bucket IN ({placeholders})
            ORDER BY bucket, created_at_utc DESC
            """.format(placeholders=",".join("?" for _ in ALL_NEWS_BUCKETS)),
            [request_date_local, *ALL_NEWS_BUCKETS],
        ).fetchall()

    latest_attempt: dict[str, tuple[str, int, datetime]] = {}
    latest_success: dict[str, datetime] = {}
    for bucket, response_status, new_events_upserted, created_at_utc in pull_rows:
        bucket_name = str(bucket)
        if bucket_name not in latest_attempt:
            latest_attempt[bucket_name] = (str(response_status), int(new_events_upserted or 0), created_at_utc)
        if str(response_status) == "success" and bucket_name not in latest_success:
            latest_success[bucket_name] = created_at_utc

    rows: list[NewsBucketHealth] = []
    for bucket in ALL_NEWS_BUCKETS:
        required = bucket in REQUIRED_NEWS_BUCKETS
        attempt = latest_attempt.get(bucket)
        success_ts = latest_success.get(bucket)
        watermark_ts = watermarks.get(bucket)
        last_attempt_status = attempt[0] if attempt else None
        last_attempt_ts = attempt[2] if attempt else None
        last_new_events = attempt[1] if attempt else 0
        success_age_sec = _age_seconds(success_ts, now_utc)
        watermark_age_sec = _age_seconds(watermark_ts, now_utc)

        if success_age_sec is not None and success_age_sec <= max_age_sec:
            status = "fresh"
            detail = (
                f"last_success_age_sec={success_age_sec} max_age_sec={max_age_sec} "
                f"last_attempt_status={last_attempt_status} "
                f"new_events_last_attempt={last_new_events} watermark_age_sec={watermark_age_sec}"
            )
        else:
            status = "stale" if required else "warning"
            if last_attempt_status in {"http_error", "limit_reached"}:
                detail = (
                    f"last_attempt_status={last_attempt_status} "
                    f"last_success_age_sec={success_age_sec} max_age_sec={max_age_sec} "
                    f"watermark_age_sec={watermark_age_sec}"
                )
            elif success_ts is None:
                detail = (
                    f"no_success_for_local_date max_age_sec={max_age_sec} "
                    f"last_attempt_status={last_attempt_status} watermark_age_sec={watermark_age_sec}"
                )
            else:
                detail = (
                    f"last_success_age_sec={success_age_sec} max_age_sec={max_age_sec} "
                    f"last_attempt_status={last_attempt_status} watermark_age_sec={watermark_age_sec}"
                )

        rows.append(
            NewsBucketHealth(
                bucket=bucket,
                required=required,
                status=status,
                last_attempt_status=last_attempt_status,
                last_attempt_utc=_iso_or_none(last_attempt_ts),
                last_success_utc=_iso_or_none(success_ts),
                watermark_utc=_iso_or_none(watermark_ts),
                new_events_upserted_last_attempt=last_new_events,
                detail=detail,
            )
        )
    return rows


def summarize_news_bucket_health(rows: list[NewsBucketHealth]) -> tuple[list[str], list[str], dict[str, dict[str, object]]]:
    required_failures = [row.bucket for row in rows if row.required and row.status != "fresh"]
    optional_warnings = [row.bucket for row in rows if not row.required and row.status != "fresh"]
    by_bucket = {
        row.bucket: {
            "required": row.required,
            "status": row.status,
            "last_attempt_status": row.last_attempt_status,
            "last_attempt_utc": row.last_attempt_utc,
            "last_success_utc": row.last_success_utc,
            "watermark_utc": row.watermark_utc,
            "new_events_upserted_last_attempt": row.new_events_upserted_last_attempt,
            "detail": row.detail,
        }
        for row in rows
    }
    return required_failures, optional_warnings, by_bucket


def asdict_rows(rows: list[NewsBucketHealth]) -> list[dict[str, object]]:
    return [asdict(row) for row in rows]
