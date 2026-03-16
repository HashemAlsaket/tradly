from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class ArtifactAlignmentResult:
    artifact_name: str
    valid: bool
    run_timestamp_utc: str | None
    age_sec: int | None
    max_age_sec: int
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_artifact_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def assess_artifact_alignment(
    *,
    artifact_name: str,
    payload: dict,
    now_utc: datetime,
    max_age: timedelta,
) -> ArtifactAlignmentResult:
    run_ts = parse_artifact_timestamp(payload.get("run_timestamp_utc"))
    reason_codes: list[str] = []
    age_sec: int | None = None

    if run_ts is None:
        reason_codes.append(f"{artifact_name}_timestamp_missing")
    else:
        age_sec = int((now_utc - run_ts.astimezone(timezone.utc)).total_seconds())
        if age_sec < 0:
            reason_codes.append(f"{artifact_name}_timestamp_future")
        if age_sec > int(max_age.total_seconds()):
            reason_codes.append(f"{artifact_name}_stale_for_downstream")

    return ArtifactAlignmentResult(
        artifact_name=artifact_name,
        valid=not reason_codes,
        run_timestamp_utc=run_ts.astimezone(timezone.utc).isoformat() if run_ts is not None else None,
        age_sec=age_sec,
        max_age_sec=int(max_age.total_seconds()),
        reason_codes=tuple(reason_codes),
    )
