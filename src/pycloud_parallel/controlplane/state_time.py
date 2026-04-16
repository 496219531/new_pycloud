from __future__ import annotations

"""Time conversion helpers for control-plane state modules."""

from datetime import datetime, timezone

from google.protobuf import timestamp_pb2


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_ts(dt: datetime) -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts


def ts_to_dt(ts: timestamp_pb2.Timestamp) -> datetime:
    if ts is None:
        return utc_now()
    if ts.seconds == 0 and ts.nanos == 0:
        return utc_now()
    try:
        dt = ts.ToDatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return utc_now()


__all__ = ["utc_now", "dt_to_ts", "ts_to_dt"]
