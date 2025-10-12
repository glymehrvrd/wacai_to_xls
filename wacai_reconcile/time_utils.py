from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

from zoneinfo import ZoneInfo

import pandas as pd

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def as_datetime(value: Union[str, datetime]) -> Optional[datetime]:
    """Parse assorted时间字段为上海时区 datetime，示例："2025-10-11 12:25:03" -> tz-aware datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        value = str(value).strip()
        if not value:
            return None
        # pandas often provides ISO-like strings
        fmt_candidates = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d",
            "%Y-%m-%d %H:%M",
        ]
        for fmt in fmt_candidates:
            try:
                dt = datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
        else:
            # Fallback to pandas parsing
            dt = pd.to_datetime(value, errors="coerce")
            if pd.isna(dt):
                return None
            dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=SHANGHAI_TZ)
    return dt.astimezone(SHANGHAI_TZ)
