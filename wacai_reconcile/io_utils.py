from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd

from .schema import DATE_COLUMNS, SHEET_COLUMNS, SHEET_NAMES
from .models import StandardRecord


def load_wacai_workbook(path: Path) -> Dict[str, pd.DataFrame]:
    """Read an existing wacai-format workbook into DataFrames.

    返回:
        dict[str, pd.DataFrame]: 示例：{"支出": pd.DataFrame(...), "收入": pd.DataFrame(...)}
    """
    if not path.exists():
        raise FileNotFoundError(f"baseline workbook not found: {path}")
    excel = pd.ExcelFile(path)
    frames: Dict[str, pd.DataFrame] = {}
    for sheet in SHEET_NAMES:
        if sheet not in excel.sheet_names:
            frames[sheet] = pd.DataFrame(columns=SHEET_COLUMNS[sheet])  # 缺少工作表时补空表保持结构
            continue
        frames[sheet] = excel.parse(sheet)
    return frames


def write_wacai_workbook(frames: Dict[str, pd.DataFrame], output_path: Path) -> None:
    """Write DataFrames into a wacai-format workbook."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet in SHEET_NAMES:
            frame = frames.get(sheet)
            if frame is None:
                frame = pd.DataFrame(columns=SHEET_COLUMNS[sheet])  # 缺少数据时写入空表占位
            frame.to_excel(writer, sheet_name=sheet, index=False)


def sort_by_date_asc(frames: Dict[str, pd.DataFrame]) -> None:
    """Sort in-place by date column ascending for each sheet."""
    for sheet, frame in frames.items():
        date_col = DATE_COLUMNS.get(sheet)
        if not date_col or date_col not in frame.columns or frame.empty:
            continue
        frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce")
        frame.sort_values(by=date_col, ascending=True, inplace=True)
        frame.reset_index(drop=True, inplace=True)
        frame[date_col] = frame[date_col].dt.strftime("%Y-%m-%d %H:%M:%S")  # 格式化为 Excel 友好字符串


def build_increment_frames(records: Iterable[StandardRecord]) -> Dict[str, pd.DataFrame]:
    """Construct DataFrames containing only accepted records for each sheet.

    返回:
        dict[str, pd.DataFrame]: 示例：{"支出": pd.DataFrame([...]), "收入": 空 DataFrame}
    """
    result: Dict[str, pd.DataFrame] = {}
    for sheet in SHEET_NAMES:
        result[sheet] = pd.DataFrame(columns=SHEET_COLUMNS[sheet])  # 初始化各 sheet 空 DataFrame
    for record in records:
        if record.canceled or record.skipped_reason:
            continue
        # DataFrame 构建保持模板列顺序，方便直接写出。
        row_df = pd.DataFrame([record.to_row()], columns=SHEET_COLUMNS[record.sheet])
        result[record.sheet] = pd.concat([result[record.sheet], row_df], ignore_index=True)
    return result
