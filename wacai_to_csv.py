"""Convert the official Wacai Excel export into sheet-level CSV files.

Usage:
    uv run python wacai_to_csv.py --input data/wacai.xlsx --output-dir data/
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


SHEET_TO_FILENAME = {
    "支出": "wacai_zhichu.csv",
    "收入": "wacai_shouru.csv",
    "转账": "wacai_zhuanzhang.csv",
    "借入借出": "wacai_jierujiechu.csv",
    "收款还款": "wacai_shoukuanhuankuan.csv",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split a Wacai Excel workbook into individual CSV files."
    )
    parser.add_argument(
        "--input",
        default="data/wacai.xlsx",
        type=Path,
        help="Path to the Wacai Excel workbook (default: data/wacai.xlsx).",
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data"),
        type=Path,
        help="Directory to write CSV files into (default: data/).",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="Encoding for generated CSV files (default: utf-8-sig).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise SystemExit(f"input Excel file not found: {args.input}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    excel = pd.ExcelFile(args.input)
    for sheet_name in excel.sheet_names:
        df = excel.parse(sheet_name)
        filename = SHEET_TO_FILENAME.get(sheet_name, f"{sheet_name}.csv")
        output_path = args.output_dir / filename
        df.to_csv(output_path, index=False, encoding=args.encoding)


if __name__ == "__main__":
    main()
