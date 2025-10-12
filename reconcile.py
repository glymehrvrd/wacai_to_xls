from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path

from wacai_reconcile.pipeline import ReconcileOptions, reconcile


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="多渠道账单标准化、对账并生成 Wacai 模板文件。")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data"),
        help="包含各渠道账单及 wacai.xlsx 基线文件的目录（默认: data/）",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        help="指定基线 wacai.xlsx 路径，覆盖默认 input-dir/wacai.xlsx。",
    )
    parser.add_argument(
        "--output-prefix",
        type=Path,
        default=Path("data/wacai"),
        help="输出文件前缀，最终文件会附加时间戳（默认: data/wacai）。",
    )
    parser.add_argument(
        "--intermediate-dir",
        type=Path,
        help="输出各渠道标准化后的中间 CSV 目录。",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        help="将对账结果写入 CSV 报告。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅执行解析与对账，不写出最终 Excel。",
    )
    parser.add_argument(
        "--auto-confirm",
        action="store_true",
        help="无需交互自动确认全部可导入交易。",
    )
    parser.add_argument(
        "--amount-tolerance",
        type=float,
        default=0.01,
        help="金额比对容差，默认 0.01。",
    )
    parser.add_argument(
        "--date-tolerance",
        type=str,
        default="48h",
        help="日期比对容差，支持 Nh/Nd 表示小时或天（默认: 48h）。",
    )
    parser.add_argument(
        "--refund-window",
        type=str,
        default="30d",
        help="支出与退款匹配窗口，支持 Nh/Nd/Nm（默认: 30d）。",
    )
    parser.add_argument(
        "--disable-account-lock",
        action="store_true",
        help="关闭余额调整/漏记款触发的账户锁定逻辑。",
    )
    parser.add_argument(
        "--incremental-only",
        action="store_true",
        help="仅输出增量交易，不与基线数据合并。",
    )
    return parser.parse_args()


def parse_duration(value: str) -> timedelta:
    value = value.strip().lower()
    if value.endswith("h"):
        hours = float(value[:-1])
        return timedelta(hours=hours)
    if value.endswith("d"):
        days = float(value[:-1])
        return timedelta(days=days)
    if value.endswith("m"):
        minutes = float(value[:-1])
        return timedelta(minutes=minutes)
    raise ValueError(f"无法解析时间跨度: {value}")


def main() -> None:
    args = parse_args()
    options = ReconcileOptions(
        input_dir=args.input_dir,
        output_prefix=args.output_prefix,
        baseline_path=args.baseline,
        intermediate_dir=args.intermediate_dir,
        dry_run=args.dry_run,
        auto_confirm=args.auto_confirm,
        amount_tolerance=args.amount_tolerance,
        date_tolerance=parse_duration(args.date_tolerance),
        refund_window=parse_duration(args.refund_window),
        disable_account_lock=args.disable_account_lock,
        report_path=args.report_path,
        incremental_only=args.incremental_only,
    )
    result = reconcile(options)
    print("=== 对账完成 ===")
    if result.output_path:
        print(f"输出文件: {result.output_path}")
    else:
        print("Dry-run 模式，未生成 Excel。")
    print(f"导入 {result.accepted} 条，跳过 {result.skipped} 条，退款抵消 {result.canceled} 条，待确认 {result.pending} 条。")
    if result.report_path:
        print(f"报告: {result.report_path}")


if __name__ == "__main__":
    main()
