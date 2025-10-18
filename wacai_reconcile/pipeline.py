from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import json

from .baseline import BaselineIndex, build_account_locks
from .io_utils import (
    build_increment_frames,
    load_wacai_workbook,
    sort_by_date_asc,
    write_wacai_workbook,
)
from .models import SheetBundle, StandardRecord
from .parsers import parse_alipay, parse_citic, parse_cmb, parse_wechat
from .refund import apply_refund_pairs
from .schema import DEFAULT_AMOUNT_TOLERANCE, DEFAULT_DATE_TOLERANCE, DEFAULT_REFUND_WINDOW, SHEET_COLUMNS


@dataclass
class ReconcileOptions:
    input_dir: Path
    output_prefix: Path
    baseline_path: Optional[Path] = None
    intermediate_dir: Optional[Path] = None
    dry_run: bool = False
    auto_confirm: bool = False
    amount_tolerance: float = DEFAULT_AMOUNT_TOLERANCE
    date_tolerance: timedelta = DEFAULT_DATE_TOLERANCE
    refund_window: timedelta = DEFAULT_REFUND_WINDOW
    disable_account_lock: bool = False
    report_path: Optional[Path] = None
    incremental_only: bool = False

    def resolved_baseline(self) -> Path:
        if self.baseline_path:
            return self.baseline_path
        return self.input_dir / "wacai.xlsx"


@dataclass
class ReconcileResult:
    output_path: Optional[Path]
    accepted: int
    skipped: int
    canceled: int
    pending: int
    report_path: Optional[Path] = None


CHANNEL_FILES = {
    "wechat": ("微信支付", parse_wechat, ["微信支付账单流水", "微信支付账单", "wechat"]),
    "alipay": ("支付宝", parse_alipay, ["支付宝交易明细", "alipay"]),
    "citic": ("中信银行信用卡", parse_citic, ["中信银行信用卡", "citic"]),
    "cmb": ("招商银行信用卡", parse_cmb, ["招商银行信用卡", "cmb"]),
}


def _sanitize_name(name: str) -> str:
    return "".join(ch for ch in name if ch.isalnum() or ch in {"-", "_"}).lower() or "channel"


def discover_channel_files(input_dir: Path) -> Dict[str, Path]:
    """Best-effort scan for each渠道文件; picks the first match per渠道."""
    found: Dict[str, Path] = {}
    for channel, (_, _, patterns) in CHANNEL_FILES.items():
        for pattern in patterns:
            # 示例：pattern = "wechat" 时，可匹配到 `微信支付账单流水...xlsx`
            for path in input_dir.glob(f"*{pattern}*"):
                if path.is_file():
                    found[channel] = path
                    break
            if channel in found:
                break
    return found


def parse_channels(channel_paths: Dict[str, Path]) -> Dict[str, List[StandardRecord]]:
    channel_records: Dict[str, List[StandardRecord]] = {}
    for channel, path in channel_paths.items():
        label, parser, _ = CHANNEL_FILES[channel]
        records = parser(path)
        for record in records:
            # Tag source信息，后续日志/报告直接可见来源。
            record.meta.channel = channel  # 示例：wechat
            record.meta.channel_label = label  # 示例：微信支付
        channel_records[channel] = records
    return channel_records


def write_intermediate_csv(intermediate_dir: Path, channel: str, records: Iterable[StandardRecord]) -> None:
    # Persist中间CSV供调试或人工校验。
    intermediate_dir.mkdir(parents=True, exist_ok=True)
    channel_dir = intermediate_dir / _sanitize_name(channel)
    channel_dir.mkdir(parents=True, exist_ok=True)
    by_sheet: Dict[str, List[dict]] = {sheet: [] for sheet in SHEET_COLUMNS}
    for record in records:
        if record.meta.supplement_only:
            continue
        by_sheet[record.sheet.value].append(record.to_row())
    for sheet, rows in by_sheet.items():
        df = pd.DataFrame(rows, columns=SHEET_COLUMNS[sheet])
        csv_path = channel_dir / f"{sheet}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def apply_account_locks(records: Iterable[StandardRecord], locks: Dict[str, datetime]) -> None:
    for record in records:
        if record.skipped_reason or record.canceled:
            continue
        lock = locks.get(record.account)
        if lock and record.timestamp <= lock:
            record.skipped_reason = "account-locked"


def apply_baseline_dedupe(records: Iterable[StandardRecord], baseline_index: BaselineIndex) -> None:
    for record in records:
        if record.skipped_reason or record.canceled:
            continue
        if baseline_index.exists(record.sheet.value, record.account, record.amount, record.timestamp, record.remark):
            record.skipped_reason = "duplicate-baseline"


def supplement_card_remarks(
    records: Iterable[StandardRecord],
    amount_tolerance: Decimal,
    date_tolerance: timedelta,
) -> None:
    """Enrich card账单备注 with wallet来源备注，便于理解交易含义。"""
    wallet_channels = {"wechat", "alipay"}
    card_channels = {"citic", "cmb"}
    wallet_records = [
        wallet
        for wallet in records
        if wallet.meta.channel in wallet_channels and not wallet.canceled and not wallet.meta.supplement_only
    ]
    wallet_by_remark: Dict[str, List[StandardRecord]] = {}
    for wallet in wallet_records:
        key = wallet.meta.base_remark or wallet.remark
        if not key:
            continue
        wallet_by_remark.setdefault(key, []).append(wallet)

    for record in records:
        if record.canceled:
            continue
        if record.skipped_reason and record.skipped_reason != "channel-duplicate":
            continue
        if record.meta.channel not in card_channels:
            continue
        base_remark = record.meta.base_remark or record.remark
        if not base_remark:
            continue
        card_account = record.account
        account_root = card_account.split("(")[0].strip() if card_account else ""
        for wallet in wallet_by_remark.get(base_remark, []):
            pay_method = wallet.meta.source_extras.get("支付方式", "")
            if card_account and card_account not in pay_method:
                if account_root and account_root not in pay_method:
                    continue
            status_text = wallet.meta.source_extras.get("状态", "")
            base_text = wallet.meta.base_remark or ""
            base_parts = [part.strip() for part in base_text.split(";") if part.strip() and part.strip().lower() != "nan"]
            supplement_candidates = base_parts.copy()
            if status_text:
                supplement_candidates.append(f"状态: {status_text}")
            supplement_text = "; ".join(dict.fromkeys(supplement_candidates))
            direction_match = record.direction == wallet.direction
            refund_match = (
                record.direction == "income"
                and wallet.direction == "expense"
                and any(keyword in (supplement_text or "") + status_text for keyword in ("退款", "关闭", "退回"))
            )
            if not direction_match and not refund_match:
                continue
            if abs((wallet.timestamp - record.timestamp).total_seconds()) > date_tolerance.total_seconds():
                continue
            if abs(wallet.amount - record.amount) > amount_tolerance:
                continue
            supplement = supplement_text or wallet.meta.base_remark or wallet.remark
            if not supplement:
                continue
            existing = record.remark or ""
            if supplement in existing and "来源补充(" in existing:
                break
            prefix = f"来源补充({wallet.source}): "
            record.remark = f"{existing}; {prefix}{supplement}" if existing else f"{prefix}{supplement}"
            record.meta.supplemented_from = wallet.meta.channel
            break


def reconcile(options: ReconcileOptions) -> ReconcileResult:
    input_dir = options.input_dir
    baseline_path = options.resolved_baseline()
    baseline_frames = load_wacai_workbook(baseline_path)

    channel_paths = discover_channel_files(input_dir)
    channel_records = parse_channels(channel_paths)

    # 拉平所有渠道记录后统一走锁定、退款、去重等校验。
    all_records = [record for records in channel_records.values() for record in records]

    locks = {}
    if not options.disable_account_lock:
        locks = build_account_locks(baseline_frames)
    apply_account_locks(all_records, locks)

    apply_refund_pairs(all_records, window=options.refund_window)

    baseline_index = BaselineIndex(
        baseline_frames,
        amount_tolerance=options.amount_tolerance,
        date_tolerance=options.date_tolerance,
    )
    apply_baseline_dedupe(all_records, baseline_index)
    supplement_card_remarks(
        all_records,
        amount_tolerance=Decimal(str(options.amount_tolerance)),
        date_tolerance=options.date_tolerance,
    )

    if options.intermediate_dir:
        grouped: Dict[str, List[StandardRecord]] = {}
        for record in all_records:
            channel = record.meta.channel or "unknown"
            grouped.setdefault(channel, []).append(record)
        for channel, records in grouped.items():
            write_intermediate_csv(options.intermediate_dir, channel, records)

        # 汇总所有记录写入单一 Excel，方便一次性查看。
        all_frames: Dict[str, List[dict]] = {sheet: [] for sheet in SHEET_COLUMNS}
        debug_rows: List[dict] = []
        for record in all_records:
            if record.meta.supplement_only:
                continue
            all_frames[record.sheet.value].append(record.to_row())
            debug_rows.append(
                {
                    "sheet": record.sheet.value,
                    "timestamp": record.timestamp.isoformat(),
                    "amount": float(record.amount),
                    "direction": record.direction,
                    "account": record.account,
                    "remark": record.remark,
                    "source": record.source,
                    "raw_id": record.raw_id or "",
                    "canceled": record.canceled,
                    "skipped_reason": record.skipped_reason or "",
                    "meta.base_remark": record.meta.base_remark or "",
                    "meta.merchant": record.meta.merchant or "",
                    "meta.matching_key": record.meta.matching_key or "",
                    "meta.channel": record.meta.channel or "",
                    "meta.channel_label": record.meta.channel_label or "",
                    "meta.supplement_only": record.meta.supplement_only,
                    "meta.duplicate_with": record.meta.duplicate_with or "",
                    "meta.supplemented_from": record.meta.supplemented_from or "",
                    "meta.accepted": record.meta.accepted,
                    "meta.source_extras": json.dumps(record.meta.source_extras, ensure_ascii=False),
                    "row_json": json.dumps(record.to_row(), ensure_ascii=False),
                }
            )
        all_path = options.intermediate_dir / "all_records.xlsx"
        with pd.ExcelWriter(all_path, engine="openpyxl") as writer:
            for sheet, rows in all_frames.items():
                df = pd.DataFrame(rows, columns=SHEET_COLUMNS[sheet])
                df.to_excel(writer, sheet_name=sheet, index=False)
            debug_df = pd.DataFrame(debug_rows)
            debug_df.to_excel(writer, sheet_name="all_records_debug", index=False)

    actionable_records = [record for record in all_records if not record.skipped_reason and not record.canceled]

    accepted_records: List[StandardRecord] = []
    if options.auto_confirm or not actionable_records:
        accepted_records = actionable_records
    else:
        accept_all = False
        skip_all = False
        for record in actionable_records:
            if accept_all:
                accepted_records.append(record)
                continue
            if skip_all:
                record.skipped_reason = "user-skip"
                continue
            print_record_summary(record)
            resp = input("导入? [Y]es/[n]o/[a]ll/[s]kip all/[q]uit: ").strip().lower()
            if resp in {"", "y", "yes"}:
                accepted_records.append(record)
            elif resp in {"n", "no"}:
                record.skipped_reason = "user-skip"
            elif resp == "a":
                accept_all = True
                accepted_records.append(record)
            elif resp == "s":
                skip_all = True
                record.skipped_reason = "user-skip"
            elif resp == "q":
                record.skipped_reason = "user-abort"
                skip_all = True
            else:
                record.skipped_reason = "user-skip"

    for record in accepted_records:
        record.meta.accepted = True

    if options.incremental_only:
        output_frames = build_increment_frames(accepted_records)
    else:
        bundle = SheetBundle()
        bundle.frames = {sheet: frame.copy() for sheet, frame in baseline_frames.items()}
        bundle.update_from_records(accepted_records)
        sort_by_date_asc(bundle.frames)
        output_frames = bundle.frames

    output_path: Optional[Path] = None
    if not options.dry_run:
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        output_path = options.output_prefix.parent / f"{options.output_prefix.name}-{timestamp}.xlsx"
        sort_by_date_asc(output_frames)
        write_wacai_workbook(output_frames, output_path)

    report_path = options.report_path
    if report_path:
        write_report(report_path, all_records)

    result = ReconcileResult(
        output_path=output_path,
        accepted=len(accepted_records),
        skipped=len([r for r in all_records if r.skipped_reason]),
        canceled=len([r for r in all_records if r.canceled]),
        pending=len(actionable_records) - len(accepted_records),
        report_path=report_path,
    )
    return result


def write_report(path: Path, records: Iterable[StandardRecord]) -> None:
    rows = []
    for record in records:
        status = (
            "canceled"
            if record.canceled
            else ("skipped" if record.skipped_reason else ("accepted" if record.meta.accepted else "pending"))
        )
        rows.append(
            {
                "sheet": record.sheet.value,
                "account": record.account,
                "timestamp": record.timestamp.isoformat(),
                "amount": float(record.amount),
                "source": record.source,
                "channel": record.meta.channel,
                "remark": record.remark,
                "status": status,
                "reason": record.skipped_reason or "",
            }
        )
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def print_record_summary(record: StandardRecord) -> None:
    print("-" * 60)
    print(f"{record.sheet.value} | {record.account} | {record.amount:.2f} | {record.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"来源: {record.source} | 备注: {record.remark}")
    print("-" * 60)
