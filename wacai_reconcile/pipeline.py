from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import json

from .baseline import BaselineIndex, build_account_locks, normalize_account_name
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
    # 过滤掉 supplement_only 的记录，这些记录仅用于补充信息，不进入中间表。
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
        # 标准化账户名称，去掉尾号（括号及其内容）
        normalized_account = normalize_account_name(record.account)
        lock = locks.get(normalized_account)
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
    """Enrich card账单备注 with wallet来源备注，便于理解交易含义。

    匹配算法流程：
    1. 建立钱包记录索引：按商家名称（meta.merchant）建立倒排索引，方便快速查找
    2. 遍历卡片记录，对每条记录执行匹配：
       a. 通过商家匹配：银行卡账单商家格式为"财付通-xxx"或"支付宝-xxx"，去掉前缀后与钱包商家匹配
       b. 支付方式匹配：钱包记录的支付方式必须包含卡片账户名（支持完整匹配或根账户匹配）
       c. 方向匹配：需满足以下条件之一：
          - 方向一致（支出对支出，收入对收入）
          - 退款匹配：卡片为收入且钱包为支出，且钱包状态包含退款关键词（退款/关闭/退回）
       d. 时间匹配：钱包和卡片的时间差需在容差范围内
       e. 金额匹配：钱包和卡片的金额差需在容差范围内
    3. 匹配成功后将钱包记录的备注补充到卡片记录中，格式：现有备注; 来源补充(渠道): 补充内容
    """
    wallet_channels = {"wechat", "alipay"}

    # 步骤1：建立钱包记录的倒排索引（按商家索引）
    # 用于快速查找与卡片记录商家匹配的钱包记录
    wallet_records = [wallet for wallet in records if wallet.meta.channel in wallet_channels and not wallet.canceled]
    wallet_by_merchant: Dict[str, List[StandardRecord]] = {}
    for wallet in wallet_records:
        merchant = wallet.meta.merchant
        if not merchant:
            continue
        wallet_by_merchant.setdefault(merchant, []).append(wallet)

    # 步骤2：遍历卡片记录，查找匹配的钱包记录并补充备注
    for record in records:
        if record.canceled:
            continue
        if record.skipped_reason and record.skipped_reason != "channel-duplicate":
            continue
        if record.meta.channel in wallet_channels:
            continue

        # 获取卡片记录的商家名称（可能是银行卡账单中的"财付通-xxx"或"支付宝-xxx"格式）
        card_merchant = record.meta.merchant
        if not card_merchant:
            continue

        # 2a. 通过商家匹配：银行卡账单商家格式为"财付通-xxx"或"支付宝-xxx"
        # 去掉前缀后与钱包商家匹配
        # 例如：银行卡"财付通-一码通行" -> "一码通行"，与微信商家"一码通行"匹配
        #      银行卡"支付宝-xxx" -> "xxx"，与支付宝商家"xxx"匹配
        merchant_normalized = card_merchant
        if card_merchant.startswith("财付通-"):
            merchant_normalized = card_merchant[4:]  # 去掉"财付通-"前缀
        elif card_merchant.startswith("支付宝-"):
            merchant_normalized = card_merchant[4:]  # 去掉"支付宝-"前缀

        card_account = record.account
        account_root = card_account.split("(")[0].strip() if card_account else ""
        for wallet in wallet_by_merchant.get(merchant_normalized, []):
            # 2b. 支付方式匹配：钱包的支付方式必须包含卡片账户名
            # 支持完整匹配（如"中信银行信用卡(1129)"）或根账户匹配（如"中信银行信用卡"）
            pay_method = wallet.meta.source_extras.get("支付方式", "")
            if card_account and card_account not in pay_method:
                if account_root and account_root not in pay_method:
                    continue  # 支付方式不匹配，跳过该钱包记录

            # 2c. 方向匹配：检查方向是否一致
            direction_match = record.direction == wallet.direction
            if not direction_match:
                continue  # 方向不匹配，跳过

            # 2d. 时间匹配：检查时间差是否在容差范围内
            if abs((wallet.timestamp - record.timestamp).total_seconds()) > date_tolerance.total_seconds():
                continue  # 时间差超出容差，跳过

            # 2e. 金额匹配：检查金额差是否在容差范围内
            if abs(wallet.amount - record.amount) > amount_tolerance:
                continue  # 金额差超出容差，跳过

            # 步骤3：匹配成功，准备补充备注
            # 补充内容不包含状态信息，只使用 base_remark
            supplement = wallet.meta.base_remark or ""
            if not supplement:
                continue  # 没有可补充的内容，跳过

            existing = record.remark or ""
            # 避免重复补充：如果补充内容已存在且已有"来源补充"标记，则跳过
            if supplement in existing and "来源补充(" in existing:
                break

            # 添加补充备注，格式：现有备注; 来源补充(渠道): 补充内容
            channel_label = wallet.meta.channel_label or "未知渠道"
            prefix = f"来源补充({channel_label}): "
            record.remark = f"{existing}; {prefix}{supplement}" if existing else f"{prefix}{supplement}"
            record.meta.supplemented_from = wallet.meta.channel
            break  # 找到一个匹配后停止查找


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
            all_frames[record.sheet.value].append(record.to_row())
            debug_rows.append(
                {
                    "sheet": record.sheet.value,
                    "timestamp": record.timestamp.isoformat(),
                    "amount": float(record.amount),
                    "direction": record.direction,
                    "account": record.account,
                    "remark": record.remark,
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
                "channel": record.meta.channel,
                "channel_label": record.meta.channel_label or "",
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
    print(
        f"{record.sheet.value} | {record.account} | {record.amount:.2f} | "
        f"{record.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    channel_label = record.meta.channel_label or "未知渠道"
    print(f"来源: {channel_label} | 备注: {record.remark}")
    print("-" * 60)
