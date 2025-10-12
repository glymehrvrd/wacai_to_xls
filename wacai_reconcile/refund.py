from __future__ import annotations

from collections import defaultdict, deque
from datetime import timedelta
from decimal import Decimal
from typing import Dict, List, Tuple

from .models import Sheet, StandardRecord
from .schema import DEFAULT_REFUND_WINDOW


def apply_refund_pairs(
    records: List[StandardRecord],
    window: timedelta = DEFAULT_REFUND_WINDOW,
) -> None:
    """Mark expense/income pairs that cancel each other out via refund."""

    expenses: Dict[Tuple[str, str, Decimal], deque[StandardRecord]] = defaultdict(deque)
    incomes: Dict[Tuple[str, str, Decimal], deque[StandardRecord]] = defaultdict(deque)

    for record in records:
        if record.sheet not in {Sheet.EXPENSE, Sheet.INCOME} or record.canceled:
            continue
        key = (
            record.account,
            record.meta.matching_key or record.remark,
            record.amount.copy_sign(Decimal("1.00")),
        )
        if record.sheet == Sheet.EXPENSE:
            expenses[key].append(record)
        elif record.sheet == Sheet.INCOME:
            incomes[key].append(record)

    for key, exp_queue in list(expenses.items()):
        income_queue = incomes.get(key)
        if not income_queue:
            continue
        exp_queue = deque(sorted(exp_queue, key=lambda r: r.timestamp))
        income_queue = deque(sorted(income_queue, key=lambda r: r.timestamp))
        expenses[key] = exp_queue
        incomes[key] = income_queue
        while exp_queue and income_queue:
            expense = exp_queue[0]
            income = income_queue[0]
            delta = abs(expense.timestamp - income.timestamp)
            if delta > window:
                # Drop the earlier record and continue searching.
                if expense.timestamp < income.timestamp:
                    exp_queue.popleft()
                else:
                    income_queue.popleft()
                continue
            expense.canceled = True
            expense.skipped_reason = "refund-matched"
            income.canceled = True
            income.skipped_reason = "refund-matched"
            exp_queue.popleft()
            income_queue.popleft()
