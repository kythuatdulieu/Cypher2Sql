from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple


@dataclass(slots=True)
class SDTRule:
    """Một luật ánh xạ giữa thế giới đồ thị và bảng quan hệ."""

    left_pred: Tuple[str, List[str]]
    right_pred: Tuple[str, List[str]]


@dataclass(slots=True)
class SDT:
    """Toàn bộ tập luật SDT."""

    rules: List[SDTRule] = field(default_factory=list)

    def add_rule(self, rule: SDTRule) -> None:
        """Thêm luật mới vào SDT."""

        self.rules.append(rule)
