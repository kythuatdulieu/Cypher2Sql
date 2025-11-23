from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple, Union


@dataclass(slots=True)
class SQLExpr:
    """Biểu thức SQL trừu tượng."""

    kind: str
    args: Tuple[Union[str, "SQLExpr"], ...]


@dataclass(slots=True)
class Predicate:
    """Mệnh đề logic trong SQL (WHERE, ON, HAVING)."""

    kind: str
    args: Tuple[Union[str, SQLExpr, "Predicate"], ...]


@dataclass(slots=True)
class FromTable:
    """ρ alias (table)."""

    table: str
    alias: str


@dataclass(slots=True)
class Join:
    """JOIN giữa hai biểu thức SQL."""

    left: "SQL"
    right: "SQL"
    on: Predicate
    kind: str = "inner"


@dataclass(slots=True)
class Project:
    """PROJECT (SELECT list)."""

    sub: "SQL"
    items: List[Tuple[str, SQLExpr]]


@dataclass(slots=True)
class GroupBy:
    """GROUP BY với danh sách khóa và biểu thức."""

    sub: "SQL"
    keys: List[SQLExpr]
    items: List[Tuple[str, SQLExpr]]
    having: Optional[Predicate] = None


@dataclass(slots=True)
class Select:
    """Bộ lọc (σ)."""

    sub: "SQL"
    pred: Predicate


@dataclass(slots=True)
class WithCTE:
    """CTE: WITH name AS (sub) body."""

    name: str
    sub: "SQL"
    body: "SQL"


@dataclass(slots=True)
class UnionIR:
    """UNION/UNION ALL."""

    left: "SQL"
    right: "SQL"
    all: bool


@dataclass(slots=True)
class OrderByIR:
    """ORDER BY."""

    sub: "SQL"
    key: SQLExpr
    asc: bool


SQL = Union[
    FromTable,
    Join,
    Project,
    GroupBy,
    Select,
    WithCTE,
    UnionIR,
    OrderByIR,
]
