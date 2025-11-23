from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple, Union


# =============================================================================
# Mẫu nút/cạnh/path
# =============================================================================


@dataclass(slots=True)
class NodePat:
    """(x:LABEL)"""

    var: str
    label: str


@dataclass(slots=True)
class EdgePat:
    """-[e:LABEL]-> hoặc <-[e:LABEL]- hoặc -[e:LABEL]- (vô hướng)."""

    var: str
    label: str
    direction: str  # "->", "<-", "--"


@dataclass(slots=True)
class PathPat:
    """Danh sách xen kẽ NodePat và EdgePat."""

    items: Sequence[Union[NodePat, EdgePat]]


# =============================================================================
# Clause
# =============================================================================


@dataclass(slots=True)
class ClauseMatch:
    """MATCH pattern [WHERE expr]"""

    pattern: PathPat
    where: Optional["Predicate"] = None


@dataclass(slots=True)
class ClauseOptMatch(ClauseMatch):
    """OPTIONAL MATCH pattern [WHERE expr]"""


@dataclass(slots=True)
class ClauseWith:
    """WITH biểu_đề AS alias ..."""

    prev: "Clause"
    keep_vars: List[str]
    rename_to: List[str]


Clause = Union[ClauseMatch, ClauseOptMatch, ClauseWith]


# =============================================================================
# Biểu thức
# =============================================================================


@dataclass(slots=True)
class ExprProp:
    """Truy cập thuộc tính: var.key"""

    var: str
    key: str


@dataclass(slots=True)
class ExprAgg:
    """Hàm tổng hợp như COUNT, SUM."""

    fn: str
    expr: "Expr"


@dataclass(slots=True)
class ExprVar:
    """Biểu thức giữ nguyên biến (dùng khi WITH đổi tên)."""

    name: str


Expr = Union[ExprProp, ExprAgg, ExprVar, int, str, float]


# =============================================================================
# Predicate
# =============================================================================


@dataclass(slots=True)
class PredicateCompare:
    """So sánh nhị nguyên trong WHERE/ON."""

    left: Expr
    op: str
    right: Expr


@dataclass(slots=True)
class PredicateAnd:
    """AND."""

    left: "Predicate"
    right: "Predicate"


@dataclass(slots=True)
class PredicateOr:
    """OR."""

    left: "Predicate"
    right: "Predicate"


@dataclass(slots=True)
class PredicateNot:
    """NOT."""

    sub: "Predicate"


Predicate = Union[PredicateCompare, PredicateAnd, PredicateOr, PredicateNot]


# =============================================================================
# Query
# =============================================================================


@dataclass(slots=True)
class ReturnQuery:
    """MATCH ... RETURN expr AS name, ..."""

    clause: Clause
    exprs: List[Expr]
    names: List[str]


@dataclass(slots=True)
class OrderBy:
    """ORDER BY expr [ASC|DESC]"""

    sub: "Query"
    key: Expr
    asc: bool = True


@dataclass(slots=True)
class UnionQuery:
    """UNION [ALL]"""

    left: "Query"
    right: "Query"
    all: bool = False


Query = Union[ReturnQuery, OrderBy, UnionQuery]
