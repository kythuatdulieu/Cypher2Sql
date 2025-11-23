from __future__ import annotations

from collections import Counter
from typing import Iterable, List, Optional, Sequence, Tuple, Set

import sqlite3


Row = Tuple


def execute_sql(conn: sqlite3.Connection, sql_text: str) -> List[Row]:
    """Thực thi SQL và trả về danh sách dòng (tuple)."""

    cur = conn.cursor()
    cur.execute(sql_text)
    rows = cur.fetchall()
    cur.close()
    return [tuple(row) for row in rows]


def table_equivalent(rows1: Sequence[Row], rows2: Sequence[Row]) -> bool:
    """Kiểm tra hai bảng có tương đương theo nghĩa bag và bỏ qua thứ tự cột."""

    if not rows1 and not rows2:
        return True
    if len(rows1) != len(rows2):
        return False
    if not rows1:
        return True
    len_cols = len(rows1[0])
    if any(len(row) != len_cols for row in rows1):
        raise ValueError("rows1 không đồng nhất số cột")
    if any(len(row) != len_cols for row in rows2):
        raise ValueError("rows2 không đồng nhất số cột")

    sig1 = [column_signature(rows1, idx) for idx in range(len_cols)]
    sig2 = [column_signature(rows2, idx) for idx in range(len_cols)]
    mapping = greedy_match_by_signature(sig1, sig2)
    if mapping is not None:
        return bags_equal(rows1, apply_perm(rows2, mapping))

    # fallback: thử tất cả hoán vị có thể khi chữ ký trùng nhau
    for perm in generate_candidate_perms(sig1, sig2):
        if bags_equal(rows1, apply_perm(rows2, perm)):
            return True
    return False


def column_signature(rows: Sequence[Row], idx: int) -> Counter:
    """Chữ ký cột = Counter giá trị (kể cả None) để hỗ trợ ghép cột."""

    return Counter(row[idx] for row in rows)


def greedy_match_by_signature(sig1: Sequence[Counter], sig2: Sequence[Counter]) -> Optional[List[int]]:
    """Ghép nhanh dựa trên chữ ký duy nhất."""

    mapping: List[Optional[int]] = [None] * len(sig1)
    used: Set[int] = set()
    for idx, sig in enumerate(sig1):
        candidates = [j for j, sig_other in enumerate(sig2) if sig_other == sig and j not in used]
        if len(candidates) == 1:
            mapping[idx] = candidates[0]
            used.add(candidates[0])
    if all(m is not None for m in mapping):
        return [m for m in mapping if m is not None]
    return None


def generate_candidate_perms(sig1: Sequence[Counter], sig2: Sequence[Counter]) -> Iterable[Tuple[int, ...]]:
    """Sinh các hoán vị phù hợp chữ ký."""

    indices = list(range(len(sig1)))
    # Chỉ giữ những ghép mà chữ ký tồn tại
    possible_positions = []
    for sig in sig1:
        positions = [idx for idx, sig_other in enumerate(sig2) if sig_other == sig]
        if not positions:
            return []
        possible_positions.append(positions)

    # backtracking đơn giản (giảm nhanh nhánh khi trùng lặp)
    def backtrack(pos: int, current: List[int], used: Set[int]) -> Iterable[Tuple[int, ...]]:
        if pos == len(indices):
            yield tuple(current)
            return
        for candidate in possible_positions[pos]:
            if candidate in used:
                continue
            used.add(candidate)
            current.append(candidate)
            yield from backtrack(pos + 1, current, used)
            current.pop()
            used.remove(candidate)

    return backtrack(0, [], set())


def apply_perm(rows: Sequence[Row], perm: Sequence[int]) -> List[Row]:
    """Áp dụng hoán vị cột."""

    return [tuple(row[idx] for idx in perm) for row in rows]


def bags_equal(rows1: Sequence[Row], rows2: Sequence[Row]) -> bool:
    """So sánh hai multiset."""

    return Counter(rows1) == Counter(rows2)


__all__ = [
    "execute_sql",
    "table_equivalent",
]
