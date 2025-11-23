from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass(slots=True)
class Table:
    """Bảng quan hệ với PK/FK theo quy tắc của Graphiti."""

    name: str
    attrs: List[str]
    pk: str
    fks: Dict[str, Tuple[str, str]] = field(default_factory=dict)

    def ensure_attr(self, attr: str) -> None:
        """Đảm bảo một thuộc tính tồn tại trong bảng, nếu chưa thì thêm mới."""

        if attr not in self.attrs:
            self.attrs.append(attr)


@dataclass(slots=True)
class RelationalSchema:
    """Tập hợp các bảng tương ứng với lược đồ quan hệ."""

    tables: Dict[str, Table] = field(default_factory=dict)

    def add_table(self, table: Table) -> None:
        """Thêm bảng mới, tránh trùng tên."""

        if table.name in self.tables:
            raise ValueError(f"Bảng {table.name} đã tồn tại trong lược đồ")
        self.tables[table.name] = table

    def get_table(self, name: str) -> Table:
        """Trả về bảng theo tên, ném lỗi khi không tìm thấy."""

        try:
            return self.tables[name]
        except KeyError as exc:
            raise KeyError(f"Không tìm thấy bảng {name}") from exc
