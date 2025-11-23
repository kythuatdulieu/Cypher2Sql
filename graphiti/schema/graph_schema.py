from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(slots=True)
class NodeType:
    """Định nghĩa một loại nút trong đồ thị."""

    label: str
    keys: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.label:
            raise ValueError("label của NodeType không được rỗng")
        if not self.keys:
            raise ValueError("NodeType phải có ít nhất một khóa để làm khóa mặc định")

    @property
    def default_key(self) -> str:
        """Trả về khóa mặc định (theo bài báo: phần tử đầu tiên trong danh sách khóa)."""

        return self.keys[0]


@dataclass(slots=True)
class EdgeType:
    """Định nghĩa một loại cạnh có hướng trong đồ thị."""

    label: str
    src_label: str
    tgt_label: str
    keys: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.label:
            raise ValueError("label của EdgeType không được rỗng")
        if not self.src_label or not self.tgt_label:
            raise ValueError("EdgeType cần cả nhãn nguồn và nhãn đích")
        if not self.keys:
            raise ValueError("EdgeType phải có ít nhất một khóa để làm khóa mặc định")

    @property
    def default_key(self) -> str:
        """Trả về khóa mặc định cho cạnh (khóa đầu tiên)."""

        return self.keys[0]


@dataclass(slots=True)
class GraphSchema:
    """Tập hợp NodeType và EdgeType tạo thành lược đồ đồ thị."""

    nodes: Dict[str, NodeType] = field(default_factory=dict)
    edges: Dict[str, EdgeType] = field(default_factory=dict)

    def add_node(self, node: NodeType) -> None:
        """Đăng ký thêm một loại nút, đảm bảo không trùng nhãn."""

        if node.label in self.nodes:
            raise ValueError(f"NodeType {node.label} đã tồn tại")
        self.nodes[node.label] = node

    def add_edge(self, edge: EdgeType) -> None:
        """Đăng ký thêm một loại cạnh, yêu cầu đã tồn tại nút nguồn/đích."""

        if edge.label in self.edges:
            raise ValueError(f"EdgeType {edge.label} đã tồn tại")
        if edge.src_label not in self.nodes or edge.tgt_label not in self.nodes:
            raise ValueError("EdgeType yêu cầu NodeType nguồn và đích đã được thêm trước")
        self.edges[edge.label] = edge
