from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from graphiti.schema.graph_schema import EdgeType, GraphSchema, NodeType
from graphiti.schema.relational_schema import RelationalSchema, Table
from graphiti.schema.sdt import SDT, SDTRule


@dataclass(slots=True)
class InferResult:
    """Gom nhóm kết quả trả về cho tiện kiểm thử."""

    schema: RelationalSchema
    sdt: SDT


def infer_sdt(gschema: GraphSchema) -> InferResult:
    """Từ GraphSchema sinh ra RelationalSchema và SDT đúng theo bài báo.

    Quy tắc:
    - Mỗi NodeType → bảng riêng, PK = khóa mặc định, cột = danh sách khóa.
    - Mỗi EdgeType → bảng riêng, PK = khóa mặc định, thêm cột SRC/TGT, tạo FK.
    - SDT gồm luật ánh xạ trực tiếp giữa nhãn đồ thị và bảng mới tạo.
    """

    tables = RelationalSchema()
    sdt = SDT()

    # Xử lý nút
    for node in gschema.nodes.values():
        table_name = node.label.lower()
        table = Table(name=table_name, attrs=list(node.keys), pk=node.default_key)
        tables.add_table(table)
        sdt.add_rule(
            SDTRule(
                left_pred=(node.label, list(node.keys)),
                right_pred=(table_name, list(node.keys)),
            )
        )

    # Xử lý cạnh
    for edge in gschema.edges.values():
        table_name = edge.label.lower()
        attrs = list(edge.keys) + ["SRC", "TGT"]
        table = Table(name=table_name, attrs=attrs, pk=edge.default_key)

        # Khóa ngoại tới bảng nguồn và đích
        src_table = tables.get_table(edge.src_label.lower())
        tgt_table = tables.get_table(edge.tgt_label.lower())
        table.fks["SRC"] = (src_table.name, src_table.pk)
        table.fks["TGT"] = (tgt_table.name, tgt_table.pk)

        tables.add_table(table)
        sdt.add_rule(
            SDTRule(
                left_pred=(edge.label, list(edge.keys) + ["SRC", "TGT"]),
                right_pred=(table_name, attrs),
            )
        )

    return InferResult(schema=tables, sdt=sdt)
