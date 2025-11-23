from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from graphiti.cypher import parser_fw
from graphiti.pipeline.infer_sdt import infer_sdt
from graphiti.pipeline.reduce_sql import execute_sql, table_equivalent
from graphiti.pipeline.transpile import transpile_query
from graphiti.schema.graph_schema import EdgeType, GraphSchema, NodeType
from graphiti.sqlir.printer import to_sql


def main() -> None:
    parser = argparse.ArgumentParser(description="Chạy Algorithm 1 (InferSDT → Transpile → ReduceToSQL)")
    parser.add_argument("--schema", required=True, help="Đường dẫn JSON mô tả lược đồ đồ thị")
    parser.add_argument("--cypher", required=True, help="File chứa query Cypher (Featherweight)")
    parser.add_argument("--target", help="File chứa SQL đích để so sánh")
    parser.add_argument("--db", help="SQLite DB để chạy ReduceToSQL")
    args = parser.parse_args()

    gschema = _load_schema(Path(args.schema))
    cypher_text = Path(args.cypher).read_text(encoding="utf-8")
    cypher_query = parser_fw.parse_query(cypher_text)

    infer = infer_sdt(gschema)
    sql_ir = transpile_query(cypher_query, infer.sdt, infer.schema)
    sql_text = to_sql(sql_ir)
    print("--- SQL sinh ra ---")
    print(sql_text)

    if args.target and args.db:
        target_sql = Path(args.target).read_text(encoding="utf-8")
        conn = sqlite3.connect(args.db)
        try:
            rows_transpiled = execute_sql(conn, sql_text)
            rows_target = execute_sql(conn, target_sql)
        finally:
            conn.close()
        equi = table_equivalent(rows_transpiled, rows_target)
        print("--- So sánh bảng ---")
        print("TƯƠNG ĐƯƠNG" if equi else "KHÁC NHAU")
    elif args.target or args.db:
        print("Cần cung cấp cả --target và --db để so sánh bảng")


def _load_schema(path: Path) -> GraphSchema:
    data = json.loads(path.read_text(encoding="utf-8"))
    nodes_raw = data.get("nodes", [])
    edges_raw = data.get("edges", [])
    schema = GraphSchema()
    for node in nodes_raw:
        schema.add_node(NodeType(label=node["label"], keys=node["keys"]))
    for edge in edges_raw:
        schema.add_edge(
            EdgeType(
                label=edge["label"],
                src_label=edge["src"],
                tgt_label=edge["tgt"],
                keys=edge["keys"],
            )
        )
    return schema


if __name__ == "__main__":
    main()
