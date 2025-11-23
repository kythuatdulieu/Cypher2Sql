import sqlite3
import unittest

from graphiti.cypher import parser_fw
from graphiti.pipeline.infer_sdt import infer_sdt
from graphiti.pipeline.reduce_sql import execute_sql, table_equivalent
from graphiti.pipeline.transpile import transpile_query
from graphiti.schema.graph_schema import EdgeType, GraphSchema, NodeType
from graphiti.sqlir.printer import to_sql


def build_schema() -> GraphSchema:
    schema = GraphSchema()
    schema.add_node(NodeType(label="Person", keys=["pid", "name"]))
    schema.add_node(NodeType(label="Company", keys=["cid", "title"]))
    schema.add_edge(EdgeType(label="WORKS_AT", src_label="Person", tgt_label="Company", keys=["wid"]))
    return schema


class GraphitiAlgorithm1Tests(unittest.TestCase):
    def test_infer_sdt_basic(self) -> None:
        infer = infer_sdt(build_schema())
        tables = infer.schema.tables
        self.assertSetEqual(set(tables), {"person", "company", "works_at"})
        self.assertEqual(tables["person"].pk, "pid")
        self.assertEqual(tables["works_at"].attrs, ["wid", "SRC", "TGT"])
        self.assertEqual(tables["works_at"].fks["SRC"], ("person", "pid"))
        self.assertEqual(tables["works_at"].fks["TGT"], ("company", "cid"))

    def test_transpile_match_join(self) -> None:
        schema = build_schema()
        infer = infer_sdt(schema)
        cypher = "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) RETURN p.pid AS pid, c.cid AS cid"
        query = parser_fw.parse_query(cypher)
        sql_ir = transpile_query(query, infer.sdt, infer.schema)
        sql_text = to_sql(sql_ir)
        self.assertIn("INNER JOIN", sql_text)
        self.assertIn("SELECT p.pid AS pid", sql_text)
        self.assertIn("c.cid AS cid", sql_text)

    def test_table_equivalent_column_permutation(self) -> None:
        rows1 = [(1, "A"), (2, "B"), (2, "B")]
        rows2 = [("A", 1), ("B", 2), ("B", 2)]
        self.assertTrue(table_equivalent(rows1, rows2))

    def test_end_to_end_equivalence(self) -> None:
        schema = build_schema()
        infer = infer_sdt(schema)
        cypher = "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) RETURN p.pid AS pid, c.cid AS cid"
        query = parser_fw.parse_query(cypher)
        sql_ir = transpile_query(query, infer.sdt, infer.schema)
        sql_text = to_sql(sql_ir)

        target_sql = (
            "SELECT p.pid, c.cid "
            "FROM person AS p "
            "INNER JOIN works_at AS w ON p.pid = w.SRC "
            "INNER JOIN company AS c ON w.TGT = c.cid"
        )

        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE person (pid INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE company (cid INTEGER PRIMARY KEY, title TEXT);
            CREATE TABLE works_at (wid INTEGER PRIMARY KEY, SRC INTEGER, TGT INTEGER);
            INSERT INTO person(pid, name) VALUES (1,'Ann'), (2,'Bob');
            INSERT INTO company(cid, title) VALUES (10,'ACME');
            INSERT INTO works_at(wid, SRC, TGT) VALUES (100,1,10), (101,2,10);
            """
        )

        try:
            rows_transpiled = execute_sql(conn, sql_text)
            rows_target = execute_sql(conn, target_sql)
        finally:
            conn.close()

        self.assertTrue(table_equivalent(rows_transpiled, rows_target))


if __name__ == "__main__":
    unittest.main()
