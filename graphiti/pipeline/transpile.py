from __future__ import annotations

from typing import Sequence, Set, Tuple

from graphiti.cypher import ast
from graphiti.schema.relational_schema import RelationalSchema, Table
from graphiti.schema.sdt import SDT
from graphiti.sqlir.ir import GroupBy, Join, OrderByIR, Predicate, Project, SQL, SQLExpr, Select, UnionIR, FromTable


# =============================================================================
# Helper builders cho SQLExpr và Predicate
# =============================================================================


def expr_column(alias: str, column: str) -> SQLExpr:
    """alias.column"""

    return SQLExpr("column", (alias, column))


def expr_star() -> SQLExpr:
    """Biểu thức *."""

    return SQLExpr("star", tuple())


def expr_literal(value) -> SQLExpr:
    """Hằng số."""

    kind = "number" if isinstance(value, (int, float)) else "string"
    return SQLExpr(kind, (value,))


def expr_func(name: str, args: Sequence[SQLExpr]) -> SQLExpr:
    """Hàm (bao gồm cả aggregate)."""

    return SQLExpr("func", (name.upper(),) + tuple(args))


def pred_cmp(op: str, left: SQLExpr, right: SQLExpr) -> Predicate:
    return Predicate("cmp", (op, left, right))


def pred_and(left: Predicate, right: Predicate) -> Predicate:
    return Predicate("and", (left, right))


def pred_or(left: Predicate, right: Predicate) -> Predicate:
    return Predicate("or", (left, right))


def pred_not(sub: Predicate) -> Predicate:
    return Predicate("not", (sub,))


# =============================================================================
# Transpile chính
# =============================================================================


def transpile_query(query: ast.Query, sdt: SDT, rschema: RelationalSchema) -> SQL:
    """Dịch Query Cypher sang SQL IR."""

    _ = sdt  # SDT chưa sử dụng trực tiếp trong phiên bản tối giản

    if isinstance(query, ast.ReturnQuery):
        _, base = transpile_clause(query.clause, sdt, rschema)
        sql_exprs = [to_sql_expr(expr) for expr in query.exprs]
        items = list(zip(query.names, sql_exprs))

        if any(is_agg_expr(expr) for expr in query.exprs):
            keys = [to_sql_expr(expr) for expr in query.exprs if not is_agg_expr(expr)]
            # Loại bỏ trùng lặp trong keys theo chuỗi biểu diễn
            dedup = []
            seen = set()
            for key in keys:
                key_repr = repr(key)
                if key_repr not in seen:
                    dedup.append(key)
                    seen.add(key_repr)
            return GroupBy(sub=base, keys=dedup, items=items)
        return Project(sub=base, items=items)

    if isinstance(query, ast.OrderBy):
        sub = transpile_query(query.sub, sdt, rschema)
        return OrderByIR(sub=sub, key=to_sql_expr(query.key), asc=query.asc)

    if isinstance(query, ast.UnionQuery):
        left = transpile_query(query.left, sdt, rschema)
        right = transpile_query(query.right, sdt, rschema)
        return UnionIR(left=left, right=right, all=query.all)

    raise TypeError(f"Không hỗ trợ query {query!r}")


def transpile_clause(clause: ast.Clause, sdt: SDT, rschema: RelationalSchema) -> Tuple[Set[str], SQL]:
    """Dịch Clause sang SQL IR và trả về tập biến khả dụng."""

    if isinstance(clause, ast.ClauseMatch):
        vars_available, rel = transpile_pattern(clause.pattern, rschema, outer=False)
        if clause.where:
            rel = Select(sub=rel, pred=to_sql_pred(clause.where))
        return vars_available, rel

    if isinstance(clause, ast.ClauseOptMatch):
        vars_available, rel = transpile_pattern(clause.pattern, rschema, outer=True)
        if clause.where:
            rel = Select(sub=rel, pred=to_sql_pred(clause.where))
        return vars_available, rel

    if isinstance(clause, ast.ClauseWith):
        raise NotImplementedError("Clause WITH chưa được hỗ trợ trong bản tối giản")

    raise TypeError(f"Không hỗ trợ clause {clause!r}")


def transpile_pattern(pattern: ast.PathPat, rschema: RelationalSchema, *, outer: bool) -> Tuple[Set[str], SQL]:
    """Dịch PathPat thành chuỗi JOIN."""

    items = list(pattern.items)
    if not items or not isinstance(items[0], ast.NodePat):
        raise ValueError("Pattern phải bắt đầu bằng NodePat")

    node0: ast.NodePat = items[0]
    table0 = _resolve_node_table(node0, rschema)
    rel: SQL = FromTable(table=table0.name, alias=node0.var)
    vars_available: Set[str] = {node0.var}
    prev_node = node0
    prev_table = table0

    idx = 1
    while idx < len(items):
        edge: ast.EdgePat = items[idx]  # type: ignore[index]
        node: ast.NodePat = items[idx + 1]  # type: ignore[index]
        edge_table = _resolve_edge_table(edge, rschema)
        node_table = _resolve_node_table(node, rschema)
        join_kind = "left" if outer else "inner"

        # Nối node trước với bảng cạnh theo hướng
        if edge.direction == "<-":
            left_expr = expr_column(prev_node.var, prev_table.pk)
            right_expr = expr_column(edge.var, "TGT")
        else:
            left_expr = expr_column(prev_node.var, prev_table.pk)
            right_expr = expr_column(edge.var, "SRC")
        rel = Join(left=rel, right=FromTable(table=edge_table.name, alias=edge.var), on=pred_cmp("=", left_expr, right_expr), kind=join_kind)
        vars_available.add(edge.var)

        # Nối cạnh với node kế tiếp
        if edge.direction == "<-":
            left_expr = expr_column(edge.var, "SRC")
        else:
            left_expr = expr_column(edge.var, "TGT")
        right_expr = expr_column(node.var, node_table.pk)
        rel = Join(left=rel, right=FromTable(table=node_table.name, alias=node.var), on=pred_cmp("=", left_expr, right_expr), kind=join_kind)
        vars_available.add(node.var)
        prev_node = node
        prev_table = node_table
        idx += 2

    return vars_available, rel


def to_sql_expr(expr: ast.Expr) -> SQLExpr:
    """Chuyển biểu thức Cypher sang SQLExpr."""

    if isinstance(expr, ast.ExprProp):
        return expr_column(expr.var, expr.key)
    if isinstance(expr, ast.ExprAgg):
        inner = to_sql_expr(expr.expr)
        return expr_func(expr.fn, [inner])
    if isinstance(expr, ast.ExprVar):
        raise NotImplementedError("ExprVar chưa được hỗ trợ trong bản transpiler tối giản")
    if isinstance(expr, (int, float)):
        return expr_literal(expr)
    if isinstance(expr, str):
        if expr == "*":
            return expr_star()
        return expr_literal(expr)
    raise TypeError(f"Không hỗ trợ biểu thức {expr!r}")


def to_sql_pred(predicate: ast.Predicate) -> Predicate:
    """Chuyển predicate của Cypher sang Predicate SQL."""

    if isinstance(predicate, ast.PredicateCompare):
        left = to_sql_expr(predicate.left)
        right = to_sql_expr(predicate.right)
        return pred_cmp(predicate.op, left, right)
    if isinstance(predicate, ast.PredicateAnd):
        return pred_and(to_sql_pred(predicate.left), to_sql_pred(predicate.right))
    if isinstance(predicate, ast.PredicateOr):
        return pred_or(to_sql_pred(predicate.left), to_sql_pred(predicate.right))
    if isinstance(predicate, ast.PredicateNot):
        return pred_not(to_sql_pred(predicate.sub))
    raise TypeError(f"Không hỗ trợ predicate {predicate!r}")


def is_agg_expr(expr: ast.Expr) -> bool:
    return isinstance(expr, ast.ExprAgg)


def _resolve_node_table(node: ast.NodePat, rschema: RelationalSchema) -> Table:
    try:
        return rschema.get_table(node.label.lower())
    except KeyError as exc:
        raise KeyError(f"Không tìm thấy bảng cho NodeType {node.label}") from exc


def _resolve_edge_table(edge: ast.EdgePat, rschema: RelationalSchema) -> Table:
    try:
        return rschema.get_table(edge.label.lower())
    except KeyError as exc:
        raise KeyError(f"Không tìm thấy bảng cho EdgeType {edge.label}") from exc
