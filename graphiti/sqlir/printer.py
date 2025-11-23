from __future__ import annotations

from graphiti.sqlir import ir


def to_sql(node: ir.SQL) -> str:
    """Xuất SQL dạng chuỗi với cú pháp PostgreSQL cơ bản."""

    if isinstance(node, ir.Project):
        select_list = ", ".join(f"{_sql_expr(expr)} AS {alias}" for alias, expr in node.items)
        sub = node.sub
        if isinstance(sub, ir.Select):
            from_part = _from_clause(sub.sub)
            return f"SELECT {select_list} FROM {from_part} WHERE {_predicate(sub.pred)}"
        from_part = _from_clause(sub, alias="_proj")
        return f"SELECT {select_list} FROM {from_part}"

    if isinstance(node, ir.GroupBy):
        select_list = ", ".join(f"{_sql_expr(expr)} AS {alias}" for alias, expr in node.items)
        sub = node.sub
        where_clause = ""
        if isinstance(sub, ir.Select):
            from_part = _from_clause(sub.sub)
            where_clause = f" WHERE {_predicate(sub.pred)}"
        else:
            from_part = _from_clause(sub, alias="_grp")
        sql = f"SELECT {select_list} FROM {from_part}{where_clause}"
        if node.keys:
            group_keys = ", ".join(_sql_expr(expr) for expr in node.keys)
            sql += f" GROUP BY {group_keys}"
        if node.having:
            sql += f" HAVING {_predicate(node.having)}"
        return sql

    if isinstance(node, ir.Select):
        from_part = _from_clause(node.sub, alias="_sel")
        return f"SELECT * FROM {from_part} WHERE {_predicate(node.pred)}"

    if isinstance(node, ir.UnionIR):
        op = "UNION ALL" if node.all else "UNION"
        left = _wrap_union(node.left)
        right = _wrap_union(node.right)
        return f"{left} {op} {right}"

    if isinstance(node, ir.OrderByIR):
        direction = "ASC" if node.asc else "DESC"
        sub = to_sql(node.sub)
        return f"SELECT * FROM ({sub}) AS _ord ORDER BY {_sql_expr(node.key)} {direction}"

    if isinstance(node, ir.WithCTE):
        return f"WITH {node.name} AS ({to_sql(node.sub)}) {to_sql(node.body)}"

    if isinstance(node, ir.Join):
        # Chủ yếu dùng như FROM; fallback sản xuất SELECT *
        from_part = _from_clause(node)
        return f"SELECT * FROM {from_part}"

    if isinstance(node, ir.FromTable):
        return f"SELECT * FROM {node.table} AS {node.alias}"

    raise TypeError(f"Không biết in SQL cho {node!r}")


def _wrap_union(node: ir.SQL) -> str:
    sql = to_sql(node)
    return f"({sql})" if not isinstance(node, ir.UnionIR) else sql


def _from_clause(node: ir.SQL, alias: str | None = None) -> str:
    if isinstance(node, ir.FromTable):
        return f"{node.table} AS {node.alias}"
    if isinstance(node, ir.Join):
        left = _wrap_from(node.left)
        right = _wrap_from(node.right)
        join_kw = "LEFT JOIN" if node.kind == "left" else "INNER JOIN"
        return f"{left} {join_kw} {right} ON {_predicate(node.on)}"
    sub_alias = alias or "_sub"
    return f"({to_sql(node)}) AS {sub_alias}"


def _wrap_from(node: ir.SQL) -> str:
    if isinstance(node, ir.FromTable):
        return _from_clause(node)
    if isinstance(node, ir.Join):
        return f"({_from_clause(node)})"
    return _from_clause(node, alias="_wrap")


def _sql_expr(expr: ir.SQLExpr) -> str:
    kind = expr.kind
    if kind == "column":
        alias, column = expr.args  # type: ignore[misc]
        return f"{alias}.{column}"
    if kind == "star":
        return "*"
    if kind == "number":
        value, = expr.args
        return str(value)
    if kind == "string":
        value, = expr.args
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"
    if kind == "func":
        name = expr.args[0]
        args = ", ".join(_sql_expr(arg) for arg in expr.args[1:])
        return f"{name}({args})"
    if kind == "alias":
        alias, = expr.args
        return f"{alias}"
    raise TypeError(f"Không biết in SQLExpr {expr!r}")


def _predicate(pred: ir.Predicate) -> str:
    kind = pred.kind
    if kind == "cmp":
        op, left, right = pred.args  # type: ignore[misc]
        return f"{_sql_expr(left)} {op} {_sql_expr(right)}"
    if kind == "and":
        left, right = pred.args  # type: ignore[misc]
        return f"({_predicate(left)} AND {_predicate(right)})"
    if kind == "or":
        left, right = pred.args  # type: ignore[misc]
        return f"({_predicate(left)} OR {_predicate(right)})"
    if kind == "not":
        (sub,) = pred.args  # type: ignore[misc]
        return f"NOT({_predicate(sub)})"
    raise TypeError(f"Không biết in Predicate {pred!r}")


__all__ = ["to_sql"]
