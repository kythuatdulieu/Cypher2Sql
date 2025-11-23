from __future__ import annotations

import re
from typing import List

from graphiti.cypher import ast


MATCH_PREFIX = "MATCH"
OPTIONAL_PREFIX = "OPTIONAL MATCH"

RETURN_REGEX = re.compile(r"\bRETURN\b", re.IGNORECASE)
ORDER_BY_REGEX = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)
WHERE_REGEX = re.compile(r"\bWHERE\b", re.IGNORECASE)


class ParseError(ValueError):
    """Ngoại lệ khi gặp cú pháp ngoài phạm vi hỗ trợ."""


def parse_query(text: str) -> ast.Query:
    """Phân tích chuỗi Cypher (giới hạn) thành AST."""

    text = text.strip()
    if not text:
        raise ParseError("Chuỗi query rỗng")

    order_part = None
    order_match = ORDER_BY_REGEX.search(text)
    if order_match:
        order_part = text[order_match.end() :].strip()
        text = text[: order_match.start()].strip()

    return_match = RETURN_REGEX.search(text)
    if not return_match:
        raise ParseError("Thiếu RETURN")

    match_part = text[: return_match.start()]
    return_part = text[return_match.end() :]

    clause = _parse_match(match_part)
    exprs, names = _parse_return_list(return_part)
    query: ast.Query = ast.ReturnQuery(clause=clause, exprs=exprs, names=names)

    if order_part:
        tokens = order_part.split()
        asc = True
        if tokens[-1].upper() in {"ASC", "DESC"}:
            asc = tokens[-1].upper() == "ASC"
            expr_text = " ".join(tokens[:-1])
        else:
            expr_text = order_part
        order_expr = _parse_expr(expr_text)
        query = ast.OrderBy(sub=query, key=order_expr, asc=asc)

    return query


def _parse_match(text: str) -> ast.Clause:
    upper = text.upper()
    if upper.startswith(OPTIONAL_PREFIX):
        pattern_text = text[len(OPTIONAL_PREFIX) :].lstrip()
        clause_cls = ast.ClauseOptMatch
    elif upper.startswith(MATCH_PREFIX):
        pattern_text = text[len(MATCH_PREFIX) :].lstrip()
        clause_cls = ast.ClauseMatch
    else:
        raise ParseError("Query phải bắt đầu bằng MATCH hoặc OPTIONAL MATCH")

    where_text = None
    where_match = WHERE_REGEX.search(pattern_text)
    if where_match:
        where_text = pattern_text[where_match.end() :].strip()
        pattern_text = pattern_text[: where_match.start()].strip()

    pattern = _parse_pattern(pattern_text)
    predicate = _parse_predicate(where_text) if where_text else None
    return clause_cls(pattern=pattern, where=predicate)


NODE_RE = re.compile(r"\(\s*(?P<var>[A-Za-z_][A-Za-z0-9_]*)\s*:\s*(?P<label>[A-Za-z_][A-Za-z0-9_]*)\s*\)")
EDGE_FORWARD_RE = re.compile(r"-\[\s*(?P<var>[A-Za-z_][A-Za-z0-9_]*)\s*:\s*(?P<label>[A-Za-z_][A-Za-z0-9_]*)\s*\]->")
EDGE_BACKWARD_RE = re.compile(r"<-\[\s*(?P<var>[A-Za-z_][A-Za-z0-9_]*)\s*:\s*(?P<label>[A-Za-z_][A-Za-z0-9_]*)\s*\]-")
EDGE_UNDIRECT_RE = re.compile(r"-\[\s*(?P<var>[A-Za-z_][A-Za-z0-9_]*)\s*:\s*(?P<label>[A-Za-z_][A-Za-z0-9_]*)\s*\]-")


def _parse_pattern(text: str) -> ast.PathPat:
    items: List[ast.NodePat | ast.EdgePat] = []
    idx = 0
    length = len(text)
    while idx < length:
        if text[idx].isspace():
            idx += 1
            continue
        node_match = NODE_RE.match(text, idx)
        if node_match:
            items.append(ast.NodePat(var=node_match.group("var"), label=node_match.group("label")))
            idx = node_match.end()
            continue
        for regex, direction in (
            (EDGE_FORWARD_RE, "->"),
            (EDGE_BACKWARD_RE, "<-"),
            (EDGE_UNDIRECT_RE, "--"),
        ):
            edge_match = regex.match(text, idx)
            if edge_match:
                items.append(
                    ast.EdgePat(
                        var=edge_match.group("var"),
                        label=edge_match.group("label"),
                        direction=direction,
                    )
                )
                idx = edge_match.end()
                break
        else:  # no break
            raise ParseError(f"Không đọc được pattern tại vị trí {idx}: {text[idx:idx+20]}")
    if not items:
        raise ParseError("Pattern rỗng")
    return ast.PathPat(items=items)


def _parse_predicate(text: str | None) -> ast.Predicate | None:
    if not text:
        return None
    # Chỉ hỗ trợ dạng đơn giản: expr OP expr với OP ∈ {=, <>, <, >}
    upper = text.upper()
    for op in (" AND ", " OR "):
        idx = upper.find(op)
        if idx != -1:
            left = _parse_predicate(text[:idx])
            right = _parse_predicate(text[idx + len(op) :])
            if left is None or right is None:
                raise ParseError("Predicate thiếu vế")
            if op.strip() == "AND":
                return ast.PredicateAnd(left=left, right=right)
            return ast.PredicateOr(left=left, right=right)
    if upper.startswith("NOT "):
        sub = _parse_predicate(text[4:])
        if sub is None:
            raise ParseError("NOT thiếu toán hạng")
        return ast.PredicateNot(sub=sub)
    # So sánh đơn
    for op in ("<=", ">=", "<>", "=", "<", ">"):
        if op in text:
            left, right = text.split(op, 1)
            return ast.PredicateCompare(left=_parse_expr(left.strip()), op=op, right=_parse_expr(right.strip()))
    raise ParseError("Predicate không hỗ trợ")


def _parse_return_list(text: str) -> tuple[list[ast.Expr], list[str]]:
    parts = [part.strip() for part in text.split(",") if part.strip()]
    exprs: list[ast.Expr] = []
    names: list[str] = []
    for part in parts:
        upper = part.upper()
        if " AS " not in upper:
            raise ParseError("Mỗi biểu thức RETURN cần dạng 'expr AS alias'")
        as_idx = upper.rfind(" AS ")
        expr_text = part[:as_idx]
        alias = part[as_idx + 4 :].strip()
        exprs.append(_parse_expr(expr_text.strip()))
        names.append(alias)
    return exprs, names


def _parse_expr(text: str) -> ast.Expr:
    text = text.strip()
    if not text:
        raise ParseError("Biểu thức rỗng")
    fn_match = re.match(r"(?P<fn>[A-Z]+)\((?P<body>.+)\)", text)
    if fn_match:
        fn = fn_match.group("fn")
        body = fn_match.group("body").strip()
        return ast.ExprAgg(fn=fn, expr=_parse_expr(body))
    if text == "*":
        return "*"
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    if text.isdigit():
        return int(text)
    if "." in text:
        var, key = text.split(".", 1)
        return ast.ExprProp(var=var, key=key)
    raise ParseError(f"Biểu thức '{text}' chưa được hỗ trợ (cần var.prop hoặc hằng)")
