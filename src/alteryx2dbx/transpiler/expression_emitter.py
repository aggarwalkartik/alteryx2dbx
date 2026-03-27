"""PySpark expression emitter — transforms Lark parse trees to PySpark column expressions."""
from __future__ import annotations

import re
from lark import Transformer, Token, Tree

from alteryx2dbx.transpiler.expression_parser import parse_expression


# Direct function mappings: Alteryx name (lowered) → PySpark function
_DIRECT_MAP: dict[str, str] = {
    "trim": "F.trim",
    "trimleft": "F.ltrim",
    "trimright": "F.rtrim",
    "length": "F.length",
    "uppercase": "F.upper",
    "lowercase": "F.lower",
    "round": "F.round",
    "ceil": "F.ceil",
    "floor": "F.floor",
    "abs": "F.abs",
    "pow": "F.pow",
    "sqrt": "F.sqrt",
    "min": "F.least",
    "max": "F.greatest",
    "padleft": "F.lpad",
    "padright": "F.rpad",
    "regex_replace": "F.regexp_replace",
    "replacechar": "F.translate",
}


class PySparkEmitter(Transformer):
    """Walk Lark parse tree and emit PySpark column expression strings."""

    # ── Atoms ──────────────────────────────────────────────────

    def start(self, children):
        return children[0]

    def number(self, children):
        token = children[0]
        val = str(token)
        if "." in val:
            return f"F.lit({val})"
        return f"F.lit({val})"

    def string(self, children):
        token = str(children[0])
        # Normalize to double-quoted
        inner = token[1:-1]  # strip quotes
        return f'F.lit("{inner}")'

    def boolean(self, children):
        token = str(children[0]).lower()
        val = "True" if token == "true" else "False"
        return f"F.lit({val})"

    def null_func(self, children):
        return "F.lit(None)"

    def field_ref(self, children):
        token = str(children[0])
        name = token[1:-1]  # strip [ ]
        return f'F.col("{name}")'

    def row_ref(self, children):
        token = str(children[0])
        # [Row-1:Revenue] → extract offset and field
        m = re.match(r"\[Row-(\d+):(.+)\]", token)
        if m:
            offset = m.group(1)
            field = m.group(2)
            return f'F.lag(F.col("{field}"), {offset})'
        return f"# TODO: unparsed row_ref {token}"

    # ── Arithmetic ─────────────────────────────────────────────

    def add(self, children):
        return f"({children[0]} + {children[1]})"

    def sub(self, children):
        return f"({children[0]} - {children[1]})"

    def mul(self, children):
        return f"({children[0]} * {children[1]})"

    def div(self, children):
        return f"({children[0]} / {children[1]})"

    def mod(self, children):
        return f"({children[0]} % {children[1]})"

    def neg(self, children):
        return f"(-{children[0]})"

    # ── Comparison ─────────────────────────────────────────────

    def _is_string_lit(self, expr: str) -> bool:
        return expr.startswith('F.lit("') and expr.endswith('")')

    def _extract_string_val(self, expr: str) -> str:
        # F.lit("Active") → Active
        m = re.match(r'^F\.lit\("(.*)"\)$', expr)
        return m.group(1) if m else ""

    def eq(self, children):
        left, _op, right = children[0], children[1], children[2]
        # Case-insensitive string comparison
        if self._is_string_lit(right):
            val = self._extract_string_val(right)
            return f'(F.lower({left}) == F.lit("{val.lower()}"))'
        if self._is_string_lit(left):
            val = self._extract_string_val(left)
            return f'(F.lit("{val.lower()}") == F.lower({right}))'
        # NULL-safe equality
        return f"({left}.eqNullSafe({right}))"

    def neq(self, children):
        left, _op, right = children[0], children[1], children[2]
        if self._is_string_lit(right):
            val = self._extract_string_val(right)
            return f'(F.lower({left}) != F.lit("{val.lower()}"))'
        if self._is_string_lit(left):
            val = self._extract_string_val(left)
            return f'(F.lit("{val.lower()}") != F.lower({right}))'
        return f"({left} != {right})"

    def gt(self, children):
        return f"({children[0]} > {children[2]})"

    def gte(self, children):
        return f"({children[0]} >= {children[2]})"

    def lt(self, children):
        return f"({children[0]} < {children[2]})"

    def lte(self, children):
        return f"({children[0]} <= {children[2]})"

    # ── Logical ────────────────────────────────────────────────

    def or_expr(self, children):
        # children: [left, OR_token, right]
        return f"({children[0]} | {children[2]})"

    def and_expr(self, children):
        return f"({children[0]} & {children[2]})"

    def not_expr(self, children):
        # children: [NOT_token, expr]
        return f"(~{children[1]})"

    # ── IF / ELSEIF / ELSE ────────────────────────────────────

    def if_expr(self, children):
        # children: IF, cond, THEN, then_val, *[elseif_clause...], ELSE, else_val, ENDIF
        # Filter out keyword tokens
        parts = []
        for c in children:
            if isinstance(c, Token):
                continue
            parts.append(c)

        # parts[0] = condition, parts[1] = then_val, ..., last = else_val
        # elseif clauses are tuples (cond, val)
        cond = parts[0]
        then_val = parts[1]
        elseifs = []
        else_val = parts[-1]

        for p in parts[2:-1]:
            # elseif_clause returns a tuple
            if isinstance(p, tuple):
                elseifs.append(p)

        result = f"F.when({cond}, {then_val})"
        for ei_cond, ei_val in elseifs:
            result += f".when({ei_cond}, {ei_val})"
        result += f".otherwise({else_val})"
        return result

    def elseif_clause(self, children):
        # children: ELSEIF_token, cond, THEN_token, val
        parts = [c for c in children if not isinstance(c, Token)]
        return (parts[0], parts[1])

    # ── Function calls ─────────────────────────────────────────

    def func_call(self, children):
        func_name = str(children[0])
        args = children[1] if len(children) > 1 else []
        if not isinstance(args, list):
            args = []

        func_lower = func_name.lower()

        # Special-case functions
        if func_lower == "isnull":
            return f"{args[0]}.isNull()"
        if func_lower == "isempty":
            return f'({args[0]} == F.lit(""))'
        if func_lower == "iif":
            cond, t, f = args[0], args[1], args[2]
            return f"F.when({cond}, {t}).otherwise({f})"
        if func_lower == "contains":
            col, val = args[0], args[1]
            inner_val = self._extract_string_val(val)
            return f'F.lower({col}).contains("{inner_val.lower()}")'
        if func_lower == "startswith":
            return f"{args[0]}.startswith({args[1]})"
        if func_lower == "endswith":
            return f"{args[0]}.endswith({args[1]})"
        if func_lower == "left":
            return f"F.substring({args[0]}, 1, {args[1]})"
        if func_lower == "right":
            return f"F.substring({args[0]}, -{args[1]}, {args[1]})"
        if func_lower == "substring":
            # 0-based → 1-based: offset start by +1
            start_expr = args[1]
            # Try to evaluate numeric start
            m = re.match(r"F\.lit\((\d+)\)", start_expr)
            if m:
                start_val = int(m.group(1)) + 1
                return f"F.substring({args[0]}, {start_val}, {args[2]})"
            return f"F.substring({args[0]}, ({start_expr} + F.lit(1)), {args[2]})"
        if func_lower == "findstring":
            return f"(F.locate({args[1]}, {args[0]}) - 1)"
        if func_lower == "tostring":
            return f'{args[0]}.cast("string")'
        if func_lower == "tonumber":
            return f'{args[0]}.cast("double")'
        if func_lower == "regex_match":
            return f"{args[0]}.rlike({args[1]})"
        if func_lower == "getword":
            return f'F.split({args[0]}, " ").getItem({args[1]})'
        if func_lower == "datetimenow":
            return "F.current_timestamp()"

        # Direct-mapped functions
        if func_lower in _DIRECT_MAP:
            pyspark_func = _DIRECT_MAP[func_lower]
            args_str = ", ".join(args)
            return f"{pyspark_func}({args_str})"

        # Unknown function
        args_str = ", ".join(args)
        return f"# TODO: unmapped function {func_name}({args_str})"

    def func_args(self, children):
        return list(children)


def transpile_expression(expr: str) -> str:
    """Parse Alteryx expression and return PySpark column expression string."""
    tree = parse_expression(expr)
    emitter = PySparkEmitter()
    return emitter.transform(tree)
