from __future__ import annotations
from pathlib import Path
from lark import Lark, Tree

_GRAMMAR_PATH = Path(__file__).parent / "grammar.lark"
_parser: Lark | None = None


def _get_parser() -> Lark:
    global _parser
    if _parser is None:
        _parser = Lark(
            _GRAMMAR_PATH.read_text(),
            parser="earley",
            ambiguity="resolve",
        )
    return _parser


def parse_expression(expr: str) -> Tree:
    return _get_parser().parse(expr)
