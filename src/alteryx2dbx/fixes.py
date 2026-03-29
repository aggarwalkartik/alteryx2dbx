"""Semantic fix registry for post-processing generated PySpark code.

Each fix is a function that takes a code string + context dict and returns
a tuple of (modified_code, was_applied). Fixes are registered in the FIXES
dict and applied via apply_fixes().
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class FixResult:
    """Result of applying all applicable fixes to a code string."""

    code: str
    applied_fixes: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Individual fix functions
# ---------------------------------------------------------------------------


def _fix_case_insensitive_join(code: str, context: dict) -> tuple[str, bool]:
    """Wrap string join key references in F.lower() for case-insensitive matching.

    Applies when tool_type is "Join" and join_fields are present.
    Transforms patterns like df_1["Col"] == df_2["Col"] into
    F.lower(df_1["Col"]) == F.lower(df_2["Col"]).
    """
    if context.get("tool_type") != "Join":
        return code, False

    join_fields = context.get("join_fields")
    if not join_fields:
        return code, False

    modified = code
    applied = False

    for jf in join_fields:
        left_col = jf.get("left", "")
        right_col = jf.get("right", "")

        # Collect all unique column names from join fields
        col_names = set()
        for col in (left_col, right_col):
            if col:
                col_names.add(col)

        for col_name in col_names:
            # Pattern: df_ref["col_name"] at a word boundary, not already wrapped
            # Use (?<!\() to avoid re-wrapping already-wrapped refs
            pattern = re.compile(
                r'(?<!\()(\b\w+\["' + re.escape(col_name) + r'"\])'
            )
            new_code = pattern.sub(r"F.lower(\1)", modified)
            if new_code != modified:
                applied = True
                modified = new_code

    return modified, applied


def _fix_null_safe_equality(code: str, context: dict) -> tuple[str, bool]:
    """Replace F.col("x") == F.lit("y") with F.col("x").eqNullSafe(F.lit("y")).

    Applies when tool_type is "Filter" or "Formula".
    """
    tool_type = context.get("tool_type", "")
    if tool_type not in ("Filter", "Formula"):
        return code, False

    # Match: F.col("...") == F.lit("...")
    pattern = re.compile(
        r'(F\.col\([^)]+\))\s*==\s*(F\.lit\([^)]+\))'
    )

    new_code = pattern.sub(r"\1.eqNullSafe(\2)", code)
    applied = new_code != code
    return new_code, applied


def _fix_numeric_cast(code: str, context: dict) -> tuple[str, bool]:
    """Add explicit .cast(DecimalType(p,s)) for FixedDecimal output fields.

    Scans output_fields for FixedDecimal types and adds cast expressions
    to matching withColumn calls.
    """
    output_fields = context.get("output_fields", [])
    if not output_fields:
        return code, False

    modified = code
    applied = False

    for fld in output_fields:
        if fld.get("type") != "FixedDecimal":
            continue

        name = fld.get("name", "")
        size = fld.get("size", 10)
        scale = fld.get("scale", 0)

        if not name:
            continue

        # Match: F.col("name")) at end of a withColumn call — add .cast()
        # Pattern: F.col("{name}") not already followed by .cast(
        pattern = re.compile(
            r'(F\.col\("' + re.escape(name) + r'"\))(?!\.cast\()'
        )
        cast_expr = rf"\1.cast(DecimalType({size}, {scale}))"
        new_code = pattern.sub(cast_expr, modified)
        if new_code != modified:
            applied = True
            modified = new_code

    return modified, applied


# ---------------------------------------------------------------------------
# Fix registry
# ---------------------------------------------------------------------------

FIXES: dict[str, dict] = {
    "case_insensitive_join": {
        "description": "Wrap string join keys in F.lower() for case-insensitive matching",
        "severity": "warning",
        "fn": _fix_case_insensitive_join,
    },
    "null_safe_equality": {
        "description": "Replace == with eqNullSafe for null-safe comparisons in filters/formulas",
        "severity": "info",
        "fn": _fix_null_safe_equality,
    },
    "numeric_cast": {
        "description": "Add explicit .cast(DecimalType(p,s)) for FixedDecimal output fields",
        "severity": "warning",
        "fn": _fix_numeric_cast,
    },
}


def apply_fixes(code: str, context: dict) -> FixResult:
    """Run all registered fixes against the given code and context.

    Returns a FixResult with the (potentially modified) code and a list of
    dicts describing which fixes were applied.
    """
    result = FixResult(code=code)

    for fix_id, entry in FIXES.items():
        new_code, was_applied = entry["fn"](result.code, context)
        if was_applied:
            result.code = new_code
            result.applied_fixes.append({
                "fix_id": fix_id,
                "description": entry["description"],
                "severity": entry["severity"],
            })

    return result
