"""Generate 04_validate.py — smart validation with auto-detected join keys."""
from __future__ import annotations

import re
from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow, GeneratedStep

# Heuristic suffixes that suggest a column is a key/identifier.
_KEY_SUFFIXES = ("_id", "_key", "_pk", "_code")
_KEY_EXACT = ("id",)

# Alteryx field types considered numeric for aggregate checks.
_NUMERIC_TYPES = re.compile(r"(?i)(int|float|double|decimal|numeric|byte|fixed\s*decimal)")


def detect_join_keys(workflow: AlteryxWorkflow) -> list[str]:
    """Auto-detect likely join/primary-key columns from a workflow.

    Priority order:
    1. Join tool configs  → ``config["join_fields"]`` left field names
    2. Unique tool configs → ``config["unique_fields"]``
    3. Heuristic fallback  → output_fields ending in ``_id``, ``_key``, ``_pk``, ``_code``, or exactly ``id``

    Returns a deduplicated list preserving discovery order.
    """
    seen: set[str] = set()
    keys: list[str] = []

    def _add(name: str) -> None:
        if name not in seen:
            seen.add(name)
            keys.append(name)

    # 1. Join tool configs
    for tool in workflow.tools.values():
        if tool.tool_type == "Join":
            for jf in tool.config.get("join_fields", []):
                left = jf.get("left")
                if left:
                    _add(left)

    # 2. Unique tool configs
    for tool in workflow.tools.values():
        if tool.tool_type == "Unique":
            for uf in tool.config.get("unique_fields", []):
                _add(uf)

    # 3. Heuristic fallback — scan output_fields across all tools
    for tool in workflow.tools.values():
        for field in tool.output_fields:
            name_lower = field.name.lower()
            if any(name_lower.endswith(suffix) for suffix in _KEY_SUFFIXES):
                _add(field.name)
            elif name_lower in _KEY_EXACT:
                _add(field.name)

    return keys


def _collect_numeric_fields(workflow: AlteryxWorkflow) -> list[str]:
    """Return deduplicated list of numeric output field names across all tools."""
    seen: set[str] = set()
    result: list[str] = []
    for tool in workflow.tools.values():
        for field in tool.output_fields:
            if field.name not in seen and _NUMERIC_TYPES.search(field.type):
                seen.add(field.name)
                result.append(field.name)
    return result


def generate_validator_v2(
    output_dir: Path,
    workflow: AlteryxWorkflow,
    steps: dict[int, GeneratedStep],
    execution_order: list[int],
) -> None:
    """Write ``04_validate.py`` — a Databricks notebook with 5 validation sections.

    Sections:
    1. Load Alteryx baseline
    2. Row Count Comparison
    3. Schema Comparison
    4. Aggregate Checks (sum/min/max on numeric columns)
    5. Row-Level Comparison with DataComPy
    """
    # Determine the last output df name
    last_output_df = "df_result"
    if execution_order:
        last_id = execution_order[-1]
        if last_id in steps and steps[last_id].output_df:
            last_output_df = steps[last_id].output_df

    # Detect keys
    detected_keys = detect_join_keys(workflow)
    join_columns = detected_keys if detected_keys else ["TODO_primary_key"]
    join_columns_str = ", ".join(f'"{k}"' for k in join_columns)

    # Detect numeric fields for aggregate checks
    numeric_fields = _collect_numeric_fields(workflow)

    lines: list[str] = []

    def _cell(text: str) -> None:
        lines.append("")
        lines.append("# COMMAND ----------")
        lines.append("")
        lines.append(text)

    # Header
    lines.append("# Databricks notebook source")
    lines.append("# 04 — Validation: Compare Alteryx baseline vs Databricks output")

    # ── Section 1: Load Alteryx baseline ──
    _cell(
        '# SECTION 1: Load Alteryx baseline\n'
        '# TODO: Update the path to the Alteryx output file\n'
        'alteryx_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(\n'
        '    "alteryx_output/<ALTERYX_BASELINE_FILE>"  # TODO: replace with actual filename\n'
        ')'
    )

    # ── Section 2: Row Count Comparison ──
    _cell(
        f'# SECTION 2: Row Count Comparison\n'
        f'alteryx_count = alteryx_df.count()\n'
        f'databricks_count = {last_output_df}.count()\n'
        f'print(f"Alteryx row count:    {{alteryx_count}}")\n'
        f'print(f"Databricks row count: {{databricks_count}}")\n'
        f'assert alteryx_count == databricks_count, (\n'
        f'    f"Row count mismatch: Alteryx={{alteryx_count}} vs Databricks={{databricks_count}}"\n'
        f')'
    )

    # ── Section 3: Schema Comparison ──
    _cell(
        f'# SECTION 3: Schema Comparison\n'
        f'alteryx_cols = set(alteryx_df.columns)\n'
        f'databricks_cols = set({last_output_df}.columns)\n'
        f'only_in_alteryx = alteryx_cols - databricks_cols\n'
        f'only_in_databricks = databricks_cols - alteryx_cols\n'
        f'if only_in_alteryx:\n'
        f'    print(f"Columns only in Alteryx: {{only_in_alteryx}}")\n'
        f'if only_in_databricks:\n'
        f'    print(f"Columns only in Databricks: {{only_in_databricks}}")\n'
        f'assert not only_in_alteryx and not only_in_databricks, "Schema mismatch detected"'
    )

    # ── Section 4: Aggregate Checks ──
    if numeric_fields:
        agg_lines = [f'# SECTION 4: Aggregate Checks (sum/min/max on numeric columns)']
        agg_lines.append('from pyspark.sql import functions as F')
        agg_lines.append('')
        agg_lines.append('agg_exprs = [')
        for nf in numeric_fields:
            agg_lines.append(f'    F.sum("{nf}").alias("{nf}_sum"),')
            agg_lines.append(f'    F.min("{nf}").alias("{nf}_min"),')
            agg_lines.append(f'    F.max("{nf}").alias("{nf}_max"),')
        agg_lines.append(']')
        agg_lines.append('')
        agg_lines.append('print("Alteryx aggregates:")')
        agg_lines.append('alteryx_df.agg(*agg_exprs).show(truncate=False)')
        agg_lines.append('print("Databricks aggregates:")')
        agg_lines.append(f'{last_output_df}.agg(*agg_exprs).show(truncate=False)')
        _cell("\n".join(agg_lines))
    else:
        _cell(
            '# SECTION 4: Aggregate Checks\n'
            '# No numeric columns detected — add manual aggregate checks if needed'
        )

    # ── Section 5: Row-Level Comparison with DataComPy ──
    _cell(
        f'# SECTION 5: Row-Level Comparison with DataComPy\n'
        f'import datacompy\n'
        f'\n'
        f'comparison = datacompy.SparkCompare(\n'
        f'    spark,\n'
        f'    base_df=alteryx_df,\n'
        f'    compare_df={last_output_df},\n'
        f'    join_columns=[{join_columns_str}],\n'
        f')\n'
        f'\n'
        f'print(comparison.report())'
    )

    lines.append("")
    with open(output_dir / "04_validate.py", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
