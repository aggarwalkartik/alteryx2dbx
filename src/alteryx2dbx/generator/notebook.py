"""Main orchestrator — generates Databricks notebook bundle from an AlteryxWorkflow."""
from __future__ import annotations

from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow, GeneratedStep
from alteryx2dbx.dag.resolver import resolve_dag
from alteryx2dbx.handlers.registry import get_handler
import alteryx2dbx.handlers  # noqa: F401  — triggers handler registration

from .config import generate_config
from .validator import generate_validator
from .report import generate_report


_LOAD_TYPES = {"DbFileInput", "TextInput", "InputData"}
_OUTPUT_TYPES = {"DbFileOutput", "OutputData", "Browse"}


def generate_notebooks(workflow: AlteryxWorkflow, output_dir: Path) -> None:
    """Generate the full 4-notebook Databricks bundle for *workflow*.

    Creates ``output_dir/<workflow.name>/`` containing:
    - 01_load_sources.py
    - 02_transformations.py
    - 03_orchestrate.py
    - 04_validate.py
    - config.yml
    - conversion_report.md
    - alteryx_output/   (empty directory for validation artefacts)
    """
    wf_dir = output_dir / workflow.name
    wf_dir.mkdir(parents=True, exist_ok=True)
    (wf_dir / "alteryx_output").mkdir(exist_ok=True)

    # 1. Resolve execution order
    execution_order = resolve_dag(workflow)

    # 2. Build input map: tool_id → list of df variable names that feed into it
    input_map: dict[int, list[str]] = _build_input_map(workflow)

    # 3. Run each tool through its handler
    steps: dict[int, GeneratedStep] = {}
    for tool_id in execution_order:
        tool = workflow.tools[tool_id]
        handler = get_handler(tool)
        input_dfs = input_map.get(tool_id, [])
        step = handler.convert(tool, input_df_names=input_dfs or None)
        steps[tool_id] = step

    # 4. Classify steps
    load_ids = [tid for tid in execution_order if workflow.tools[tid].tool_type in _LOAD_TYPES]
    output_ids = [tid for tid in execution_order if workflow.tools[tid].tool_type in _OUTPUT_TYPES]
    transform_ids = [tid for tid in execution_order if tid not in load_ids and tid not in output_ids]

    # 5. Write notebooks
    _write_notebook(wf_dir / "01_load_sources.py", f"01 — Load Sources: {workflow.name}", load_ids, steps)
    _write_notebook(wf_dir / "02_transformations.py", f"02 — Transformations: {workflow.name}", transform_ids, steps)
    _write_orchestrator(wf_dir / "03_orchestrate.py", workflow.name, output_ids, steps)

    # 6. Validator
    last_output_df = steps[output_ids[-1]].output_df if output_ids else "df_result"
    generate_validator(wf_dir, last_output_df)

    # 7. Config
    generate_config(workflow, wf_dir)

    # 8. Report
    generate_report(wf_dir, workflow.tools, steps, execution_order)


# ── Private helpers ──────────────────────────────────────────────────


def _build_input_map(workflow: AlteryxWorkflow) -> dict[int, list[str]]:
    """For each tool, compute which df variable names feed into it.

    For Join tools, inputs are ordered [left_df, right_df] based on target_anchor.
    For Filter True/False outputs, the source anchor determines the df name suffix.
    """
    # First pass: collect all inputs with their target anchor info
    raw_inputs: dict[int, list[tuple[str, str]]] = {}  # tool_id → [(df_name, target_anchor)]
    for conn in workflow.connections:
        source_anchor = conn.source_anchor
        if source_anchor.lower() == "true":
            df_name = f"df_{conn.source_tool_id}_true"
        elif source_anchor.lower() == "false":
            df_name = f"df_{conn.source_tool_id}_false"
        else:
            df_name = f"df_{conn.source_tool_id}"
        raw_inputs.setdefault(conn.target_tool_id, []).append((df_name, conn.target_anchor))

    # Second pass: order inputs correctly for dual-input tools
    _LEFT_ANCHORS = {"left", "find", "targets", "f", "#1"}
    _RIGHT_ANCHORS = {"right", "replace", "source", "r", "s", "#2"}

    input_map: dict[int, list[str]] = {}
    for tool_id, inputs in raw_inputs.items():
        tool = workflow.tools.get(tool_id)
        if tool and tool.tool_type in ("Join", "FindReplace", "AppendFields") and len(inputs) >= 2:
            # Order by target_anchor: left/find/targets first, right/replace/source second
            left_dfs = [df for df, anchor in inputs if anchor.lower() in _LEFT_ANCHORS]
            right_dfs = [df for df, anchor in inputs if anchor.lower() in _RIGHT_ANCHORS]
            other_dfs = [df for df, anchor in inputs
                         if anchor.lower() not in _LEFT_ANCHORS and anchor.lower() not in _RIGHT_ANCHORS]
            input_map[tool_id] = left_dfs + right_dfs + other_dfs
        else:
            input_map[tool_id] = [df for df, _ in inputs]

    return input_map


def _collect_imports(tool_ids: list[int], steps: dict[int, GeneratedStep]) -> set[str]:
    """Merge imports across steps."""
    imports: set[str] = set()
    for tid in tool_ids:
        imports.update(steps[tid].imports)
    return imports


def _write_notebook(
    path: Path,
    title: str,
    tool_ids: list[int],
    steps: dict[int, GeneratedStep],
) -> None:
    """Write a Databricks notebook (.py) with a cell per tool."""
    lines = ["# Databricks notebook source", f"# {title}"]

    # Imports cell
    imports = _collect_imports(tool_ids, steps)
    if imports:
        lines.append("")
        lines.append("# COMMAND ----------")
        lines.append("")
        for imp in sorted(imports):
            lines.append(imp)

    # One cell per step
    for tid in tool_ids:
        step = steps[tid]
        lines.append("")
        lines.append("# COMMAND ----------")
        lines.append("")
        lines.append(step.code)

    lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _write_orchestrator(
    path: Path,
    workflow_name: str,
    output_ids: list[int],
    steps: dict[int, GeneratedStep],
) -> None:
    """Write 03_orchestrate.py that runs notebooks 01 & 02, then outputs."""
    lines = [
        "# Databricks notebook source",
        f"# 03 — Orchestrate: {workflow_name}",
        "",
        "# COMMAND ----------",
        "",
        '# Run load sources notebook',
        f'dbutils.notebook.run("01_load_sources", timeout_seconds=600)',
        "",
        "# COMMAND ----------",
        "",
        '# Run transformations notebook',
        f'dbutils.notebook.run("02_transformations", timeout_seconds=600)',
    ]

    # Imports for output steps
    imports = _collect_imports(output_ids, steps)
    if imports:
        lines.append("")
        lines.append("# COMMAND ----------")
        lines.append("")
        for imp in sorted(imports):
            lines.append(imp)

    # Output cells
    for tid in output_ids:
        step = steps[tid]
        lines.append("")
        lines.append("# COMMAND ----------")
        lines.append("")
        lines.append(step.code)

    lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
