# Document Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `document` CLI command that generates comprehensive migration documentation (Markdown + optional Confluence draft) from Alteryx workflows.

**Architecture:** New `document/` package with modules for report generation, Mermaid diagrams, config loading, and Confluence publishing. Reuses existing parser, DAG resolver, and handler registry for metadata. Separate from the notebook generation pipeline.

**Tech Stack:** Python, click, networkx (existing), pyyaml (existing), atlassian-python-api (optional), pytest

**Spec:** `docs/specs/2026-04-02-document-command-design.md`

---

### Task 1: Mermaid Diagram Generator

**Files:**
- Create: `src/alteryx2dbx/document/__init__.py`
- Create: `src/alteryx2dbx/document/mermaid.py`
- Test: `tests/test_document/test_mermaid.py`

- [ ] **Step 1: Create tests directory and write the failing test**

Create `tests/test_document/__init__.py` (empty) and `tests/test_document/test_mermaid.py`:

```python
from alteryx2dbx.parser.models import AlteryxWorkflow, AlteryxTool, AlteryxConnection


def _simple_workflow():
    """Input → Filter → Output."""
    return AlteryxWorkflow(
        name="test_wf",
        version="2024.1",
        tools={
            1: AlteryxTool(1, "AlteryxBasePluginsGui.DbFileInput.DbFileInput", "DbFileInput",
                           {"file_path": "data.csv"}, "Load Data", []),
            2: AlteryxTool(2, "AlteryxBasePluginsGui.Filter.Filter", "Filter",
                           {"expression": "[x] > 0"}, "Filter Positive", []),
            3: AlteryxTool(3, "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput", "DbFileOutput",
                           {"file_path": "out.csv"}, "Write Output", []),
        },
        connections=[
            AlteryxConnection(1, "Output", 2, "Input"),
            AlteryxConnection(2, "True", 3, "Input"),
        ],
        properties={},
    )


def _workflow_with_unsupported():
    """Input → UnsupportedTool → Output."""
    return AlteryxWorkflow(
        name="test_wf",
        version="2024.1",
        tools={
            1: AlteryxTool(1, "AlteryxBasePluginsGui.DbFileInput.DbFileInput", "DbFileInput",
                           {}, "Load", []),
            2: AlteryxTool(2, "com.unknown.Widget", "Widget",
                           {}, "Unknown Widget", []),
            3: AlteryxTool(3, "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput", "DbFileOutput",
                           {}, "Save", []),
        },
        connections=[
            AlteryxConnection(1, "Output", 2, "Input"),
            AlteryxConnection(2, "Output", 3, "Input"),
        ],
        properties={},
    )


def test_mermaid_basic_structure():
    from alteryx2dbx.document.mermaid import generate_mermaid
    wf = _simple_workflow()
    mermaid = generate_mermaid(wf)
    assert mermaid.startswith("```mermaid")
    assert "flowchart TD" in mermaid or "graph TD" in mermaid
    assert mermaid.strip().endswith("```")


def test_mermaid_contains_all_nodes():
    from alteryx2dbx.document.mermaid import generate_mermaid
    wf = _simple_workflow()
    mermaid = generate_mermaid(wf)
    assert "Load Data" in mermaid
    assert "Filter Positive" in mermaid
    assert "Write Output" in mermaid


def test_mermaid_contains_edges():
    from alteryx2dbx.document.mermaid import generate_mermaid
    wf = _simple_workflow()
    mermaid = generate_mermaid(wf)
    # Edges connect node IDs
    assert "node_1" in mermaid
    assert "node_2" in mermaid
    assert "node_3" in mermaid
    assert "-->" in mermaid


def test_mermaid_color_coding():
    from alteryx2dbx.document.mermaid import generate_mermaid
    wf = _simple_workflow()
    mermaid = generate_mermaid(wf)
    # Input tools should be green-styled, output blue
    assert "fill:#" in mermaid or "style" in mermaid or ":::" in mermaid


def test_mermaid_labels_dual_output_edges():
    from alteryx2dbx.document.mermaid import generate_mermaid
    wf = _simple_workflow()
    mermaid = generate_mermaid(wf)
    # Filter True branch should be labeled
    assert "True" in mermaid


def test_mermaid_unsupported_tool_colored():
    from alteryx2dbx.document.mermaid import generate_mermaid
    wf = _workflow_with_unsupported()
    mermaid = generate_mermaid(wf)
    assert "Unknown Widget" in mermaid
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_mermaid.py -v`
Expected: FAIL — `No module named 'alteryx2dbx.document'`.

- [ ] **Step 3: Implement Mermaid generator**

Create `src/alteryx2dbx/document/__init__.py` (empty).

Create `src/alteryx2dbx/document/mermaid.py`:

```python
"""Generate Mermaid.js flowchart from an AlteryxWorkflow."""
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxWorkflow
from alteryx2dbx.handlers.registry import get_handler
import alteryx2dbx.handlers  # noqa: F401

_INPUT_TYPES = {"DbFileInput", "TextInput", "InputData", "DynamicInput"}
_OUTPUT_TYPES = {"DbFileOutput", "OutputData", "Browse"}

# Anchors that should be labeled on edges
_LABELED_ANCHORS = {"True", "False", "Join", "Left", "Right", "Unique", "Duplicates",
                    "J", "L", "R", "U", "D"}


def _classify_tool(tool) -> str:
    """Return 'input', 'output', 'unsupported', or 'transform'."""
    if tool.tool_type in _INPUT_TYPES or tool.plugin.startswith("box_input_v"):
        return "input"
    if tool.tool_type in _OUTPUT_TYPES or tool.plugin.startswith("box_output_v"):
        return "output"
    handler = get_handler(tool)
    if type(handler).__name__ == "UnsupportedHandler":
        return "unsupported"
    return "transform"


def _escape_mermaid(text: str) -> str:
    """Escape characters that break Mermaid syntax."""
    return text.replace('"', "'").replace("<", "&lt;").replace(">", "&gt;")


def generate_mermaid(workflow: AlteryxWorkflow) -> str:
    """Return a fenced Mermaid code block with the workflow DAG."""
    lines = ["```mermaid", "flowchart TD"]

    # Define nodes
    for tool_id, tool in sorted(workflow.tools.items()):
        label = _escape_mermaid(tool.annotation or tool.tool_type)
        node_label = f"[{tool_id}] {tool.tool_type}: {label}"
        lines.append(f'    node_{tool_id}["{_escape_mermaid(node_label)}"]')

    lines.append("")

    # Define edges
    for conn in workflow.connections:
        src = f"node_{conn.source_tool_id}"
        dst = f"node_{conn.target_tool_id}"
        if conn.source_anchor in _LABELED_ANCHORS:
            lines.append(f"    {src} -->|{conn.source_anchor}| {dst}")
        else:
            lines.append(f"    {src} --> {dst}")

    lines.append("")

    # Style classes
    lines.append("    %% Color coding")
    for tool_id, tool in sorted(workflow.tools.items()):
        category = _classify_tool(tool)
        if category == "input":
            lines.append(f"    style node_{tool_id} fill:#d4edda,stroke:#28a745")
        elif category == "output":
            lines.append(f"    style node_{tool_id} fill:#cce5ff,stroke:#007bff")
        elif category == "unsupported":
            lines.append(f"    style node_{tool_id} fill:#f8d7da,stroke:#dc3545")

    lines.append("```")
    return "\n".join(lines)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_mermaid.py -v`
Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/alteryx2dbx/document/__init__.py src/alteryx2dbx/document/mermaid.py tests/test_document/__init__.py tests/test_document/test_mermaid.py
git commit -m "feat: add Mermaid flowchart generator for workflow DAGs"
```

---

### Task 2: Migration Report Generator

**Files:**
- Create: `src/alteryx2dbx/document/report.py`
- Test: `tests/test_document/test_report.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_document/test_report.py`:

```python
from pathlib import Path
from alteryx2dbx.parser.xml_parser import parse_yxmd

SIMPLE_YXMD = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2024.1">
  <Properties>
    <MetaInfo>
      <Name>ReportTest</Name>
      <Author>Test Author</Author>
      <Description>Test workflow for report generation</Description>
    </MetaInfo>
  </Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Position x="78" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <File FileFormat="0">\\\\server\\data\\input.csv</File>
        </Configuration>
        <Annotation DisplayMode="0"><Name>Load Customers</Name></Annotation>
        <MetaInfo connection="Output">
          <RecordInfo>
            <Field name="id" type="Int32"/>
            <Field name="name" size="254" type="V_WString"/>
            <Field name="revenue" type="Double"/>
          </RecordInfo>
        </MetaInfo>
      </Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter">
        <Position x="258" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <Mode>Custom</Mode>
          <Expression>[revenue] &gt; 100</Expression>
        </Configuration>
        <Annotation DisplayMode="0"><Name>High Revenue</Name></Annotation>
        <MetaInfo connection="True">
          <RecordInfo>
            <Field name="id" type="Int32"/>
            <Field name="name" size="254" type="V_WString"/>
            <Field name="revenue" type="Double"/>
          </RecordInfo>
        </MetaInfo>
      </Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput">
        <Position x="438" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <File FileFormat="0">\\\\server\\output\\result.csv</File>
        </Configuration>
        <Annotation DisplayMode="0"><Name>Write Results</Name></Annotation>
      </Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection>
      <Origin ToolID="1" Connection="Output"/>
      <Destination ToolID="2" Connection="Input"/>
    </Connection>
    <Connection>
      <Origin ToolID="2" Connection="True"/>
      <Destination ToolID="3" Connection="Input"/>
    </Connection>
  </Connections>
</AlteryxDocument>
'''


def test_migration_report_generated(tmp_path):
    from alteryx2dbx.document.report import generate_migration_report
    wf_file = tmp_path / "report_test.yxmd"
    wf_file.write_text(SIMPLE_YXMD, encoding="utf-8")
    wf = parse_yxmd(wf_file)
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    generate_migration_report(wf, output_dir)
    report_path = output_dir / "migration_report.md"
    assert report_path.exists()
    content = report_path.read_text()
    return content


def test_report_has_executive_summary(tmp_path):
    content = test_migration_report_generated(tmp_path)
    assert "## Executive Summary" in content
    assert "ReportTest" in content


def test_report_has_data_flow_diagram(tmp_path):
    content = test_migration_report_generated(tmp_path)
    assert "## Data Flow Diagram" in content
    assert "```mermaid" in content


def test_report_has_data_sources(tmp_path):
    content = test_migration_report_generated(tmp_path)
    assert "## Data Source Inventory" in content
    assert "Load Customers" in content


def test_report_has_output_inventory(tmp_path):
    content = test_migration_report_generated(tmp_path)
    assert "## Output Inventory" in content
    assert "Write Results" in content


def test_report_has_business_logic(tmp_path):
    content = test_migration_report_generated(tmp_path)
    assert "## Business Logic Summary" in content
    assert "revenue" in content.lower()


def test_report_has_review_checklist(tmp_path):
    content = test_migration_report_generated(tmp_path)
    assert "## Manual Review Checklist" in content


def test_report_has_conversion_details(tmp_path):
    content = test_migration_report_generated(tmp_path)
    assert "## Conversion Details" in content
    assert "Tool ID" in content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_report.py -v`
Expected: FAIL — `No module named 'alteryx2dbx.document.report'`.

- [ ] **Step 3: Implement migration report generator**

Create `src/alteryx2dbx/document/report.py`:

```python
"""Generate migration_report.md for a single workflow."""
from __future__ import annotations

from pathlib import Path
from datetime import date

from alteryx2dbx.parser.models import AlteryxWorkflow, AlteryxTool, GeneratedStep
from alteryx2dbx.dag.resolver import resolve_dag
from alteryx2dbx.handlers.registry import get_handler
from alteryx2dbx.fixes import apply_fixes
from alteryx2dbx.document.mermaid import generate_mermaid
import alteryx2dbx.handlers  # noqa: F401

_INPUT_TYPES = {"DbFileInput", "TextInput", "InputData", "DynamicInput"}
_OUTPUT_TYPES = {"DbFileOutput", "OutputData", "Browse"}
_LOGIC_TYPES = {"Filter", "Join", "Formula", "Summarize", "Select", "FindReplace"}

_BOX_INPUT_PREFIX = "box_input_v"
_BOX_OUTPUT_PREFIX = "box_output_v"


def generate_migration_report(workflow: AlteryxWorkflow, output_dir: Path) -> Path:
    """Generate migration_report.md and return its path."""
    execution_order = resolve_dag(workflow)
    steps = _run_handlers(workflow, execution_order)
    lines = []

    lines.append(f"# {workflow.name}")
    lines.append("")

    # 1. Executive Summary
    lines.extend(_executive_summary(workflow, steps, execution_order))

    # 2. Data Flow Diagram
    lines.append("## Data Flow Diagram")
    lines.append("")
    lines.append(generate_mermaid(workflow))
    lines.append("")

    # 3. Data Source Inventory
    lines.extend(_data_source_inventory(workflow, execution_order))

    # 4. Output Inventory
    lines.extend(_output_inventory(workflow, execution_order))

    # 5. Business Logic Summary
    lines.extend(_business_logic_summary(workflow, steps, execution_order))

    # 6. Conversion Details
    lines.extend(_conversion_details(workflow, steps, execution_order))

    # 7. Manual Review Checklist
    lines.extend(_review_checklist(workflow, steps, execution_order))

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "migration_report.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def _run_handlers(workflow: AlteryxWorkflow, execution_order: list[int]) -> dict[int, GeneratedStep]:
    """Run handlers on all tools to get confidence scores and metadata."""
    input_map = _build_simple_input_map(workflow)
    steps = {}
    for tool_id in execution_order:
        tool = workflow.tools[tool_id]
        handler = get_handler(tool)
        input_dfs = input_map.get(tool_id, [])
        step = handler.convert(tool, input_df_names=input_dfs or None)
        # Apply fixes for accurate confidence
        context = {"tool_type": tool.tool_type, **tool.config,
                   "output_fields": [{"name": f.name, "type": f.type, "size": f.size, "scale": f.scale}
                                     for f in tool.output_fields]}
        fix_result = apply_fixes(step.code, context)
        step.code = fix_result.code
        steps[tool_id] = step
    return steps


def _build_simple_input_map(workflow: AlteryxWorkflow) -> dict[int, list[str]]:
    """Simplified input map for documentation (just need df names, not full routing)."""
    input_map: dict[int, list[str]] = {}
    for conn in workflow.connections:
        df_name = f"df_{conn.source_tool_id}"
        input_map.setdefault(conn.target_tool_id, []).append(df_name)
    return input_map


def _is_input_tool(tool: AlteryxTool) -> bool:
    return tool.tool_type in _INPUT_TYPES or tool.plugin.startswith(_BOX_INPUT_PREFIX)


def _is_output_tool(tool: AlteryxTool) -> bool:
    return tool.tool_type in _OUTPUT_TYPES or tool.plugin.startswith(_BOX_OUTPUT_PREFIX)


def _executive_summary(workflow: AlteryxWorkflow, steps: dict[int, GeneratedStep],
                       execution_order: list[int]) -> list[str]:
    lines = ["## Executive Summary", ""]
    meta = workflow.properties.get("MetaInfo", {})
    author = meta.get("Author", "Unknown")
    description = meta.get("Description", "")
    total = len(execution_order)
    confidences = [steps[tid].confidence for tid in execution_order if tid in steps]
    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    supported = sum(1 for c in confidences if c > 0)
    unsupported = total - supported

    if avg_conf > 0.9:
        readiness = "Ready"
    elif avg_conf > 0.7:
        readiness = "Needs Review"
    else:
        readiness = "Significant Manual Work"

    if total < 10:
        complexity = "Simple"
    elif total < 30:
        complexity = "Medium"
    else:
        complexity = "Complex"

    lines.append(f"- **Workflow**: {workflow.name}")
    if author and author != "Unknown":
        lines.append(f"- **Author**: {author}")
    if description:
        lines.append(f"- **Description**: {description}")
    lines.append(f"- **Readiness**: {readiness}")
    lines.append(f"- **Confidence**: {avg_conf:.0%}")
    lines.append(f"- **Complexity**: {complexity} ({total} tools)")
    lines.append(f"- **Supported**: {supported}/{total} tools")
    if unsupported > 0:
        lines.append(f"- **Unsupported**: {unsupported} tools")
    lines.append(f"- **Generated**: {date.today().isoformat()}")
    lines.append("")
    return lines


def _data_source_inventory(workflow: AlteryxWorkflow, execution_order: list[int]) -> list[str]:
    lines = ["## Data Source Inventory", ""]
    lines.append("| Tool ID | Type | Source | Format | Fields | Notes |")
    lines.append("|---------|------|--------|--------|--------|-------|")
    for tid in execution_order:
        tool = workflow.tools[tid]
        if not _is_input_tool(tool):
            continue
        config = tool.config
        source = config.get("file_path", config.get("File", ""))
        if tool.plugin.startswith(_BOX_INPUT_PREFIX):
            source = f"Box: {config.get('file_name', source)} (ID: {config.get('box_file_id', '?')})"
        fmt = config.get("file_format", config.get("FileFormat", ""))
        field_count = len(tool.output_fields)
        notes = tool.annotation or ""
        lines.append(f"| {tid} | {tool.tool_type} | {source} | {fmt} | {field_count} | {notes} |")
    lines.append("")
    return lines


def _output_inventory(workflow: AlteryxWorkflow, execution_order: list[int]) -> list[str]:
    lines = ["## Output Inventory", ""]
    lines.append("| Tool ID | Type | Destination | Format | Notes |")
    lines.append("|---------|------|-------------|--------|-------|")
    for tid in execution_order:
        tool = workflow.tools[tid]
        if not _is_output_tool(tool):
            continue
        config = tool.config
        dest = config.get("file_path", config.get("File", ""))
        if tool.plugin.startswith(_BOX_OUTPUT_PREFIX):
            dest = f"Box: {config.get('file_name', dest)} (folder: {config.get('box_parent_id', '?')})"
        fmt = config.get("file_format", config.get("FileFormat", ""))
        notes = tool.annotation or ""
        lines.append(f"| {tid} | {tool.tool_type} | {dest} | {fmt} | {notes} |")
    lines.append("")
    return lines


def _business_logic_summary(workflow: AlteryxWorkflow, steps: dict[int, GeneratedStep],
                            execution_order: list[int]) -> list[str]:
    lines = ["## Business Logic Summary", ""]
    for tid in execution_order:
        tool = workflow.tools[tid]
        config = tool.config
        step = steps.get(tid)
        confidence_flag = "" if not step or step.confidence >= 1.0 else " :warning:"

        if tool.tool_type == "Filter":
            expr = config.get("expression", "?")
            lines.append(f"- **[{tid}] Filter: {tool.annotation or 'Filter'}**{confidence_flag}")
            lines.append(f"  - Filters rows where `{expr}`")
        elif tool.tool_type == "Join":
            fields = config.get("join_fields", [])
            field_str = ", ".join(f"{f['left']} = {f['right']}" for f in fields) if fields else "?"
            lines.append(f"- **[{tid}] Join: {tool.annotation or 'Join'}**{confidence_flag}")
            lines.append(f"  - Joins on: {field_str}")
        elif tool.tool_type == "Formula":
            formula_fields = config.get("formula_fields", [])
            for ff in formula_fields:
                lines.append(f"- **[{tid}] Formula: {tool.annotation or 'Formula'}**{confidence_flag}")
                lines.append(f"  - Sets `{ff.get('field', '?')}` = `{ff.get('expression', '?')}`")
        elif tool.tool_type == "Summarize":
            fields = config.get("summarize_fields", [])
            actions = [f"{f.get('action', '?')} of {f.get('field', '?')}" for f in fields]
            group_fields = [f.get("field", "") for f in fields if f.get("action") == "GroupBy"]
            agg_fields = [a for a in actions if "GroupBy" not in a]
            lines.append(f"- **[{tid}] Summarize: {tool.annotation or 'Summarize'}**{confidence_flag}")
            if group_fields:
                lines.append(f"  - Groups by: {', '.join(group_fields)}")
            if agg_fields:
                lines.append(f"  - Aggregates: {', '.join(agg_fields)}")
        elif tool.tool_type in ("AlteryxSelect", "Select"):
            fields = config.get("select_fields", [])
            dropped = [f for f in fields if f.get("selected") == "False"]
            renamed = [f for f in fields if f.get("rename")]
            if dropped or renamed:
                lines.append(f"- **[{tid}] Select: {tool.annotation or 'Select'}**{confidence_flag}")
                if dropped:
                    lines.append(f"  - Drops {len(dropped)} field(s)")
                if renamed:
                    lines.append(f"  - Renames {len(renamed)} field(s)")

    if len(lines) == 2:  # only header
        lines.append("No business logic tools detected.")
    lines.append("")
    return lines


def _conversion_details(workflow: AlteryxWorkflow, steps: dict[int, GeneratedStep],
                        execution_order: list[int]) -> list[str]:
    lines = ["## Conversion Details", ""]
    lines.append("| Tool ID | Type | Annotation | Confidence | Notes |")
    lines.append("|---------|------|------------|------------|-------|")
    for tid in execution_order:
        tool = workflow.tools.get(tid)
        step = steps.get(tid)
        if tool and step:
            notes_str = "; ".join(step.notes) if step.notes else ""
            lines.append(f"| {tid} | {tool.tool_type} | {tool.annotation} | {step.confidence:.0%} | {notes_str} |")
    lines.append("")
    return lines


def _review_checklist(workflow: AlteryxWorkflow, steps: dict[int, GeneratedStep],
                      execution_order: list[int]) -> list[str]:
    lines = ["## Manual Review Checklist", ""]
    items = []
    for tid in execution_order:
        tool = workflow.tools[tid]
        step = steps.get(tid)
        if not step:
            continue
        if step.confidence == 0.0:
            items.append(f"- [ ] **Tool {tid} ({tool.tool_type})**: Unsupported — needs manual implementation")
        elif step.confidence < 0.7:
            items.append(f"- [ ] **Tool {tid} ({tool.tool_type})**: Low confidence ({step.confidence:.0%}) — review generated code")
        if tool.plugin.startswith(_BOX_INPUT_PREFIX) or tool.plugin.startswith(_BOX_OUTPUT_PREFIX):
            items.append(f"- [ ] **Tool {tid} ({tool.tool_type})**: Box auth setup required (Databricks Secret scope)")
        for note in step.notes:
            if "network path" in note.lower() or "unc" in note.lower():
                items.append(f"- [ ] **Tool {tid}**: Network/UNC path needs remapping to cloud storage")
                break
        if "TODO" in step.code:
            items.append(f"- [ ] **Tool {tid} ({tool.tool_type})**: Contains TODO comments — review generated code")

    if not items:
        lines.append("No items require manual review.")
    else:
        lines.extend(items)
    lines.append("")
    return lines
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_report.py -v`
Expected: All 8 tests PASS.

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest --tb=short`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/alteryx2dbx/document/report.py tests/test_document/test_report.py
git commit -m "feat: add migration report generator with 7 sections"
```

---

### Task 3: Portfolio Report Generator (Batch Mode)

**Files:**
- Create: `src/alteryx2dbx/document/portfolio.py`
- Test: `tests/test_document/test_portfolio.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_document/test_portfolio.py`:

```python
from alteryx2dbx.document.portfolio import generate_portfolio_report
from pathlib import Path


def test_portfolio_report_generated(tmp_path):
    results = [
        {"name": "wf_a", "tools_total": 10, "avg_confidence": 0.95, "supported": 10, "unsupported": 0, "readiness": "Ready"},
        {"name": "wf_b", "tools_total": 25, "avg_confidence": 0.65, "supported": 20, "unsupported": 5, "readiness": "Significant Manual Work"},
        {"name": "wf_c", "tools_total": 5, "avg_confidence": 0.82, "supported": 4, "unsupported": 1, "readiness": "Needs Review"},
    ]
    generate_portfolio_report(tmp_path, results)
    report_path = tmp_path / "portfolio_report.md"
    assert report_path.exists()
    content = report_path.read_text()
    assert "## Summary" in content
    assert "wf_a" in content
    assert "wf_b" in content


def test_portfolio_sorted_by_confidence(tmp_path):
    results = [
        {"name": "good", "tools_total": 5, "avg_confidence": 0.95, "supported": 5, "unsupported": 0, "readiness": "Ready"},
        {"name": "bad", "tools_total": 10, "avg_confidence": 0.40, "supported": 4, "unsupported": 6, "readiness": "Significant Manual Work"},
    ]
    generate_portfolio_report(tmp_path, results)
    content = (tmp_path / "portfolio_report.md").read_text()
    # "bad" should appear before "good" in the table (sorted by lowest confidence)
    bad_pos = content.index("bad")
    good_pos = content.index("good")
    assert bad_pos < good_pos
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_portfolio.py -v`
Expected: FAIL — `No module named 'alteryx2dbx.document.portfolio'`.

- [ ] **Step 3: Implement portfolio report**

Create `src/alteryx2dbx/document/portfolio.py`:

```python
"""Generate portfolio_report.md for batch documentation runs."""
from __future__ import annotations

from pathlib import Path
from datetime import date


def generate_portfolio_report(output_dir: Path, results: list[dict]) -> Path:
    """Write portfolio_report.md summarizing multiple workflows."""
    total_workflows = len(results)
    avg_confidence = sum(r["avg_confidence"] for r in results) / total_workflows if results else 0
    total_tools = sum(r["tools_total"] for r in results)

    readiness_counts = {}
    for r in results:
        readiness = r.get("readiness", "Unknown")
        readiness_counts[readiness] = readiness_counts.get(readiness, 0) + 1

    lines = [
        "# Portfolio Assessment",
        "",
        "## Summary",
        "",
        f"- **Total workflows**: {total_workflows}",
        f"- **Total tools**: {total_tools}",
        f"- **Average confidence**: {avg_confidence:.0%}",
    ]
    for readiness, count in sorted(readiness_counts.items()):
        lines.append(f"- **{readiness}**: {count} workflow(s)")
    lines.append(f"- **Generated**: {date.today().isoformat()}")
    lines.append("")

    # Table sorted by lowest confidence first
    lines.append("## Workflows")
    lines.append("")
    lines.append("| Workflow | Tools | Confidence | Readiness | Unsupported |")
    lines.append("|----------|-------|------------|-----------|-------------|")
    for r in sorted(results, key=lambda x: x["avg_confidence"]):
        lines.append(
            f"| [{r['name']}]({r['name']}/migration_report.md) "
            f"| {r['tools_total']} "
            f"| {r['avg_confidence']:.0%} "
            f"| {r.get('readiness', '?')} "
            f"| {r.get('unsupported', 0)} |"
        )
    lines.append("")

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "portfolio_report.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_portfolio.py -v`
Expected: Both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/alteryx2dbx/document/portfolio.py tests/test_document/test_portfolio.py
git commit -m "feat: add portfolio report generator for batch documentation"
```

---

### Task 4: Config File Loader

**Files:**
- Create: `src/alteryx2dbx/document/config.py`
- Test: `tests/test_document/test_config.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_document/test_config.py`:

```python
import os
from pathlib import Path
from alteryx2dbx.document.config import load_config


def test_load_config_from_file(tmp_path):
    config_file = tmp_path / ".alteryx2dbx.yml"
    config_file.write_text(
        "confluence:\n"
        "  url: https://company.atlassian.net\n"
        "  space: DATA-MIGRATION\n"
        '  parent_page: "Migration Reports"\n'
        "  pat: my-secret-token\n",
        encoding="utf-8",
    )
    config = load_config(tmp_path)
    assert config is not None
    assert config["confluence"]["url"] == "https://company.atlassian.net"
    assert config["confluence"]["space"] == "DATA-MIGRATION"
    assert config["confluence"]["pat"] == "my-secret-token"


def test_load_config_no_file(tmp_path):
    config = load_config(tmp_path)
    assert config is None


def test_load_config_pat_from_env(tmp_path, monkeypatch):
    config_file = tmp_path / ".alteryx2dbx.yml"
    config_file.write_text(
        "confluence:\n"
        "  url: https://company.atlassian.net\n"
        "  space: DATA-MIGRATION\n"
        '  parent_page: "Migration Reports"\n'
        "  pat: ''\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CONFLUENCE_PAT", "env-token-123")
    config = load_config(tmp_path)
    assert config["confluence"]["pat"] == "env-token-123"


def test_load_config_walks_up_directories(tmp_path):
    config_file = tmp_path / ".alteryx2dbx.yml"
    config_file.write_text(
        "confluence:\n"
        "  url: https://example.com\n"
        "  space: TEST\n",
        encoding="utf-8",
    )
    subdir = tmp_path / "deep" / "nested"
    subdir.mkdir(parents=True)
    config = load_config(subdir)
    assert config is not None
    assert config["confluence"]["url"] == "https://example.com"


def test_load_config_explicit_path(tmp_path):
    config_file = tmp_path / "custom_config.yml"
    config_file.write_text(
        "confluence:\n"
        "  url: https://custom.com\n"
        "  space: CUSTOM\n",
        encoding="utf-8",
    )
    config = load_config(tmp_path, config_path=config_file)
    assert config["confluence"]["url"] == "https://custom.com"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_config.py -v`
Expected: FAIL — `No module named 'alteryx2dbx.document.config'`.

- [ ] **Step 3: Implement config loader**

Create `src/alteryx2dbx/document/config.py`:

```python
"""Load .alteryx2dbx.yml configuration."""
from __future__ import annotations

import os
from pathlib import Path

import yaml


def load_config(start_dir: Path, *, config_path: Path | None = None) -> dict | None:
    """Load .alteryx2dbx.yml, walking up from start_dir. Returns None if not found."""
    if config_path:
        if config_path.exists():
            return _parse_and_resolve(config_path)
        return None

    current = start_dir.resolve()
    while True:
        candidate = current / ".alteryx2dbx.yml"
        if candidate.exists():
            return _parse_and_resolve(candidate)
        parent = current.parent
        if parent == current:
            break
        current = parent

    return None


def _parse_and_resolve(path: Path) -> dict:
    """Parse YAML and resolve PAT from env if empty."""
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    confluence = config.get("confluence", {})

    # PAT resolution: config file → env var
    pat = confluence.get("pat", "")
    if not pat:
        pat = os.environ.get("CONFLUENCE_PAT", "")
    confluence["pat"] = pat

    return config
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_config.py -v`
Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/alteryx2dbx/document/config.py tests/test_document/test_config.py
git commit -m "feat: add .alteryx2dbx.yml config loader with PAT resolution"
```

---

### Task 5: Confluence Publisher

**Files:**
- Create: `src/alteryx2dbx/document/confluence.py`
- Test: `tests/test_document/test_confluence.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_document/test_confluence.py`:

```python
from unittest.mock import MagicMock, patch
from alteryx2dbx.document.confluence import publish_draft, confluence_available


def test_confluence_available_when_installed():
    # atlassian-python-api may or may not be installed in test env
    result = confluence_available()
    assert isinstance(result, bool)


def test_publish_draft_creates_page():
    mock_confluence = MagicMock()
    mock_confluence.get_page_by_title.return_value = None
    mock_confluence.create_page.return_value = {"id": "12345", "_links": {"base": "https://test.atlassian.net/wiki", "webui": "/spaces/TEST/pages/12345"}}

    config = {
        "confluence": {
            "url": "https://test.atlassian.net",
            "space": "TEST",
            "parent_page": "Migration Reports",
            "pat": "fake-token",
        }
    }
    markdown = "# Test Report\n\nSome content."

    with patch("alteryx2dbx.document.confluence._get_confluence_client", return_value=mock_confluence):
        result = publish_draft(config, "Test Workflow", markdown)

    mock_confluence.create_page.assert_called_once()
    assert result is not None


def test_publish_draft_updates_existing_page():
    mock_confluence = MagicMock()
    mock_confluence.get_page_by_title.return_value = {"id": "99999"}
    mock_confluence.update_page.return_value = {"id": "99999", "_links": {"base": "https://test.atlassian.net/wiki", "webui": "/spaces/TEST/pages/99999"}}

    config = {
        "confluence": {
            "url": "https://test.atlassian.net",
            "space": "TEST",
            "parent_page": "Migration Reports",
            "pat": "fake-token",
        }
    }
    markdown = "# Updated Report\n\nNew content."

    with patch("alteryx2dbx.document.confluence._get_confluence_client", return_value=mock_confluence):
        result = publish_draft(config, "Test Workflow", markdown)

    mock_confluence.update_page.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_confluence.py -v`
Expected: FAIL — `No module named 'alteryx2dbx.document.confluence'`.

- [ ] **Step 3: Implement Confluence publisher**

Create `src/alteryx2dbx/document/confluence.py`:

```python
"""Publish migration reports as Confluence draft pages."""
from __future__ import annotations

import re


def confluence_available() -> bool:
    """Check if atlassian-python-api is installed."""
    try:
        import atlassian  # noqa: F401
        return True
    except ImportError:
        return False


def _get_confluence_client(config: dict):
    """Create an authenticated Confluence client."""
    from atlassian import Confluence
    conf = config["confluence"]
    return Confluence(
        url=conf["url"],
        token=conf["pat"],
    )


def _markdown_to_storage(markdown: str) -> str:
    """Convert Markdown to Confluence storage format (simplified).

    Handles headings, tables, lists, code blocks, and bold/italic.
    Mermaid blocks are wrapped in a code macro.
    """
    lines = markdown.split("\n")
    result = []
    in_code_block = False
    code_lang = ""
    code_lines = []

    for line in lines:
        if line.startswith("```") and not in_code_block:
            in_code_block = True
            code_lang = line[3:].strip()
            code_lines = []
            continue
        elif line.startswith("```") and in_code_block:
            in_code_block = False
            code_content = "\n".join(code_lines)
            if code_lang == "mermaid":
                result.append(
                    f'<ac:structured-macro ac:name="code"><ac:parameter ac:name="language">text</ac:parameter>'
                    f"<ac:plain-text-body><![CDATA[{code_content}]]></ac:plain-text-body></ac:structured-macro>"
                )
            else:
                result.append(
                    f'<ac:structured-macro ac:name="code"><ac:parameter ac:name="language">{code_lang or "text"}</ac:parameter>'
                    f"<ac:plain-text-body><![CDATA[{code_content}]]></ac:plain-text-body></ac:structured-macro>"
                )
            continue

        if in_code_block:
            code_lines.append(line)
            continue

        # Headings
        if line.startswith("# "):
            result.append(f"<h1>{line[2:]}</h1>")
        elif line.startswith("## "):
            result.append(f"<h2>{line[3:]}</h2>")
        elif line.startswith("### "):
            result.append(f"<h3>{line[4:]}</h3>")
        # Checkbox items
        elif line.strip().startswith("- [ ]"):
            content = line.strip()[5:].strip()
            content = _inline_formatting(content)
            result.append(f"<ac:task><ac:task-body>{content}</ac:task-body></ac:task>")
        # List items
        elif line.strip().startswith("- "):
            content = line.strip()[2:]
            content = _inline_formatting(content)
            result.append(f"<li>{content}</li>")
        # Table rows
        elif "|" in line and not line.strip().startswith("|--"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            row = "".join(f"<td>{_inline_formatting(c)}</td>" for c in cells)
            result.append(f"<tr>{row}</tr>")
        # Separator lines (skip)
        elif line.strip().startswith("|--"):
            continue
        # Empty lines
        elif not line.strip():
            result.append("<br/>")
        # Regular paragraphs
        else:
            result.append(f"<p>{_inline_formatting(line)}</p>")

    return "\n".join(result)


def _inline_formatting(text: str) -> str:
    """Apply bold and italic inline formatting."""
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"`(.+?)`", r"<code>\1</code>", text)
    return text


def publish_draft(config: dict, workflow_name: str, markdown: str) -> dict | None:
    """Create or update a Confluence draft page. Returns page info dict or None."""
    confluence = _get_confluence_client(config)
    conf = config["confluence"]
    space = conf["space"]
    parent_page_title = conf.get("parent_page", "")

    # Find parent page ID
    parent_id = None
    if parent_page_title:
        parent = confluence.get_page_by_title(space, parent_page_title)
        if parent:
            parent_id = parent["id"]

    # Check if page already exists
    title = f"Migration Report: {workflow_name}"
    existing = confluence.get_page_by_title(space, title)
    storage_body = _markdown_to_storage(markdown)

    if existing:
        return confluence.update_page(
            page_id=existing["id"],
            title=title,
            body=storage_body,
            type="page",
            status="draft",
        )
    else:
        return confluence.create_page(
            space=space,
            title=title,
            body=storage_body,
            parent_id=parent_id,
            type="page",
            status="draft",
        )


def pat_setup_guide() -> str:
    """Return a step-by-step guide for creating a Confluence PAT."""
    return (
        "To publish to Confluence, you need a Personal Access Token (PAT):\n"
        "\n"
        "1. Go to your Confluence instance → Profile → Personal Access Tokens\n"
        "2. Click 'Create token'\n"
        "3. Give it a name like 'alteryx2dbx'\n"
        "4. Copy the token\n"
        "5. Add it to your .alteryx2dbx.yml:\n"
        "   confluence:\n"
        "     pat: your-token-here\n"
        "\n"
        "   Or set the CONFLUENCE_PAT environment variable.\n"
        "   On Databricks, use: dbutils.secrets.get('confluence', 'pat')\n"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_document/test_confluence.py -v`
Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/alteryx2dbx/document/confluence.py tests/test_document/test_confluence.py
git commit -m "feat: add Confluence draft publisher with markdown conversion"
```

---

### Task 6: Wire Up the `document` CLI Command

**Files:**
- Modify: `src/alteryx2dbx/cli.py`
- Test: `tests/test_cli_document.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_cli_document.py`:

```python
from pathlib import Path
from click.testing import CliRunner
from alteryx2dbx.cli import main

FIXTURES = Path(__file__).parent / "fixtures"


def test_document_single_file(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, [
        "document", str(FIXTURES / "simple_filter.yxmd"), "-o", str(tmp_path),
    ])
    assert result.exit_code == 0, result.output
    report = tmp_path / "simple_filter" / "migration_report.md"
    assert report.exists()
    content = report.read_text()
    assert "## Executive Summary" in content
    assert "## Data Flow Diagram" in content


def test_document_directory(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, [
        "document", str(FIXTURES), "-o", str(tmp_path),
    ])
    assert result.exit_code == 0, result.output
    # Should generate individual reports + portfolio
    assert (tmp_path / "portfolio_report.md").exists()


def test_document_no_confluence_prints_tip(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, [
        "document", str(FIXTURES / "simple_filter.yxmd"), "-o", str(tmp_path),
    ])
    assert result.exit_code == 0
    assert "Tip" in result.output or "confluence" in result.output.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_cli_document.py -v`
Expected: FAIL — `No such command 'document'`.

- [ ] **Step 3: Add document command to cli.py**

Add imports at the top of `src/alteryx2dbx/cli.py`:

```python
from .document.report import generate_migration_report
from .document.portfolio import generate_portfolio_report
from .document.config import load_config
from .document.confluence import publish_draft, confluence_available, pat_setup_guide
```

Add the command after the existing `tools` command:

```python
@main.command()
@click.argument("source", type=click.Path(exists=True))
@click.option("-o", "--output", default="./output", help="Output directory")
@click.option("--config", "config_path", default=None, type=click.Path(), help="Path to .alteryx2dbx.yml")
def document(source, output, config_path):
    """Generate migration documentation for workflow(s)."""
    source_path = Path(source)
    output_path = Path(output)

    if source_path.is_file():
        files = [source_path]
    else:
        files = list(source_path.glob("**/*.yxmd")) + list(source_path.glob("**/*.yxzp"))

    if not files:
        click.echo("No .yxmd or .yxzp files found.")
        return

    # Load config for Confluence
    config_p = Path(config_path) if config_path else None
    config = load_config(Path.cwd(), config_path=config_p)

    results = []
    for f in files:
        click.echo(f"Documenting: {f.name}")
        unpacked = unpack_source(f)
        try:
            wf = parse_yxmd(unpacked.workflow_path)
            wf_output = output_path / wf.name
            report_path = generate_migration_report(wf, wf_output)
            click.echo(f"  Report: {report_path}")

            # Collect stats for portfolio report
            from alteryx2dbx.dag.resolver import resolve_dag
            execution_order = resolve_dag(wf)
            from alteryx2dbx.handlers.registry import get_handler
            confidences = []
            supported = 0
            for tid in execution_order:
                tool = wf.tools[tid]
                handler = get_handler(tool)
                is_supported = type(handler).__name__ != "UnsupportedHandler"
                if is_supported:
                    supported += 1
                step = handler.convert(tool)
                confidences.append(step.confidence)
            avg_conf = sum(confidences) / len(confidences) if confidences else 0
            readiness = "Ready" if avg_conf > 0.9 else "Needs Review" if avg_conf > 0.7 else "Significant Manual Work"
            results.append({
                "name": wf.name,
                "tools_total": len(execution_order),
                "avg_confidence": avg_conf,
                "supported": supported,
                "unsupported": len(execution_order) - supported,
                "readiness": readiness,
            })

            # Confluence publishing
            if config and config.get("confluence", {}).get("pat"):
                if confluence_available():
                    markdown = report_path.read_text(encoding="utf-8")
                    try:
                        result = publish_draft(config, wf.name, markdown)
                        if result:
                            click.echo(f"  Confluence draft created/updated")
                    except Exception as e:
                        click.echo(f"  Confluence error: {e}", err=True)
                else:
                    click.echo("  Install confluence support: pip install alteryx2dbx[confluence]")
        except Exception as e:
            click.echo(f"  Error: {e}", err=True)
        finally:
            unpacked.cleanup()

    # Portfolio report for batch mode
    if len(files) > 1:
        generate_portfolio_report(output_path, results)
        click.echo(f"Portfolio: {output_path / 'portfolio_report.md'}")

    # Confluence guidance
    if not config:
        click.echo("\nTip: Create .alteryx2dbx.yml to enable Confluence publishing. See README.")
    elif not config.get("confluence", {}).get("pat"):
        click.echo(f"\n{pat_setup_guide()}")

    click.echo(f"\nDone. Documented {len(files)} workflow(s).")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_cli_document.py -v`
Expected: All 3 tests PASS.

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest --tb=short`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/alteryx2dbx/cli.py tests/test_cli_document.py
git commit -m "feat: add document CLI command with Confluence integration"
```

---

### Task 7: Update pyproject.toml and README

**Files:**
- Modify: `pyproject.toml`
- Modify: `README.md`

- [ ] **Step 1: Add Confluence optional dependency to pyproject.toml**

```toml
[project.optional-dependencies]
box = ["boxsdk[jwt]>=3.0"]
confluence = ["atlassian-python-api>=3.0"]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
]
```

- [ ] **Step 2: Add document command section to README.md**

After the existing "Convert a single workflow" section, add:

```markdown
### Document a workflow (migration report)

```bash
alteryx2dbx document workflow.yxmd -o ./output
alteryx2dbx document ./workflows/ -o ./output          # batch + portfolio report
```

Generates a comprehensive `migration_report.md` with executive summary, data flow diagram (Mermaid), source/output inventory, business logic summary, and manual review checklist.

**On Databricks:**

```python
%pip install -e /Workspace/Repos/your-name/alteryx2dbx

# Upload your .yxmd files to a Volume, then:
!alteryx2dbx document /Volumes/catalog/schema/workflows/ -o /Volumes/catalog/schema/output/
```

**Confluence integration (optional):**

Create `.alteryx2dbx.yml` in the repo root:

```yaml
confluence:
  url: https://company.atlassian.net
  space: DATA-MIGRATION
  parent_page: "Alteryx Migration Reports"
```

Set your PAT via environment variable or Databricks secret:

```python
import os
os.environ["CONFLUENCE_PAT"] = dbutils.secrets.get("confluence", "pat")
```

When configured, `document` automatically creates Confluence **draft** pages (never publishes directly).
```

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml README.md
git commit -m "docs: add document command to README, add confluence optional dependency"
```
