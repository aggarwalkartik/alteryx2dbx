# alteryx2dbx Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a deterministic CLI tool that converts Alteryx `.yxmd` workflows into runnable PySpark Databricks notebooks with validation.

**Architecture:** 5-stage pipeline (XML Parser → DAG Resolver → Expression Transpiler → Code Generator → Validator Generator). Each stage is independent and testable. Tool handlers are pluggable via a registry pattern.

**Tech Stack:** Python 3.11+, lxml, lark, networkx, jinja2, click, datacompy, pytest

---

## Phase 1: Project Scaffolding + IR Models

### Task 1: Initialize project with pyproject.toml

**Files:**
- Create: `alteryx2dbx/pyproject.toml`
- Create: `alteryx2dbx/src/alteryx2dbx/__init__.py`
- Create: `alteryx2dbx/README.md`

**Step 1: Create project directory**

```bash
cd "/Users/kartikaggarwal/Projects"
mkdir -p alteryx2dbx/src/alteryx2dbx
mkdir -p alteryx2dbx/tests/fixtures
```

**Step 2: Write pyproject.toml**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "alteryx2dbx"
version = "0.1.0"
description = "Convert Alteryx workflows to PySpark Databricks notebooks"
requires-python = ">=3.11"
license = "MIT"
dependencies = [
    "lxml>=5.0",
    "lark>=1.1",
    "networkx>=3.0",
    "jinja2>=3.1",
    "click>=8.1",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
]

[project.scripts]
alteryx2dbx = "alteryx2dbx.cli:main"
```

**Step 3: Write __init__.py**

```python
"""alteryx2dbx - Convert Alteryx workflows to PySpark Databricks notebooks."""

__version__ = "0.1.0"
```

**Step 4: Init git repo**

```bash
cd "/Users/kartikaggarwal/Projects/alteryx2dbx"
git init
```

**Step 5: Create .gitignore**

```
__pycache__/
*.pyc
.pytest_cache/
dist/
*.egg-info/
.venv/
output/
```

**Step 6: Install in dev mode**

```bash
cd "/Users/kartikaggarwal/Projects/alteryx2dbx"
pip install -e ".[dev]"
```

**Step 7: Commit**

```bash
git add -A
git commit -m "feat: initialize project scaffolding"
```

---

### Task 2: Define IR models (dataclasses)

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/parser/__init__.py`
- Create: `alteryx2dbx/src/alteryx2dbx/parser/models.py`
- Test: `alteryx2dbx/tests/test_models.py`

**Step 1: Write failing test**

```python
# tests/test_models.py
from alteryx2dbx.parser.models import (
    AlteryxField,
    AlteryxConnection,
    AlteryxTool,
    AlteryxWorkflow,
    GeneratedStep,
)


def test_alteryx_field_creation():
    field = AlteryxField(name="Revenue", type="Double", size=None, scale=None)
    assert field.name == "Revenue"
    assert field.type == "Double"


def test_alteryx_connection_creation():
    conn = AlteryxConnection(
        source_tool_id=1, source_anchor="Output",
        target_tool_id=2, target_anchor="Input",
    )
    assert conn.source_tool_id == 1
    assert conn.target_tool_id == 2


def test_alteryx_tool_creation():
    tool = AlteryxTool(
        tool_id=1,
        plugin="AlteryxBasePluginsGui.Filter.Filter",
        tool_type="Filter",
        config={"expression": "[Revenue] > 100"},
        annotation="Filter High Revenue",
        input_fields=[],
        output_fields=[],
    )
    assert tool.tool_type == "Filter"
    assert tool.annotation == "Filter High Revenue"


def test_alteryx_workflow_creation():
    wf = AlteryxWorkflow(
        name="test_workflow",
        version="11.7",
        tools={},
        connections=[],
        properties={},
    )
    assert wf.name == "test_workflow"


def test_generated_step_creation():
    step = GeneratedStep(
        step_name="filter_active",
        code='df_5 = df_1.filter(F.col("status") == F.lit("active"))',
        imports={"from pyspark.sql import functions as F"},
        input_dfs=["df_1"],
        output_df="df_5",
        notes=[],
        confidence=1.0,
    )
    assert step.confidence == 1.0
    assert step.output_df == "df_5"


def test_generated_step_with_warnings():
    step = GeneratedStep(
        step_name="multi_row",
        code="df_10 = df_8  # PASSTHROUGH",
        imports=set(),
        input_dfs=["df_8"],
        output_df="df_10",
        notes=["Self-referencing multi-row formula detected"],
        confidence=0.5,
    )
    assert len(step.notes) == 1
    assert step.confidence == 0.5
```

**Step 2: Run test to verify it fails**

```bash
cd "/Users/kartikaggarwal/Projects/alteryx2dbx"
pytest tests/test_models.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'alteryx2dbx.parser'`

**Step 3: Write models.py**

```python
# src/alteryx2dbx/parser/models.py
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AlteryxField:
    name: str
    type: str
    size: int | None = None
    scale: int | None = None


@dataclass
class AlteryxConnection:
    source_tool_id: int
    source_anchor: str
    target_tool_id: int
    target_anchor: str


@dataclass
class AlteryxTool:
    tool_id: int
    plugin: str
    tool_type: str
    config: dict
    annotation: str = ""
    input_fields: list[AlteryxField] = field(default_factory=list)
    output_fields: list[AlteryxField] = field(default_factory=list)


@dataclass
class AlteryxWorkflow:
    name: str
    version: str
    tools: dict[int, AlteryxTool] = field(default_factory=dict)
    connections: list[AlteryxConnection] = field(default_factory=list)
    properties: dict = field(default_factory=dict)


@dataclass
class GeneratedStep:
    step_name: str
    code: str
    imports: set[str] = field(default_factory=set)
    input_dfs: list[str] = field(default_factory=list)
    output_df: str = ""
    notes: list[str] = field(default_factory=list)
    confidence: float = 1.0
```

**Step 4: Create __init__.py**

```python
# src/alteryx2dbx/parser/__init__.py
```

**Step 5: Run tests**

```bash
pytest tests/test_models.py -v
```

Expected: All PASS

**Step 6: Commit**

```bash
git add src/alteryx2dbx/parser/ tests/test_models.py
git commit -m "feat: define IR dataclass models"
```

---

### Task 3: Create sample .yxmd test fixture

**Files:**
- Create: `alteryx2dbx/tests/fixtures/simple_filter.yxmd`

We need a realistic but minimal Alteryx workflow XML to test against. This fixture has: InputData → Filter → OutputData.

**Step 1: Create fixture file**

```xml
<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2024.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Position x="78" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <Passwords/>
          <File OutputFileName="" RecordLimit="" SearchSubDirs="False" FileFormat="19">\\server\data\customers.xlsx</File>
          <FormatSpecificOptions>
            <HeaderRow>True</HeaderRow>
            <NoProgress>False</NoProgress>
            <Delimeter>,</Delimeter>
          </FormatSpecificOptions>
        </Configuration>
        <Annotation DisplayMode="0">
          <Name>Input Customers</Name>
          <DefaultAnnotationText>customers.xlsx</DefaultAnnotationText>
          <Left value="False"/>
        </Annotation>
        <MetaInfo connection="Output">
          <RecordInfo>
            <Field name="customer_id" size="254" type="V_WString"/>
            <Field name="name" size="254" type="V_WString"/>
            <Field name="status" size="254" type="V_WString"/>
            <Field name="revenue" type="Double"/>
          </RecordInfo>
        </MetaInfo>
      </Properties>
      <EngineSettings EngineDll="AlteryxBasePluginsEngine.dll" EngineDllEntryPoint="AlteryxDbFileInput"/>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter">
        <Position x="258" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <Mode>Custom</Mode>
          <Expression>[status] = "active" AND [revenue] &gt; 100</Expression>
        </Configuration>
        <Annotation DisplayMode="0">
          <Name>Filter Active High Rev</Name>
          <DefaultAnnotationText>[status] = "active" AND [revenue] &gt; 100</DefaultAnnotationText>
          <Left value="False"/>
        </Annotation>
        <MetaInfo connection="True">
          <RecordInfo>
            <Field name="customer_id" size="254" type="V_WString"/>
            <Field name="name" size="254" type="V_WString"/>
            <Field name="status" size="254" type="V_WString"/>
            <Field name="revenue" type="Double"/>
          </RecordInfo>
        </MetaInfo>
      </Properties>
      <EngineSettings EngineDll="AlteryxBasePluginsEngine.dll" EngineDllEntryPoint="AlteryxFilter"/>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput">
        <Position x="438" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <File MaxRecords="" FileFormat="19">\\server\output\active_customers.xlsx</File>
          <Passwords/>
        </Configuration>
        <Annotation DisplayMode="0">
          <Name>Output Active Customers</Name>
          <DefaultAnnotationText>active_customers.xlsx</DefaultAnnotationText>
          <Left value="False"/>
        </Annotation>
      </Properties>
      <EngineSettings EngineDll="AlteryxBasePluginsEngine.dll" EngineDllEntryPoint="AlteryxDbFileOutput"/>
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
  <Properties>
    <Memory default="True"/>
    <GlobalRecordLimit value="0"/>
    <TempFiles default="True"/>
    <Annotation on="True" includeToolName="False"/>
    <ConvErrorLimit value="10"/>
    <ConvErrorLimit_Stop value="False"/>
    <CancelOnError value="False"/>
    <DisableBrowse value="False"/>
    <EnablePerformanceProfiling value="False"/>
    <RunWithE2 value="True"/>
    <PredssionErrorLimit value="10"/>
    <DataThefpowerful value="1048576"/>
    <MetaInfo>
      <NameIsFileName value="True"/>
      <Name>simple_filter</Name>
      <Description/>
      <RootToolName/>
      <ToolVersion/>
      <ToolInDb value="False"/>
      <CategoryName/>
      <SearchTags/>
      <Author/>
      <Company/>
      <Copyright/>
      <DescriptionLink actual="" displayed=""/>
    </MetaInfo>
  </Properties>
</AlteryxDocument>
```

**Step 2: Commit**

```bash
git add tests/fixtures/simple_filter.yxmd
git commit -m "test: add simple filter workflow fixture"
```

---

## Phase 2: XML Parser

### Task 4: Build the XML parser

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/parser/xml_parser.py`
- Test: `alteryx2dbx/tests/test_parser.py`

**Step 1: Write failing tests**

```python
# tests/test_parser.py
from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow
from alteryx2dbx.parser.xml_parser import parse_yxmd


FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_simple_filter_returns_workflow():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert isinstance(wf, AlteryxWorkflow)
    assert wf.name == "simple_filter"


def test_parse_simple_filter_tools():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert len(wf.tools) == 3
    assert 1 in wf.tools
    assert 2 in wf.tools
    assert 3 in wf.tools


def test_parse_tool_types():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert wf.tools[1].tool_type == "DbFileInput"
    assert wf.tools[2].tool_type == "Filter"
    assert wf.tools[3].tool_type == "DbFileOutput"


def test_parse_tool_plugins():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert "DbFileInput" in wf.tools[1].plugin
    assert "Filter" in wf.tools[2].plugin


def test_parse_tool_annotations():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert wf.tools[1].annotation == "Input Customers"
    assert wf.tools[2].annotation == "Filter Active High Rev"


def test_parse_connections():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert len(wf.connections) == 2
    conn1 = wf.connections[0]
    assert conn1.source_tool_id == 1
    assert conn1.source_anchor == "Output"
    assert conn1.target_tool_id == 2
    assert conn1.target_anchor == "Input"


def test_parse_filter_config():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    filter_tool = wf.tools[2]
    assert "expression" in filter_tool.config
    assert "active" in filter_tool.config["expression"]


def test_parse_input_fields():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    input_tool = wf.tools[1]
    assert len(input_tool.output_fields) == 4
    field_names = [f.name for f in input_tool.output_fields]
    assert "customer_id" in field_names
    assert "revenue" in field_names


def test_parse_input_config_file_path():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    input_tool = wf.tools[1]
    assert "file_path" in input_tool.config
    assert "customers.xlsx" in input_tool.config["file_path"]


def test_parse_nonexistent_file_raises():
    import pytest
    with pytest.raises(FileNotFoundError):
        parse_yxmd(Path("nonexistent.yxmd"))
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_parser.py -v
```

Expected: FAIL — `ImportError: cannot import name 'parse_yxmd'`

**Step 3: Write xml_parser.py**

```python
# src/alteryx2dbx/parser/xml_parser.py
from __future__ import annotations

from pathlib import Path
from xml.etree import ElementTree as ET

from .models import (
    AlteryxConnection,
    AlteryxField,
    AlteryxTool,
    AlteryxWorkflow,
)


def parse_yxmd(path: Path) -> AlteryxWorkflow:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Workflow file not found: {path}")

    tree = ET.parse(path)
    root = tree.getroot()

    tools = _parse_nodes(root)
    connections = _parse_connections(root)
    properties = _parse_properties(root)
    name = _extract_workflow_name(root, path)
    version = root.get("yxmdVer", "unknown")

    return AlteryxWorkflow(
        name=name,
        version=version,
        tools=tools,
        connections=connections,
        properties=properties,
    )


def _parse_nodes(root: ET.Element) -> dict[int, AlteryxTool]:
    tools: dict[int, AlteryxTool] = {}
    nodes_elem = root.find("Nodes")
    if nodes_elem is None:
        return tools

    for node in nodes_elem.findall("Node"):
        tool_id = int(node.get("ToolID", "0"))
        plugin = _extract_plugin(node)
        tool_type = _extract_tool_type(plugin)
        config = _extract_config(node, tool_type)
        annotation = _extract_annotation(node)
        output_fields = _extract_fields(node)

        tools[tool_id] = AlteryxTool(
            tool_id=tool_id,
            plugin=plugin,
            tool_type=tool_type,
            config=config,
            annotation=annotation,
            input_fields=[],
            output_fields=output_fields,
        )

    return tools


def _extract_plugin(node: ET.Element) -> str:
    gui = node.find("GuiSettings")
    if gui is not None:
        return gui.get("Plugin", "")
    return ""


def _extract_tool_type(plugin: str) -> str:
    if not plugin:
        return "Unknown"
    parts = plugin.split(".")
    if len(parts) >= 3:
        return parts[-1]
    return parts[-1] if parts else "Unknown"


def _extract_config(node: ET.Element, tool_type: str) -> dict:
    config: dict = {}
    config_elem = node.find(".//Configuration")
    if config_elem is None:
        return config

    # Expression (Filter, Formula)
    expr_elem = config_elem.find("Expression")
    if expr_elem is not None and expr_elem.text:
        config["expression"] = expr_elem.text

    # Mode (Filter)
    mode_elem = config_elem.find("Mode")
    if mode_elem is not None and mode_elem.text:
        config["mode"] = mode_elem.text

    # File path (Input/Output)
    file_elem = config_elem.find("File")
    if file_elem is not None and file_elem.text:
        config["file_path"] = file_elem.text
        if file_elem.get("FileFormat"):
            config["file_format"] = file_elem.get("FileFormat")

    # Formula fields
    formula_fields = config_elem.find("FormulaFields")
    if formula_fields is not None:
        config["formula_fields"] = []
        for ff in formula_fields.findall("FormulaField"):
            config["formula_fields"].append({
                "field": ff.get("field", ""),
                "expression": ff.get("expression", ""),
                "type": ff.get("type", ""),
                "size": ff.get("size", ""),
            })

    # Join info
    join_info = config_elem.find("JoinInfo")
    if join_info is not None:
        config["join_type"] = join_info.get("connection", "")
        config["join_fields"] = []
        for jf in join_info.findall(".//Field"):
            config["join_fields"].append({
                "left": jf.get("left", ""),
                "right": jf.get("right", ""),
            })

    # Select fields
    select_fields_elem = config_elem.find("SelectFields")
    if select_fields_elem is not None:
        config["select_fields"] = []
        for sf in select_fields_elem.findall("SelectField"):
            config["select_fields"].append({
                "field": sf.get("field", ""),
                "selected": sf.get("selected", "True"),
                "rename": sf.get("rename", ""),
                "type": sf.get("type", ""),
                "size": sf.get("size", ""),
            })

    # Summarize fields
    summarize_fields = config_elem.find("SummarizeFields")
    if summarize_fields is not None:
        config["summarize_fields"] = []
        for sf in summarize_fields.findall("SummarizeField"):
            config["summarize_fields"].append({
                "field": sf.get("field", ""),
                "action": sf.get("action", ""),
                "rename": sf.get("rename", ""),
            })

    # Sort info
    sort_info = config_elem.find("SortInfo")
    if sort_info is not None:
        config["sort_fields"] = []
        for sf in sort_info.findall("Field"):
            config["sort_fields"].append({
                "field": sf.get("field", ""),
                "order": sf.get("order", "Ascending"),
            })

    # Store raw XML for unsupported tools
    config["_raw_xml"] = ET.tostring(config_elem, encoding="unicode")

    return config


def _extract_annotation(node: ET.Element) -> str:
    ann = node.find(".//Annotation")
    if ann is not None:
        name = ann.find("Name")
        if name is not None and name.text:
            return name.text
    return ""


def _extract_fields(node: ET.Element) -> list[AlteryxField]:
    fields: list[AlteryxField] = []
    for meta in node.findall(".//MetaInfo"):
        record_info = meta.find("RecordInfo")
        if record_info is None:
            continue
        for f in record_info.findall("Field"):
            fields.append(AlteryxField(
                name=f.get("name", ""),
                type=f.get("type", ""),
                size=int(f.get("size")) if f.get("size") else None,
                scale=int(f.get("scale")) if f.get("scale") else None,
            ))
    return fields


def _parse_connections(root: ET.Element) -> list[AlteryxConnection]:
    connections: list[AlteryxConnection] = []
    conns_elem = root.find("Connections")
    if conns_elem is None:
        return connections

    for conn in conns_elem.findall("Connection"):
        origin = conn.find("Origin")
        dest = conn.find("Destination")
        if origin is not None and dest is not None:
            connections.append(AlteryxConnection(
                source_tool_id=int(origin.get("ToolID", "0")),
                source_anchor=origin.get("Connection", "Output"),
                target_tool_id=int(dest.get("ToolID", "0")),
                target_anchor=dest.get("Connection", "Input"),
            ))

    return connections


def _parse_properties(root: ET.Element) -> dict:
    props: dict = {}
    props_elem = root.find("Properties")
    if props_elem is None:
        return props

    for child in props_elem:
        if child.tag == "MetaInfo":
            continue
        if child.text and child.text.strip():
            props[child.tag] = child.text
        elif child.attrib:
            props[child.tag] = dict(child.attrib)

    return props


def _extract_workflow_name(root: ET.Element, path: Path) -> str:
    name_elem = root.find(".//MetaInfo/Name")
    if name_elem is not None and name_elem.text:
        return name_elem.text
    return path.stem
```

**Step 4: Run tests**

```bash
pytest tests/test_parser.py -v
```

Expected: All PASS

**Step 5: Commit**

```bash
git add src/alteryx2dbx/parser/xml_parser.py tests/test_parser.py
git commit -m "feat: XML parser for .yxmd files"
```

---

## Phase 3: DAG Resolver

### Task 5: Build DAG resolver with topological sort

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/dag/__init__.py`
- Create: `alteryx2dbx/src/alteryx2dbx/dag/resolver.py`
- Test: `alteryx2dbx/tests/test_dag.py`

**Step 1: Write failing tests**

```python
# tests/test_dag.py
import pytest
from pathlib import Path

from alteryx2dbx.parser.xml_parser import parse_yxmd
from alteryx2dbx.dag.resolver import resolve_dag, CyclicWorkflowError


FIXTURES = Path(__file__).parent / "fixtures"


def test_resolve_simple_filter_order():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    order = resolve_dag(wf)
    assert order == [1, 2, 3]


def test_resolve_returns_all_tools():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    order = resolve_dag(wf)
    assert set(order) == {1, 2, 3}


def test_resolve_respects_dependencies():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    order = resolve_dag(wf)
    assert order.index(1) < order.index(2)
    assert order.index(2) < order.index(3)


def test_resolve_empty_workflow():
    from alteryx2dbx.parser.models import AlteryxWorkflow
    wf = AlteryxWorkflow(name="empty", version="1.0")
    order = resolve_dag(wf)
    assert order == []
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_dag.py -v
```

Expected: FAIL — `ImportError`

**Step 3: Write resolver.py**

```python
# src/alteryx2dbx/dag/resolver.py
from __future__ import annotations

import networkx as nx

from alteryx2dbx.parser.models import AlteryxWorkflow


class CyclicWorkflowError(Exception):
    pass


def resolve_dag(workflow: AlteryxWorkflow) -> list[int]:
    if not workflow.tools:
        return []

    g = nx.DiGraph()

    for tool_id in workflow.tools:
        g.add_node(tool_id)

    for conn in workflow.connections:
        g.add_edge(conn.source_tool_id, conn.target_tool_id)

    if not nx.is_directed_acyclic_graph(g):
        cycles = list(nx.simple_cycles(g))
        raise CyclicWorkflowError(
            f"Workflow contains cycles: {cycles}"
        )

    return list(nx.topological_sort(g))
```

**Step 4: Create __init__.py**

```python
# src/alteryx2dbx/dag/__init__.py
```

**Step 5: Run tests**

```bash
pytest tests/test_dag.py -v
```

Expected: All PASS

**Step 6: Commit**

```bash
git add src/alteryx2dbx/dag/ tests/test_dag.py
git commit -m "feat: DAG resolver with topological sort"
```

---

## Phase 4: Expression Transpiler

### Task 6: Write the Lark grammar for Alteryx expressions

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/transpiler/__init__.py`
- Create: `alteryx2dbx/src/alteryx2dbx/transpiler/grammar.lark`
- Create: `alteryx2dbx/src/alteryx2dbx/transpiler/expression_parser.py`
- Test: `alteryx2dbx/tests/test_transpiler_parse.py`

**Step 1: Write failing tests**

```python
# tests/test_transpiler_parse.py
import pytest
from alteryx2dbx.transpiler.expression_parser import parse_expression


def test_parse_number():
    tree = parse_expression("42")
    assert tree is not None


def test_parse_string():
    tree = parse_expression('"hello"')
    assert tree is not None


def test_parse_field_ref():
    tree = parse_expression("[Revenue]")
    assert tree is not None


def test_parse_comparison():
    tree = parse_expression("[Revenue] > 100")
    assert tree is not None


def test_parse_and():
    tree = parse_expression('[status] = "active" AND [revenue] > 100')
    assert tree is not None


def test_parse_or():
    tree = parse_expression('[a] = 1 OR [b] = 2')
    assert tree is not None


def test_parse_if_then_else():
    tree = parse_expression('IF [Revenue] > 1000 THEN "High" ELSE "Low" ENDIF')
    assert tree is not None


def test_parse_if_elseif():
    tree = parse_expression(
        'IF [x] > 100 THEN "A" ELSEIF [x] > 50 THEN "B" ELSE "C" ENDIF'
    )
    assert tree is not None


def test_parse_function_call():
    tree = parse_expression("Contains([Name], \"Smith\")")
    assert tree is not None


def test_parse_nested_function():
    tree = parse_expression('ToString(Round([Revenue] * 1.1, 2))')
    assert tree is not None


def test_parse_iif():
    tree = parse_expression('IIF([x] > 0, "pos", "neg")')
    assert tree is not None


def test_parse_null():
    tree = parse_expression("NULL()")
    assert tree is not None


def test_parse_isnull():
    tree = parse_expression("IsNull([Revenue])")
    assert tree is not None


def test_parse_arithmetic():
    tree = parse_expression("[price] * [quantity] - [discount]")
    assert tree is not None


def test_parse_string_concat():
    tree = parse_expression('[first] + " " + [last]')
    assert tree is not None


def test_parse_not():
    tree = parse_expression("NOT [is_deleted]")
    assert tree is not None


def test_parse_row_ref():
    tree = parse_expression("[Row-1:Revenue]")
    assert tree is not None


def test_parse_negative_number():
    tree = parse_expression("-1.5")
    assert tree is not None


def test_parse_boolean_true():
    tree = parse_expression("True")
    assert tree is not None


def test_parse_parenthesized():
    tree = parse_expression("([a] + [b]) * [c]")
    assert tree is not None
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_transpiler_parse.py -v
```

Expected: FAIL

**Step 3: Write the Lark grammar**

```lark
// src/alteryx2dbx/transpiler/grammar.lark

start: expr

// Expressions — precedence from lowest to highest
?expr: or_expr

?or_expr: and_expr
    | or_expr OR and_expr    -> or_op

?and_expr: not_expr
    | and_expr AND not_expr  -> and_op

?not_expr: comparison
    | NOT not_expr           -> not_op

?comparison: addition
    | comparison "=" addition   -> eq
    | comparison "==" addition  -> eq
    | comparison "!=" addition  -> neq
    | comparison "<>" addition  -> neq
    | comparison ">" addition   -> gt
    | comparison ">=" addition  -> gte
    | comparison "<" addition   -> lt
    | comparison "<=" addition  -> lte

?addition: multiplication
    | addition "+" multiplication  -> add
    | addition "-" multiplication  -> sub

?multiplication: unary
    | multiplication "*" unary  -> mul
    | multiplication "/" unary  -> div
    | multiplication "%" unary  -> mod

?unary: atom
    | "-" atom  -> neg

?atom: NUMBER                     -> number
    | ESCAPED_STRING              -> string
    | "True"i                     -> true
    | "False"i                    -> false
    | "NULL" "(" ")"              -> null
    | field_ref
    | row_ref
    | function_call
    | if_expr
    | "(" expr ")"

field_ref: "[" FIELD_NAME "]"
row_ref: "[" ROW_REF_PATTERN "]"
function_call: FUNC_NAME "(" args? ")"
args: expr ("," expr)*

if_expr: IF expr THEN expr elseif_clause* else_clause? ENDIF

elseif_clause: ELSEIF expr THEN expr
else_clause: ELSE expr

// Keywords (case-insensitive)
IF: "IF"i
THEN: "THEN"i
ELSEIF: "ELSEIF"i
ELSE: "ELSE"i
ENDIF: "ENDIF"i
AND: "AND"i | "&&"
OR: "OR"i | "||"
NOT: "NOT"i | "!"

// Tokens
FUNC_NAME: /[A-Za-z_][A-Za-z0-9_]*/
FIELD_NAME: /[^[\]]+/
ROW_REF_PATTERN: /Row-?\d+:[^[\]]+/

%import common.NUMBER
%import common.ESCAPED_STRING
%import common.WS
%ignore WS
```

**Step 4: Write expression_parser.py**

```python
# src/alteryx2dbx/transpiler/expression_parser.py
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
```

**Step 5: Create __init__.py**

```python
# src/alteryx2dbx/transpiler/__init__.py
```

**Step 6: Run tests and iterate on grammar until all pass**

```bash
pytest tests/test_transpiler_parse.py -v
```

Expected: All PASS (may need grammar tweaks — the Lark grammar above handles the common cases but edge cases like `ROW_REF_PATTERN` vs `FIELD_NAME` token priority may need adjustment)

**Step 7: Commit**

```bash
git add src/alteryx2dbx/transpiler/ tests/test_transpiler_parse.py
git commit -m "feat: Lark grammar and parser for Alteryx expressions"
```

---

### Task 7: Build the PySpark expression emitter

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/transpiler/expression_emitter.py`
- Test: `alteryx2dbx/tests/test_transpiler_emit.py`

**Step 1: Write failing tests**

```python
# tests/test_transpiler_emit.py
from alteryx2dbx.transpiler.expression_emitter import transpile_expression


def test_emit_number():
    assert transpile_expression("42") == "F.lit(42)"


def test_emit_string():
    assert transpile_expression('"hello"') == 'F.lit("hello")'


def test_emit_field_ref():
    assert transpile_expression("[Revenue]") == 'F.col("Revenue")'


def test_emit_comparison_gt():
    result = transpile_expression("[Revenue] > 100")
    assert 'F.col("Revenue") > F.lit(100)' == result


def test_emit_eq_case_insensitive():
    # Alteryx string = is case-insensitive
    result = transpile_expression('[status] = "active"')
    assert "lower" in result
    assert "eqNullSafe" in result or "==" in result


def test_emit_and():
    result = transpile_expression("[a] > 1 AND [b] > 2")
    assert "&" in result


def test_emit_or():
    result = transpile_expression("[a] > 1 OR [b] > 2")
    assert "|" in result


def test_emit_not():
    result = transpile_expression("NOT [deleted]")
    assert "~" in result


def test_emit_if_then_else():
    result = transpile_expression('IF [x] > 100 THEN "High" ELSE "Low" ENDIF')
    assert "F.when" in result
    assert "otherwise" in result


def test_emit_if_elseif():
    result = transpile_expression(
        'IF [x] > 100 THEN "A" ELSEIF [x] > 50 THEN "B" ELSE "C" ENDIF'
    )
    assert result.count("F.when") + result.count(".when(") >= 2


def test_emit_isnull():
    result = transpile_expression("IsNull([Revenue])")
    assert "isNull" in result


def test_emit_contains():
    result = transpile_expression('Contains([Name], "Smith")')
    assert "contains" in result


def test_emit_null():
    result = transpile_expression("NULL()")
    assert "F.lit(None)" in result


def test_emit_arithmetic():
    result = transpile_expression("[price] * [quantity]")
    assert 'F.col("price") * F.col("quantity")' == result


def test_emit_iif():
    result = transpile_expression('IIF([x] > 0, "pos", "neg")')
    assert "F.when" in result
    assert "otherwise" in result


def test_emit_round():
    result = transpile_expression("Round([Revenue], 2)")
    assert "F.round" in result


def test_emit_trim():
    result = transpile_expression("Trim([Name])")
    assert "F.trim" in result


def test_emit_length():
    result = transpile_expression("Length([Name])")
    assert "F.length" in result


def test_emit_left():
    result = transpile_expression("Left([Name], 3)")
    assert "F.substring" in result


def test_emit_uppercase():
    result = transpile_expression("Uppercase([Name])")
    assert "F.upper" in result


def test_emit_substring_zero_based():
    # Alteryx Substring is 0-based, Spark is 1-based
    result = transpile_expression("Substring([Name], 0, 3)")
    assert "F.substring" in result
    # Should offset the start by +1
    assert ", 1," in result


def test_emit_tostring():
    result = transpile_expression("ToString([Revenue])")
    assert "cast" in result and "string" in result


def test_emit_tonumber():
    result = transpile_expression("ToNumber([Price])")
    assert "cast" in result and "double" in result
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_transpiler_emit.py -v
```

Expected: FAIL

**Step 3: Write expression_emitter.py**

```python
# src/alteryx2dbx/transpiler/expression_emitter.py
from __future__ import annotations

from lark import Transformer, v_args

from .expression_parser import parse_expression


# Functions that take string args — need case-insensitive comparison fix
_STRING_COMPARE_FUNCS = {"Contains", "StartsWith", "EndsWith", "FindString"}

# Function name → PySpark mapping
_FUNC_MAP: dict[str, str | None] = {
    # String
    "Contains": None,  # special handling
    "StartsWith": None,  # special handling
    "EndsWith": None,  # special handling
    "Trim": "F.trim",
    "TrimLeft": "F.ltrim",
    "TrimRight": "F.rtrim",
    "Length": "F.length",
    "Uppercase": "F.upper",
    "Lowercase": "F.lower",
    "Left": None,  # special: F.substring(col, 1, n)
    "Right": None,  # special: F.substring(col, -n, n)
    "Substring": None,  # special: 0-based → 1-based
    "FindString": None,  # special: F.locate() - 1
    "PadLeft": "F.lpad",
    "PadRight": "F.rpad",
    "ReplaceChar": "F.translate",
    "Regex_Replace": "F.regexp_replace",
    "Regex_Match": None,  # special: .rlike()
    "GetWord": None,  # special: F.split().getItem()
    # Numeric
    "Round": "F.round",
    "Ceil": "F.ceil",
    "Floor": "F.floor",
    "Abs": "F.abs",
    "Pow": "F.pow",
    "Log": "F.log",
    "Log10": "F.log10",
    "Sqrt": "F.sqrt",
    # Conversion
    "ToString": None,  # special: .cast("string")
    "ToNumber": None,  # special: .cast("double")
    # Test
    "IsNull": None,  # special: .isNull()
    "IsEmpty": None,  # special: == F.lit("")
    # Conditional
    "IIF": None,  # special: F.when().otherwise()
    "Switch": None,  # special: chained F.when()
    # Min/Max
    "Min": "F.least",
    "Max": "F.greatest",
    # DateTime
    "DateTimeParse": None,  # special
    "DateTimeFormat": None,  # special
    "DateTimeNow": None,  # special: F.current_timestamp()
}


class PySparkEmitter(Transformer):

    def start(self, args):
        return args[0]

    def number(self, args):
        val = args[0]
        num = float(val) if "." in str(val) else int(val)
        return f"F.lit({num})"

    def string(self, args):
        val = args[0]
        # Lark ESCAPED_STRING includes quotes
        return f"F.lit({val})"

    def true(self, args):
        return "F.lit(True)"

    def false(self, args):
        return "F.lit(False)"

    def null(self, args):
        return "F.lit(None)"

    def field_ref(self, args):
        name = str(args[0]).strip()
        return f'F.col("{name}")'

    def row_ref(self, args):
        raw = str(args[0]).strip()
        # [Row-1:Revenue] → needs window function
        parts = raw.split(":")
        offset = parts[0].replace("Row", "")
        field = parts[1] if len(parts) > 1 else ""
        return f'F.lag(F.col("{field}"), {abs(int(offset))}).over(window_spec)  # TODO: define window_spec'

    # Binary operators
    def add(self, args):
        return f"({args[0]} + {args[1]})"

    def sub(self, args):
        return f"({args[0]} - {args[1]})"

    def mul(self, args):
        return f"({args[0]} * {args[1]})"

    def div(self, args):
        return f"({args[0]} / {args[1]})"

    def mod(self, args):
        return f"({args[0]} % {args[1]})"

    def neg(self, args):
        return f"(-{args[0]})"

    # Comparison
    def eq(self, args):
        left, right = args
        # Case-insensitive for string comparisons
        if "F.lit(" in right and '"' in right:
            val = right.split('"')[1]
            return f'(F.lower({left}) == F.lit("{val.lower()}"))'
        return f"({left}.eqNullSafe({right}))"

    def neq(self, args):
        return f"({args[0]} != {args[1]})"

    def gt(self, args):
        return f"({args[0]} > {args[1]})"

    def gte(self, args):
        return f"({args[0]} >= {args[1]})"

    def lt(self, args):
        return f"({args[0]} < {args[1]})"

    def lte(self, args):
        return f"({args[0]} <= {args[1]})"

    # Logical
    def and_op(self, args):
        return f"({args[0]} & {args[1]})"

    def or_op(self, args):
        return f"({args[0]} | {args[1]})"

    def not_op(self, args):
        return f"(~{args[0]})"

    # Conditional
    def if_expr(self, args):
        # args: condition, then_val, [elseif_clauses...], [else_val]
        parts = list(args)
        condition = parts[0]
        then_val = parts[1]
        result = f"F.when({condition}, {then_val})"

        i = 2
        while i < len(parts):
            item = parts[i]
            if isinstance(item, tuple) and item[0] == "elseif":
                result += f".when({item[1]}, {item[2]})"
            elif isinstance(item, tuple) and item[0] == "else":
                result += f".otherwise({item[1]})"
            i += 1

        return result

    def elseif_clause(self, args):
        return ("elseif", args[0], args[1])

    def else_clause(self, args):
        return ("else", args[0])

    # Function calls
    def function_call(self, args):
        func_name = str(args[0])
        func_args = args[1] if len(args) > 1 else []
        if isinstance(func_args, str):
            func_args = [func_args]

        return self._emit_function(func_name, func_args)

    def args(self, args):
        return list(args)

    def _emit_function(self, name: str, args: list[str]) -> str:
        # Special-case functions
        if name == "IsNull":
            return f"{args[0]}.isNull()"
        if name == "IsEmpty":
            return f"({args[0]} == F.lit(\"\"))"
        if name == "IIF":
            cond, true_val = args[0], args[1]
            false_val = args[2] if len(args) > 2 else "F.lit(None)"
            return f"F.when({cond}, {true_val}).otherwise({false_val})"
        if name == "ToString":
            return f"{args[0]}.cast(\"string\")"
        if name == "ToNumber":
            return f"{args[0]}.cast(\"double\")"
        if name == "Contains":
            return f"F.lower({args[0]}).contains({args[1]}.lower())" if "F.lit" not in str(args[1]) else f"{args[0]}.contains({args[1]})"
        if name == "StartsWith":
            return f"{args[0]}.startswith({args[1]})"
        if name == "EndsWith":
            return f"{args[0]}.endswith({args[1]})"
        if name == "Left":
            return f"F.substring({args[0]}, 1, {args[1]})"
        if name == "Right":
            n = args[1]
            return f"F.substring({args[0]}, -{n}, {n})"
        if name == "Substring":
            # 0-based to 1-based
            start = args[1]
            length = args[2] if len(args) > 2 else "F.lit(256)"
            if "F.lit(" in start:
                num = int(start.replace("F.lit(", "").replace(")", ""))
                start = f"F.lit({num + 1})"
            else:
                start = f"({start} + F.lit(1))"
            return f"F.substring({args[0]}, {start}, {length})"
        if name == "FindString":
            return f"(F.locate({args[1]}, {args[0]}) - 1)"
        if name == "Regex_Match":
            return f"{args[0]}.rlike({args[1]})"
        if name == "GetWord":
            return f'F.split({args[0]}, F.lit(" ")).getItem({args[1]})'
        if name == "DateTimeNow":
            return "F.current_timestamp()"

        # Standard mapped functions
        pyspark_func = _FUNC_MAP.get(name)
        if pyspark_func:
            args_str = ", ".join(args)
            return f"{pyspark_func}({args_str})"

        # Unknown function — pass through with warning
        args_str = ", ".join(args)
        return f"# TODO: unmapped function {name}({args_str})"


def transpile_expression(expr: str) -> str:
    tree = parse_expression(expr)
    emitter = PySparkEmitter()
    return emitter.transform(tree)
```

**Step 4: Run tests and iterate**

```bash
pytest tests/test_transpiler_emit.py -v
```

Expected: Most PASS. Some may need emitter tweaks based on exact output format. Iterate until all pass.

**Step 5: Commit**

```bash
git add src/alteryx2dbx/transpiler/expression_emitter.py tests/test_transpiler_emit.py
git commit -m "feat: PySpark expression emitter with semantic fixes"
```

---

## Phase 5: Tool Handlers (Phase 1 — Must-Have)

### Task 8: Handler base class + registry + unsupported handler

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/__init__.py`
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/base.py`
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/registry.py`
- Test: `alteryx2dbx/tests/test_handlers/__init__.py`
- Test: `alteryx2dbx/tests/test_handlers/test_registry.py`

**Step 1: Write failing tests**

```python
# tests/test_handlers/test_registry.py
import pytest
from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep
from alteryx2dbx.handlers.registry import get_handler, HandlerRegistry
from alteryx2dbx.handlers.base import ToolHandler


def test_unsupported_tool_returns_passthrough():
    tool = AlteryxTool(
        tool_id=99,
        plugin="SomeCustomPlugin.Foo.Bar",
        tool_type="Bar",
        config={"_raw_xml": "<Configuration/>"},
        annotation="Custom Tool",
    )
    handler = get_handler(tool)
    assert handler is not None


def test_unsupported_tool_generates_passthrough_code():
    tool = AlteryxTool(
        tool_id=99,
        plugin="SomeCustomPlugin.Foo.Bar",
        tool_type="Bar",
        config={"_raw_xml": "<Configuration/>"},
        annotation="Custom Tool",
    )
    handler = get_handler(tool)
    step = handler.convert(tool, input_df_names=["df_50"])
    assert "UNSUPPORTED" in step.code or "TODO" in step.code
    assert step.confidence == 0.0


def test_registry_returns_handler_for_known_tool():
    tool = AlteryxTool(
        tool_id=1,
        plugin="AlteryxBasePluginsGui.Filter.Filter",
        tool_type="Filter",
        config={"expression": "[x] > 1"},
    )
    handler = get_handler(tool)
    assert handler is not None
```

**Step 2: Write base.py**

```python
# src/alteryx2dbx/handlers/base.py
from __future__ import annotations

from abc import ABC, abstractmethod

from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep


class ToolHandler(ABC):

    @abstractmethod
    def convert(
        self,
        tool: AlteryxTool,
        input_df_names: list[str] | None = None,
    ) -> GeneratedStep:
        ...


class UnsupportedHandler(ToolHandler):

    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        raw_xml = tool.config.get("_raw_xml", "<!-- no config -->")
        code = (
            f"# ⚠️ UNSUPPORTED TOOL: {tool.tool_type} (Tool ID: {tool.tool_id})\n"
            f"# Annotation: {tool.annotation}\n"
            f"# Original config:\n"
            + "\n".join(f"# {line}" for line in raw_xml.split("\n"))
            + f"\n# TODO: Implement this transformation manually\n"
            f"df_{tool.tool_id} = {input_df}  # PASSTHROUGH"
        )
        return GeneratedStep(
            step_name=f"unsupported_{tool.tool_type.lower()}_{tool.tool_id}",
            code=code,
            imports=set(),
            input_dfs=[input_df],
            output_df=f"df_{tool.tool_id}",
            notes=[f"Unsupported tool: {tool.tool_type} ({tool.plugin})"],
            confidence=0.0,
        )
```

**Step 3: Write registry.py**

```python
# src/alteryx2dbx/handlers/registry.py
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool
from .base import ToolHandler, UnsupportedHandler


class HandlerRegistry:
    def __init__(self):
        self._handlers: dict[str, type[ToolHandler]] = {}
        self._type_handlers: dict[str, type[ToolHandler]] = {}

    def register(self, plugin: str, handler_cls: type[ToolHandler]):
        self._handlers[plugin] = handler_cls

    def register_type(self, tool_type: str, handler_cls: type[ToolHandler]):
        self._type_handlers[tool_type] = handler_cls

    def get(self, tool: AlteryxTool) -> ToolHandler:
        # Try exact plugin match first
        handler_cls = self._handlers.get(tool.plugin)
        if handler_cls:
            return handler_cls()

        # Try tool_type match
        handler_cls = self._type_handlers.get(tool.tool_type)
        if handler_cls:
            return handler_cls()

        return UnsupportedHandler()


_registry = HandlerRegistry()


def get_handler(tool: AlteryxTool) -> ToolHandler:
    return _registry.get(tool)


def register_handler(plugin: str, handler_cls: type[ToolHandler]):
    _registry.register(plugin, handler_cls)


def register_type_handler(tool_type: str, handler_cls: type[ToolHandler]):
    _registry.register_type(tool_type, handler_cls)
```

**Step 4: Run tests**

```bash
pytest tests/test_handlers/ -v
```

Expected: First two pass, third fails (Filter handler not registered yet — that's OK, we'll register in next task)

**Step 5: Commit**

```bash
git add src/alteryx2dbx/handlers/ tests/test_handlers/
git commit -m "feat: handler base class, registry, and unsupported fallback"
```

---

### Task 9: InputData handler

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/input_data.py`
- Test: `alteryx2dbx/tests/test_handlers/test_input_data.py`

**Step 1: Write failing tests**

```python
# tests/test_handlers/test_input_data.py
from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep
from alteryx2dbx.handlers.input_data import InputDataHandler


def test_input_excel():
    tool = AlteryxTool(
        tool_id=1,
        plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput",
        tool_type="DbFileInput",
        config={"file_path": "\\\\server\\data\\customers.xlsx", "file_format": "19"},
        annotation="Input Customers",
    )
    handler = InputDataHandler()
    step = handler.convert(tool)
    assert "spark.read" in step.code
    assert step.output_df == "df_1"
    assert step.confidence == 1.0
    assert "customers.xlsx" in step.code


def test_input_csv():
    tool = AlteryxTool(
        tool_id=2,
        plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput",
        tool_type="DbFileInput",
        config={"file_path": "/data/sales.csv", "file_format": "0"},
        annotation="Input Sales",
    )
    handler = InputDataHandler()
    step = handler.convert(tool)
    assert "spark.read" in step.code
    assert "csv" in step.code


def test_input_has_config_comment():
    tool = AlteryxTool(
        tool_id=1,
        plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput",
        tool_type="DbFileInput",
        config={"file_path": "/data/test.xlsx", "file_format": "19"},
    )
    handler = InputDataHandler()
    step = handler.convert(tool)
    # Should include a comment about where to update the path
    assert "TODO" in step.code or "UPDATE" in step.code or "file_path" in step.notes[0] if step.notes else True
```

**Step 2: Write handler**

```python
# src/alteryx2dbx/handlers/input_data.py
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep
from .base import ToolHandler
from .registry import register_type_handler


# Alteryx FileFormat codes
_FORMAT_MAP = {
    "0": "csv",
    "19": "excel",
    "25": "parquet",
}


class InputDataHandler(ToolHandler):

    def convert(self, tool, input_df_names=None):
        file_path = tool.config.get("file_path", "UNKNOWN_PATH")
        file_format = tool.config.get("file_format", "0")
        fmt = _FORMAT_MAP.get(file_format, "csv")

        notes = [f"Update file_path to Databricks-accessible location: {file_path}"]

        if fmt == "excel":
            code = (
                f"# {tool.annotation or 'Input Data'} (Tool {tool.tool_id})\n"
                f'# Original path: {file_path}\n'
                f'# TODO: Update path to Databricks-accessible location\n'
                f'df_{tool.tool_id} = spark.read.format("com.crealytics.spark.excel") \\\n'
                f'    .option("header", "true") \\\n'
                f'    .option("inferSchema", "true") \\\n'
                f'    .load("{file_path}")'
            )
            imports = {"# Requires: com.crealytics:spark-excel library"}
        elif fmt == "parquet":
            code = (
                f"# {tool.annotation or 'Input Data'} (Tool {tool.tool_id})\n"
                f'df_{tool.tool_id} = spark.read.parquet("{file_path}")'
            )
            imports = set()
        else:
            code = (
                f"# {tool.annotation or 'Input Data'} (Tool {tool.tool_id})\n"
                f'# Original path: {file_path}\n'
                f'# TODO: Update path to Databricks-accessible location\n'
                f'df_{tool.tool_id} = spark.read.format("csv") \\\n'
                f'    .option("header", "true") \\\n'
                f'    .option("inferSchema", "true") \\\n'
                f'    .load("{file_path}")'
            )
            imports = set()

        return GeneratedStep(
            step_name=f"load_{tool.annotation or f'source_{tool.tool_id}'}".lower().replace(" ", "_"),
            code=code,
            imports=imports,
            input_dfs=[],
            output_df=f"df_{tool.tool_id}",
            notes=notes,
            confidence=1.0,
        )


# Register
register_type_handler("DbFileInput", InputDataHandler)
```

**Step 3: Run tests**

```bash
pytest tests/test_handlers/test_input_data.py -v
```

Expected: All PASS

**Step 4: Commit**

```bash
git add src/alteryx2dbx/handlers/input_data.py tests/test_handlers/test_input_data.py
git commit -m "feat: InputData handler for Excel/CSV/Parquet"
```

---

### Task 10: Filter handler

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/filter.py`
- Test: `alteryx2dbx/tests/test_handlers/test_filter.py`

**Step 1: Write failing tests**

```python
# tests/test_handlers/test_filter.py
from alteryx2dbx.parser.models import AlteryxTool
from alteryx2dbx.handlers.filter import FilterHandler


def test_filter_custom_expression():
    tool = AlteryxTool(
        tool_id=5,
        plugin="AlteryxBasePluginsGui.Filter.Filter",
        tool_type="Filter",
        config={"expression": '[status] = "active"', "mode": "Custom"},
        annotation="Filter Active",
    )
    handler = FilterHandler()
    step = handler.convert(tool, input_df_names=["df_1"])
    assert "filter" in step.code or "where" in step.code
    assert step.output_df == "df_5"
    assert step.confidence == 1.0
    assert "df_1" in step.input_dfs


def test_filter_produces_true_and_false_outputs():
    tool = AlteryxTool(
        tool_id=5,
        plugin="AlteryxBasePluginsGui.Filter.Filter",
        tool_type="Filter",
        config={"expression": "[revenue] > 100", "mode": "Custom"},
    )
    handler = FilterHandler()
    step = handler.convert(tool, input_df_names=["df_1"])
    # Filter should produce both True and False branches
    assert "df_5_true" in step.code or "df_5" in step.code
```

**Step 2: Write handler**

```python
# src/alteryx2dbx/handlers/filter.py
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep
from alteryx2dbx.transpiler.expression_emitter import transpile_expression
from .base import ToolHandler
from .registry import register_type_handler


class FilterHandler(ToolHandler):

    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        expression = tool.config.get("expression", "True")

        try:
            pyspark_expr = transpile_expression(expression)
            confidence = 1.0
            notes = []
        except Exception as e:
            pyspark_expr = f"# FAILED TO TRANSPILE: {expression}\n# Error: {e}"
            confidence = 0.3
            notes = [f"Expression transpilation failed: {e}"]

        code = (
            f"# {tool.annotation or 'Filter'} (Tool {tool.tool_id})\n"
            f"# Alteryx expression: {expression}\n"
            f"_filter_condition_{tool.tool_id} = {pyspark_expr}\n"
            f"df_{tool.tool_id}_true = {input_df}.filter(_filter_condition_{tool.tool_id})\n"
            f"df_{tool.tool_id}_false = {input_df}.filter(~(_filter_condition_{tool.tool_id}))\n"
            f"df_{tool.tool_id} = df_{tool.tool_id}_true  # Default: True branch"
        )

        return GeneratedStep(
            step_name=f"filter_{tool.annotation or tool.tool_id}".lower().replace(" ", "_"),
            code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df],
            output_df=f"df_{tool.tool_id}",
            notes=notes,
            confidence=confidence,
        )


register_type_handler("Filter", FilterHandler)
```

**Step 3: Run tests**

```bash
pytest tests/test_handlers/test_filter.py -v
```

**Step 4: Commit**

```bash
git add src/alteryx2dbx/handlers/filter.py tests/test_handlers/test_filter.py
git commit -m "feat: Filter handler with expression transpilation"
```

---

### Task 11: Formula handler

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/formula.py`
- Test: `alteryx2dbx/tests/test_handlers/test_formula.py`

**Step 1: Write failing tests**

```python
# tests/test_handlers/test_formula.py
from alteryx2dbx.parser.models import AlteryxTool
from alteryx2dbx.handlers.formula import FormulaHandler


def test_formula_single_field():
    tool = AlteryxTool(
        tool_id=10,
        plugin="AlteryxBasePluginsGui.Formula.Formula",
        tool_type="Formula",
        config={
            "formula_fields": [
                {"field": "revenue_with_tax", "expression": "[revenue] * 1.21", "type": "Double", "size": ""},
            ]
        },
        annotation="Calculate Tax",
    )
    handler = FormulaHandler()
    step = handler.convert(tool, input_df_names=["df_8"])
    assert "withColumn" in step.code
    assert "revenue_with_tax" in step.code
    assert step.output_df == "df_10"


def test_formula_multiple_fields():
    tool = AlteryxTool(
        tool_id=11,
        plugin="AlteryxBasePluginsGui.Formula.Formula",
        tool_type="Formula",
        config={
            "formula_fields": [
                {"field": "full_name", "expression": '[first] + " " + [last]', "type": "V_WString", "size": "512"},
                {"field": "is_vip", "expression": 'IF [revenue] > 10000 THEN "Yes" ELSE "No" ENDIF', "type": "V_WString", "size": "10"},
            ]
        },
    )
    handler = FormulaHandler()
    step = handler.convert(tool, input_df_names=["df_5"])
    assert step.code.count("withColumn") == 2
```

**Step 2: Write handler**

```python
# src/alteryx2dbx/handlers/formula.py
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep
from alteryx2dbx.transpiler.expression_emitter import transpile_expression
from .base import ToolHandler
from .registry import register_type_handler


class FormulaHandler(ToolHandler):

    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        formula_fields = tool.config.get("formula_fields", [])

        lines = [f"# {tool.annotation or 'Formula'} (Tool {tool.tool_id})"]
        lines.append(f"df_{tool.tool_id} = {input_df}")
        all_notes = []
        min_confidence = 1.0

        for ff in formula_fields:
            field_name = ff.get("field", "unknown_field")
            expression = ff.get("expression", "")

            try:
                pyspark_expr = transpile_expression(expression)
                lines.append(
                    f'df_{tool.tool_id} = df_{tool.tool_id}.withColumn("{field_name}", {pyspark_expr})'
                )
            except Exception as e:
                lines.append(f"# FAILED: {field_name} = {expression}")
                lines.append(f"# Error: {e}")
                lines.append(
                    f'# df_{tool.tool_id} = df_{tool.tool_id}.withColumn("{field_name}", ...)'
                )
                all_notes.append(f"Failed to transpile formula for {field_name}: {e}")
                min_confidence = min(min_confidence, 0.3)

        return GeneratedStep(
            step_name=f"formula_{tool.annotation or tool.tool_id}".lower().replace(" ", "_"),
            code="\n".join(lines),
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df],
            output_df=f"df_{tool.tool_id}",
            notes=all_notes,
            confidence=min_confidence,
        )


register_type_handler("Formula", FormulaHandler)
```

**Step 3: Run tests, commit**

```bash
pytest tests/test_handlers/test_formula.py -v
git add src/alteryx2dbx/handlers/formula.py tests/test_handlers/test_formula.py
git commit -m "feat: Formula handler with multi-field support"
```

---

### Task 12: Select, Join, Sort, Summarize, Union, OutputData handlers

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/select.py`
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/join.py`
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/sort.py`
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/summarize.py`
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/union.py`
- Create: `alteryx2dbx/src/alteryx2dbx/handlers/output_data.py`
- Tests for each in `alteryx2dbx/tests/test_handlers/`

Each handler follows the same pattern as Filter/Formula. Key logic per handler:

**SelectHandler:** Maps to `.select()` + `.withColumnRenamed()`. Drops deselected fields, renames where specified, reorders.

**JoinHandler:** Maps to `.join()`. Extracts join type (Inner/Left/Right/Full) and key pairs from config. Produces three output DFs: `df_N_joined`, `df_N_left_only`, `df_N_right_only`.

**SortHandler:** Maps to `.orderBy()`. Extracts field + order (Ascending/Descending) from config.

**SummarizeHandler:** Maps to `.groupBy().agg()`. Maps Alteryx actions (Sum, Count, Avg, Min, Max, First, Last, Concat, CountDistinct) to PySpark aggregate functions.

**UnionHandler:** Maps to `.union()` or `.unionByName()`. Takes multiple input DFs.

**OutputDataHandler:** Maps to `.write` or `.toPandas().to_excel()`. Extracts output file path, generates write code with a TODO for path update.

**Step 1: Write tests for each** (follow same pattern as Filter/Formula tests above)

**Step 2: Implement each handler** (follow same pattern)

**Step 3: Register all handlers** — add imports to `handlers/__init__.py`:

```python
# src/alteryx2dbx/handlers/__init__.py
# Import all handlers to trigger registration
from . import input_data  # noqa: F401
from . import output_data  # noqa: F401
from . import filter  # noqa: F401
from . import formula  # noqa: F401
from . import select  # noqa: F401
from . import join  # noqa: F401
from . import sort  # noqa: F401
from . import summarize  # noqa: F401
from . import union  # noqa: F401
```

**Step 4: Run all handler tests**

```bash
pytest tests/test_handlers/ -v
```

**Step 5: Commit**

```bash
git add src/alteryx2dbx/handlers/ tests/test_handlers/
git commit -m "feat: Phase 1 handlers — Select, Join, Sort, Summarize, Union, Output"
```

---

## Phase 6: Code Generator

### Task 13: Notebook generator (assembles .py files from GeneratedSteps)

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/generator/__init__.py`
- Create: `alteryx2dbx/src/alteryx2dbx/generator/notebook.py`
- Create: `alteryx2dbx/src/alteryx2dbx/generator/config.py`
- Create: `alteryx2dbx/src/alteryx2dbx/generator/report.py`
- Create: `alteryx2dbx/src/alteryx2dbx/generator/validator.py`
- Test: `alteryx2dbx/tests/test_generator.py`

**Step 1: Write failing tests**

```python
# tests/test_generator.py
from pathlib import Path

from alteryx2dbx.parser.xml_parser import parse_yxmd
from alteryx2dbx.generator.notebook import generate_notebooks


FIXTURES = Path(__file__).parent / "fixtures"


def test_generate_creates_output_dir(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    output_dir = tmp_path / "output"
    generate_notebooks(wf, output_dir)
    assert output_dir.exists()
    assert (output_dir / "simple_filter").exists()


def test_generate_creates_all_files(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    output_dir = tmp_path / "output"
    generate_notebooks(wf, output_dir)
    wf_dir = output_dir / "simple_filter"
    assert (wf_dir / "config.yml").exists()
    assert (wf_dir / "01_load_sources.py").exists()
    assert (wf_dir / "02_transformations.py").exists()
    assert (wf_dir / "03_orchestrate.py").exists()
    assert (wf_dir / "04_validate.py").exists()
    assert (wf_dir / "conversion_report.md").exists()


def test_generated_notebooks_have_databricks_header(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    output_dir = tmp_path / "output"
    generate_notebooks(wf, output_dir)
    wf_dir = output_dir / "simple_filter"
    content = (wf_dir / "01_load_sources.py").read_text()
    assert "# Databricks notebook source" in content


def test_generated_notebooks_have_command_separators(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    output_dir = tmp_path / "output"
    generate_notebooks(wf, output_dir)
    wf_dir = output_dir / "simple_filter"
    content = (wf_dir / "02_transformations.py").read_text()
    assert "# COMMAND ----------" in content
```

**Step 2: Write notebook.py**

This is the orchestrator that:
1. Parses the workflow
2. Resolves the DAG
3. Runs each tool through its handler
4. Classifies steps as load/transform/output
5. Writes the four .py files using Databricks notebook format

```python
# src/alteryx2dbx/generator/notebook.py
from __future__ import annotations

from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow, GeneratedStep
from alteryx2dbx.dag.resolver import resolve_dag
from alteryx2dbx.handlers.registry import get_handler
import alteryx2dbx.handlers  # noqa: F401 — triggers registration

# Tool types that are "load" steps
_INPUT_TYPES = {"DbFileInput", "TextInput", "InputData"}
# Tool types that are "output" steps
_OUTPUT_TYPES = {"DbFileOutput", "OutputData", "Browse"}

NOTEBOOK_HEADER = "# Databricks notebook source"
COMMAND_SEP = "\n# COMMAND ----------\n"


def generate_notebooks(workflow: AlteryxWorkflow, output_dir: Path):
    output_dir = Path(output_dir)
    wf_dir = output_dir / workflow.name
    wf_dir.mkdir(parents=True, exist_ok=True)
    (wf_dir / "alteryx_output").mkdir(exist_ok=True)

    # Resolve execution order
    order = resolve_dag(workflow)

    # Build connection map: tool_id → list of input df names
    input_map = _build_input_map(workflow, order)

    # Generate steps
    steps: dict[int, GeneratedStep] = {}
    for tool_id in order:
        tool = workflow.tools[tool_id]
        handler = get_handler(tool)
        input_dfs = input_map.get(tool_id, [])
        step = handler.convert(tool, input_df_names=input_dfs)
        steps[tool_id] = step

    # Classify into load / transform / output
    load_steps = []
    transform_steps = []
    output_steps = []

    for tool_id in order:
        tool = workflow.tools[tool_id]
        if tool.tool_type in _INPUT_TYPES:
            load_steps.append((tool_id, steps[tool_id]))
        elif tool.tool_type in _OUTPUT_TYPES:
            output_steps.append((tool_id, steps[tool_id]))
        else:
            transform_steps.append((tool_id, steps[tool_id]))

    # Collect all imports
    all_imports = set()
    for step in steps.values():
        all_imports.update(step.imports)

    imports_block = "\n".join(sorted(all_imports)) if all_imports else ""

    # Write 01_load_sources.py
    _write_notebook(
        wf_dir / "01_load_sources.py",
        f"Step 1: Load Data Sources — {workflow.name}",
        imports_block,
        load_steps,
    )

    # Write 02_transformations.py
    _write_notebook(
        wf_dir / "02_transformations.py",
        f"Step 2: Transformations — {workflow.name}",
        imports_block,
        transform_steps,
    )

    # Write 03_orchestrate.py
    _write_orchestrator(wf_dir / "03_orchestrate.py", workflow.name, output_steps, imports_block)

    # Write 04_validate.py
    from .validator import write_validator
    write_validator(wf_dir / "04_validate.py", workflow, output_steps)

    # Write config.yml
    from .config import write_config
    write_config(wf_dir / "config.yml", workflow)

    # Write conversion_report.md
    from .report import write_report
    write_report(wf_dir / "conversion_report.md", workflow, steps)


def _build_input_map(
    workflow: AlteryxWorkflow, order: list[int]
) -> dict[int, list[str]]:
    input_map: dict[int, list[str]] = {tid: [] for tid in order}

    for conn in workflow.connections:
        src = conn.source_tool_id
        dst = conn.target_tool_id
        anchor = conn.source_anchor

        if anchor in ("True", "False"):
            df_name = f"df_{src}_{anchor.lower()}"
        else:
            df_name = f"df_{src}"

        if dst in input_map:
            input_map[dst].append(df_name)

    return input_map


def _write_notebook(
    path: Path,
    title: str,
    imports: str,
    steps: list[tuple[int, GeneratedStep]],
):
    parts = [NOTEBOOK_HEADER, f"# {title}"]

    if imports:
        parts.append(COMMAND_SEP)
        parts.append(imports)

    for _tool_id, step in steps:
        parts.append(COMMAND_SEP)
        parts.append(step.code)

    path.write_text("\n".join(parts), encoding="utf-8")


def _write_orchestrator(
    path: Path,
    name: str,
    output_steps: list[tuple[int, GeneratedStep]],
    imports: str,
):
    parts = [
        NOTEBOOK_HEADER,
        f"# Step 3: Run Pipeline & Write Output — {name}",
        COMMAND_SEP,
        '# Run previous steps',
        'dbutils.notebook.run("01_load_sources", timeout_seconds=600)',
        'dbutils.notebook.run("02_transformations", timeout_seconds=600)',
    ]

    if imports:
        parts.append(COMMAND_SEP)
        parts.append(imports)

    for _tool_id, step in output_steps:
        parts.append(COMMAND_SEP)
        parts.append(step.code)

    path.write_text("\n".join(parts), encoding="utf-8")
```

**Step 3: Write config.py, validator.py, report.py** (supporting generators)

```python
# src/alteryx2dbx/generator/config.py
from __future__ import annotations

from pathlib import Path
import yaml

from alteryx2dbx.parser.models import AlteryxWorkflow


def write_config(path: Path, workflow: AlteryxWorkflow):
    config = {
        "workflow": workflow.name,
        "version": workflow.version,
        "sources": [],
        "outputs": [],
    }

    for tool in workflow.tools.values():
        if "file_path" in tool.config:
            entry = {
                "tool_id": tool.tool_id,
                "type": tool.tool_type,
                "path": tool.config["file_path"],
                "annotation": tool.annotation,
            }
            if tool.tool_type in ("DbFileInput", "TextInput", "InputData"):
                config["sources"].append(entry)
            elif tool.tool_type in ("DbFileOutput", "OutputData"):
                config["outputs"].append(entry)

    path.write_text(yaml.dump(config, default_flow_style=False, sort_keys=False), encoding="utf-8")
```

```python
# src/alteryx2dbx/generator/validator.py
from __future__ import annotations

from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow, GeneratedStep


NOTEBOOK_HEADER = "# Databricks notebook source"
COMMAND_SEP = "\n# COMMAND ----------\n"


def write_validator(
    path: Path,
    workflow: AlteryxWorkflow,
    output_steps: list[tuple[int, GeneratedStep]],
):
    parts = [
        NOTEBOOK_HEADER,
        f"# Step 4: Validate Output vs Alteryx — {workflow.name}",
        COMMAND_SEP,
        "import datacompy",
        "import pandas as pd",
        COMMAND_SEP,
        "# Load Databricks output (update variable name if needed)",
    ]

    if output_steps:
        last_tool_id = output_steps[-1][0]
        parts.append(f"spark_output = df_{last_tool_id}.toPandas()")
    else:
        parts.append("# spark_output = df_LAST.toPandas()  # TODO: set correct df")

    parts.extend([
        COMMAND_SEP,
        '# Load original Alteryx output',
        '# Drop your Alteryx .xlsx output into the alteryx_output/ folder',
        'alteryx_output = pd.read_excel("alteryx_output/output.xlsx")  # TODO: update filename',
        COMMAND_SEP,
        "# Normalize columns",
        "for df in [spark_output, alteryx_output]:",
        '    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")',
        COMMAND_SEP,
        "# Compare",
        "compare = datacompy.Compare(",
        "    alteryx_output, spark_output,",
        '    # TODO: set join columns for your data',
        '    join_columns=["id"],',
        "    abs_tol=0.01,",
        "    rel_tol=0.0001,",
        '    df1_name="Alteryx",',
        '    df2_name="Databricks",',
        ")",
        COMMAND_SEP,
        '# Results',
        'print(f"Match: {compare.matches()}")',
        'print(f"Rows in Alteryx only: {len(compare.df1_unq_rows)}")',
        'print(f"Rows in Databricks only: {len(compare.df2_unq_rows)}")',
        'print(compare.report())',
    ])

    path.write_text("\n".join(parts), encoding="utf-8")
```

```python
# src/alteryx2dbx/generator/report.py
from __future__ import annotations

from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow, GeneratedStep


def write_report(
    path: Path,
    workflow: AlteryxWorkflow,
    steps: dict[int, GeneratedStep],
):
    total = len(steps)
    converted = sum(1 for s in steps.values() if s.confidence > 0.0)
    avg_conf = sum(s.confidence for s in steps.values()) / total if total else 0
    needs_review = [s for s in steps.values() if s.notes]

    lines = [
        f"# Conversion Report: {workflow.name}\n",
        "## Summary",
        f"- Tools converted: {converted}/{total} ({converted/total*100:.0f}%)" if total else "- No tools",
        f"- Average confidence: {avg_conf:.2f}",
        f"- Manual review needed: {len(needs_review)} tools\n",
        "## Tool Details\n",
        "| Tool ID | Type | Annotation | Confidence | Notes |",
        "|---------|------|------------|-----------|-------|",
    ]

    for tool_id, step in sorted(steps.items()):
        tool = workflow.tools[tool_id]
        status = "✓" if step.confidence >= 0.9 else "⚠️" if step.confidence > 0 else "❌"
        notes = "; ".join(step.notes) if step.notes else status
        lines.append(
            f"| {tool_id} | {tool.tool_type} | {tool.annotation} | {step.confidence:.1f} | {notes} |"
        )

    path.write_text("\n".join(lines), encoding="utf-8")
```

**Step 4: Create __init__.py**

```python
# src/alteryx2dbx/generator/__init__.py
```

**Step 5: Run tests**

```bash
pytest tests/test_generator.py -v
```

Expected: All PASS

**Step 6: Commit**

```bash
git add src/alteryx2dbx/generator/ tests/test_generator.py
git commit -m "feat: notebook generator — assembles 4-step output per workflow"
```

---

## Phase 7: CLI

### Task 14: Build the CLI with Click

**Files:**
- Create: `alteryx2dbx/src/alteryx2dbx/cli.py`
- Test: `alteryx2dbx/tests/test_cli.py`

**Step 1: Write failing tests**

```python
# tests/test_cli.py
from pathlib import Path
from click.testing import CliRunner

from alteryx2dbx.cli import main


FIXTURES = Path(__file__).parent / "fixtures"


def test_cli_convert_single_file(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, [
        "convert",
        str(FIXTURES / "simple_filter.yxmd"),
        "-o", str(tmp_path / "output"),
    ])
    assert result.exit_code == 0
    assert (tmp_path / "output" / "simple_filter" / "01_load_sources.py").exists()


def test_cli_convert_directory(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, [
        "convert",
        str(FIXTURES),
        "-o", str(tmp_path / "output"),
    ])
    assert result.exit_code == 0


def test_cli_analyze(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, [
        "analyze",
        str(FIXTURES / "simple_filter.yxmd"),
    ])
    assert result.exit_code == 0
    assert "simple_filter" in result.output


def test_cli_tools():
    runner = CliRunner()
    result = runner.invoke(main, ["tools"])
    assert result.exit_code == 0
    assert "Filter" in result.output
```

**Step 2: Write cli.py**

```python
# src/alteryx2dbx/cli.py
from __future__ import annotations

from pathlib import Path

import click

from .parser.xml_parser import parse_yxmd
from .generator.notebook import generate_notebooks
from .dag.resolver import resolve_dag
from .handlers.registry import get_handler
import alteryx2dbx.handlers  # noqa: F401


@click.group()
@click.version_option()
def main():
    """alteryx2dbx — Convert Alteryx workflows to PySpark Databricks notebooks."""
    pass


@main.command()
@click.argument("source", type=click.Path(exists=True))
@click.option("-o", "--output", default="./output", help="Output directory")
def convert(source: str, output: str):
    """Convert .yxmd file(s) to Databricks notebooks."""
    source_path = Path(source)
    output_path = Path(output)

    if source_path.is_file():
        files = [source_path]
    else:
        files = list(source_path.glob("**/*.yxmd"))

    if not files:
        click.echo("No .yxmd files found.")
        return

    for f in files:
        click.echo(f"Converting: {f.name}")
        try:
            wf = parse_yxmd(f)
            generate_notebooks(wf, output_path)
            click.echo(f"  ✓ Output: {output_path / wf.name}/")
        except Exception as e:
            click.echo(f"  ✗ Error: {e}", err=True)

    click.echo(f"\nDone. Converted {len(files)} workflow(s).")


@main.command()
@click.argument("source", type=click.Path(exists=True))
def analyze(source: str):
    """Analyze workflow without generating code."""
    source_path = Path(source)
    files = [source_path] if source_path.is_file() else list(source_path.glob("**/*.yxmd"))

    for f in files:
        wf = parse_yxmd(f)
        order = resolve_dag(wf)
        click.echo(f"\n{'='*50}")
        click.echo(f"Workflow: {wf.name}")
        click.echo(f"Version: {wf.version}")
        click.echo(f"Tools: {len(wf.tools)}")
        click.echo(f"Connections: {len(wf.connections)}")
        click.echo(f"\nTool breakdown:")

        supported = 0
        for tool_id in order:
            tool = wf.tools[tool_id]
            handler = get_handler(tool)
            is_supported = type(handler).__name__ != "UnsupportedHandler"
            status = "✓" if is_supported else "✗"
            if is_supported:
                supported += 1
            click.echo(f"  {status} [{tool_id}] {tool.tool_type}: {tool.annotation}")

        pct = supported / len(wf.tools) * 100 if wf.tools else 0
        click.echo(f"\nCoverage: {supported}/{len(wf.tools)} ({pct:.0f}%)")


@main.command()
def tools():
    """List supported Alteryx tools."""
    from .handlers.registry import _registry

    click.echo("Supported tool types:")
    for tool_type in sorted(_registry._type_handlers.keys()):
        handler = _registry._type_handlers[tool_type]
        click.echo(f"  ✓ {tool_type} ({handler.__name__})")

    click.echo(f"\nTotal: {len(_registry._type_handlers)} tool types")
```

**Step 3: Run tests**

```bash
pytest tests/test_cli.py -v
```

**Step 4: Commit**

```bash
git add src/alteryx2dbx/cli.py tests/test_cli.py
git commit -m "feat: CLI with convert, analyze, and tools commands"
```

---

## Phase 8: Integration Test

### Task 15: End-to-end test with simple_filter fixture

**Files:**
- Test: `alteryx2dbx/tests/test_e2e.py`

**Step 1: Write integration test**

```python
# tests/test_e2e.py
from pathlib import Path

from click.testing import CliRunner

from alteryx2dbx.cli import main


FIXTURES = Path(__file__).parent / "fixtures"


def test_e2e_simple_filter(tmp_path):
    """Full pipeline: parse → DAG → handlers → generate → validate files exist."""
    runner = CliRunner()
    result = runner.invoke(main, [
        "convert",
        str(FIXTURES / "simple_filter.yxmd"),
        "-o", str(tmp_path),
    ])
    assert result.exit_code == 0

    wf_dir = tmp_path / "simple_filter"

    # All output files exist
    assert (wf_dir / "config.yml").exists()
    assert (wf_dir / "01_load_sources.py").exists()
    assert (wf_dir / "02_transformations.py").exists()
    assert (wf_dir / "03_orchestrate.py").exists()
    assert (wf_dir / "04_validate.py").exists()
    assert (wf_dir / "conversion_report.md").exists()

    # Load sources has the input tool
    load = (wf_dir / "01_load_sources.py").read_text()
    assert "Databricks notebook source" in load
    assert "spark.read" in load
    assert "customers.xlsx" in load

    # Transformations has the filter
    transform = (wf_dir / "02_transformations.py").read_text()
    assert "filter" in transform.lower()
    assert "active" in transform

    # Orchestrator chains steps
    orch = (wf_dir / "03_orchestrate.py").read_text()
    assert "01_load_sources" in orch
    assert "02_transformations" in orch

    # Validator has datacompy
    validate = (wf_dir / "04_validate.py").read_text()
    assert "datacompy" in validate
    assert "compare" in validate.lower()

    # Report has all 3 tools
    report = (wf_dir / "conversion_report.md").read_text()
    assert "3" in report or "100%" in report


def test_e2e_analyze_simple_filter():
    runner = CliRunner()
    result = runner.invoke(main, [
        "analyze",
        str(FIXTURES / "simple_filter.yxmd"),
    ])
    assert result.exit_code == 0
    assert "simple_filter" in result.output
    assert "Filter" in result.output
```

**Step 2: Run all tests**

```bash
pytest tests/ -v
```

Expected: All PASS

**Step 3: Commit**

```bash
git add tests/test_e2e.py
git commit -m "test: end-to-end integration test"
```

---

## Phase 9: Additional Fixtures + More Handlers

### Task 16: Add more complex .yxmd fixtures

Create fixtures for:
- `join_workflow.yxmd` — Input → Input → Join → Output
- `formula_workflow.yxmd` — Input → Formula (multiple fields) → Summarize → Output
- `complex_workflow.yxmd` — Input → Select → Filter → Formula → Join → Summarize → Sort → Output

Use these to write integration tests that validate the full pipeline handles multi-tool, multi-branch workflows.

### Task 17: Phase 2 handlers

Implement handlers for: Unique, Sample, RecordID, CrossTab, Transpose, DataCleansing, MultiFieldFormula. Follow the same TDD pattern as Phase 1 handlers.

### Task 18: Create GitHub repo + README

```bash
cd "/Users/kartikaggarwal/Projects/alteryx2dbx"
gh repo create alteryx2dbx --public --source=. --push
```

Write a README with:
- What it does (1 paragraph)
- Quick start (`pip install alteryx2dbx` → `alteryx2dbx convert workflow.yxmd`)
- Supported tools table
- Output structure example
- Contributing guide

---

## Execution Order Summary

| Phase | Tasks | What it builds |
|-------|-------|---------------|
| 1 | 1-3 | Scaffolding, IR models, test fixture |
| 2 | 4 | XML parser |
| 3 | 5 | DAG resolver |
| 4 | 6-7 | Expression transpiler (grammar + emitter) |
| 5 | 8-12 | All Phase 1 tool handlers |
| 6 | 13 | Code generator (4 notebooks + config + report) |
| 7 | 14 | CLI |
| 8 | 15 | End-to-end integration test |
| 9 | 16-18 | More fixtures, Phase 2 handlers, GitHub repo |

Each phase is independently testable. After Phase 8 you have a working tool that handles the 9 most common Alteryx tools.
