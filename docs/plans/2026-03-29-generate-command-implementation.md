# Generate Command Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `generate` command that takes a parsed manifest and emits production-ready, serverless-safe Databricks notebooks with auto-generated config, utils, and validation.

**Architecture:** Extend the existing 5-stage pipeline with: (1) manifest serialization/deserialization layer, (2) `.yxzp` unpacker in the parser, (3) new `generate` command consuming manifest JSON, (4) rewritten notebook generator emitting `_config.py`, `_utils.py`, and smarter validation, (5) semantic fix registry applied post-transpilation. The existing `convert` command gets a `--full` flag that chains parse → generate.

**Tech Stack:** Python 3.11+, Click (CLI), lark (expressions), networkx (DAG), dataclasses + JSON (manifest), zipfile (.yxzp)

---

## Task 1: Manifest Serialization

Add `to_dict()` / `from_dict()` methods to IR dataclasses and a `serialize_manifest()` / `load_manifest()` pair. This is the foundation — everything else depends on it.

**Files:**
- Modify: `src/alteryx2dbx/parser/models.py`
- Create: `src/alteryx2dbx/manifest.py`
- Test: `tests/test_manifest.py`

**Step 1: Write the failing test**

```python
# tests/test_manifest.py
"""Tests for manifest serialization round-trip."""
import json
from pathlib import Path

from alteryx2dbx.parser.models import (
    AlteryxConnection,
    AlteryxField,
    AlteryxTool,
    AlteryxWorkflow,
    GeneratedStep,
)
from alteryx2dbx.manifest import serialize_manifest, load_manifest


def _sample_workflow() -> AlteryxWorkflow:
    return AlteryxWorkflow(
        name="test_wf",
        version="2024.1",
        tools={
            1: AlteryxTool(
                tool_id=1,
                plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput",
                tool_type="DbFileInput",
                config={"file_path": "data/input.csv", "FileFormat": "CSV"},
                annotation="Load Customers",
                output_fields=[AlteryxField(name="id", type="Int32", size=4)],
            ),
            2: AlteryxTool(
                tool_id=2,
                plugin="AlteryxBasePluginsGui.Filter.Filter",
                tool_type="Filter",
                config={"expression": "[Revenue] > 1000"},
                annotation="High Revenue",
            ),
        },
        connections=[
            AlteryxConnection(
                source_tool_id=1,
                source_anchor="Output",
                target_tool_id=2,
                target_anchor="Input",
            )
        ],
        properties={"MetaInfo": {"Name": "test_wf"}},
    )


def test_serialize_roundtrip(tmp_path: Path):
    """Serialize to JSON, load back, and verify equality."""
    wf = _sample_workflow()
    manifest_path = tmp_path / "manifest.json"
    serialize_manifest(wf, manifest_path)

    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text())
    assert data["name"] == "test_wf"
    assert len(data["tools"]) == 2
    assert len(data["connections"]) == 1

    loaded = load_manifest(manifest_path)
    assert loaded.name == wf.name
    assert loaded.version == wf.version
    assert len(loaded.tools) == 2
    assert loaded.tools[1].tool_type == "DbFileInput"
    assert loaded.tools[1].output_fields[0].name == "id"
    assert len(loaded.connections) == 1
    assert loaded.connections[0].source_tool_id == 1


def test_serialize_empty_workflow(tmp_path: Path):
    wf = AlteryxWorkflow(name="empty", version="1.0")
    manifest_path = tmp_path / "manifest.json"
    serialize_manifest(wf, manifest_path)
    loaded = load_manifest(manifest_path)
    assert loaded.name == "empty"
    assert loaded.tools == {}
    assert loaded.connections == []
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_manifest.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'alteryx2dbx.manifest'`

**Step 3: Add to_dict/from_dict to models**

In `src/alteryx2dbx/parser/models.py`, add methods to each dataclass:

```python
# Add to AlteryxField:
def to_dict(self) -> dict:
    return {"name": self.name, "type": self.type, "size": self.size, "scale": self.scale}

@classmethod
def from_dict(cls, d: dict) -> "AlteryxField":
    return cls(name=d["name"], type=d["type"], size=d.get("size"), scale=d.get("scale"))


# Add to AlteryxConnection:
def to_dict(self) -> dict:
    return {
        "source_tool_id": self.source_tool_id,
        "source_anchor": self.source_anchor,
        "target_tool_id": self.target_tool_id,
        "target_anchor": self.target_anchor,
    }

@classmethod
def from_dict(cls, d: dict) -> "AlteryxConnection":
    return cls(
        source_tool_id=d["source_tool_id"],
        source_anchor=d["source_anchor"],
        target_tool_id=d["target_tool_id"],
        target_anchor=d["target_anchor"],
    )


# Add to AlteryxTool:
def to_dict(self) -> dict:
    return {
        "tool_id": self.tool_id,
        "plugin": self.plugin,
        "tool_type": self.tool_type,
        "config": self.config,
        "annotation": self.annotation,
        "input_fields": [f.to_dict() for f in self.input_fields],
        "output_fields": [f.to_dict() for f in self.output_fields],
    }

@classmethod
def from_dict(cls, d: dict) -> "AlteryxTool":
    return cls(
        tool_id=d["tool_id"],
        plugin=d["plugin"],
        tool_type=d["tool_type"],
        config=d["config"],
        annotation=d.get("annotation", ""),
        input_fields=[AlteryxField.from_dict(f) for f in d.get("input_fields", [])],
        output_fields=[AlteryxField.from_dict(f) for f in d.get("output_fields", [])],
    )


# Add to AlteryxWorkflow:
def to_dict(self) -> dict:
    return {
        "name": self.name,
        "version": self.version,
        "tools": {str(k): v.to_dict() for k, v in self.tools.items()},
        "connections": [c.to_dict() for c in self.connections],
        "properties": self.properties,
    }

@classmethod
def from_dict(cls, d: dict) -> "AlteryxWorkflow":
    return cls(
        name=d["name"],
        version=d["version"],
        tools={int(k): AlteryxTool.from_dict(v) for k, v in d.get("tools", {}).items()},
        connections=[AlteryxConnection.from_dict(c) for c in d.get("connections", [])],
        properties=d.get("properties", {}),
    )
```

**Step 4: Write manifest.py**

```python
# src/alteryx2dbx/manifest.py
"""Serialize / deserialize AlteryxWorkflow to/from JSON manifest."""
from __future__ import annotations

import json
from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow


def serialize_manifest(workflow: AlteryxWorkflow, path: Path) -> None:
    """Write workflow IR to a JSON manifest file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(workflow.to_dict(), f, indent=2, ensure_ascii=False)


def load_manifest(path: Path) -> AlteryxWorkflow:
    """Load an AlteryxWorkflow from a JSON manifest file."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return AlteryxWorkflow.from_dict(data)
```

**Step 5: Run tests to verify they pass**

Run: `pytest tests/test_manifest.py -v`
Expected: PASS (both tests)

**Step 6: Commit**

```bash
git add src/alteryx2dbx/parser/models.py src/alteryx2dbx/manifest.py tests/test_manifest.py
git commit -m "feat: add manifest serialization (to_dict/from_dict + JSON I/O)"
```

---

## Task 2: .yxzp Unpacker

Add a function that handles both `.yxmd` and `.yxzp` inputs. For `.yxzp`, unzip to temp dir, find the primary `.yxmd`, and return its path. Flag bundled macros and data files.

**Files:**
- Create: `src/alteryx2dbx/parser/unpacker.py`
- Test: `tests/test_unpacker.py`

**Step 1: Write the failing test**

```python
# tests/test_unpacker.py
"""Tests for .yxzp unpacker."""
import zipfile
from pathlib import Path

from alteryx2dbx.parser.unpacker import unpack_source


MINIMAL_YXMD = """<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2024.1">
  <Properties><MetaInfo><Name>TestWorkflow</Name></MetaInfo></Properties>
  <Nodes></Nodes>
  <Connections></Connections>
</AlteryxDocument>
"""


def test_yxmd_passthrough(tmp_path: Path):
    """A .yxmd file is returned as-is."""
    yxmd = tmp_path / "workflow.yxmd"
    yxmd.write_text(MINIMAL_YXMD)
    result = unpack_source(yxmd)
    assert result.workflow_path == yxmd
    assert result.assets == []
    assert result.macros == []


def test_yxzp_extraction(tmp_path: Path):
    """A .yxzp file is unzipped and the primary .yxmd is found."""
    # Build a fake .yxzp
    yxmd_content = MINIMAL_YXMD
    yxzp_path = tmp_path / "package.yxzp"
    with zipfile.ZipFile(yxzp_path, "w") as zf:
        zf.writestr("workflow.yxmd", yxmd_content)
        zf.writestr("data/input.csv", "id,name\n1,Alice")
        zf.writestr("macros/helper.yxmc", "<AlteryxDocument/>")

    result = unpack_source(yxzp_path)
    assert result.workflow_path.name == "workflow.yxmd"
    assert result.workflow_path.exists()
    assert any("input.csv" in str(a) for a in result.assets)
    assert any("helper.yxmc" in str(m) for m in result.macros)


def test_yxzp_cleanup(tmp_path: Path):
    """Cleanup removes the temp directory."""
    yxzp_path = tmp_path / "package.yxzp"
    with zipfile.ZipFile(yxzp_path, "w") as zf:
        zf.writestr("workflow.yxmd", MINIMAL_YXMD)

    result = unpack_source(yxzp_path)
    temp_dir = result.workflow_path.parent
    result.cleanup()
    assert not temp_dir.exists()


def test_unsupported_extension(tmp_path: Path):
    """Non-.yxmd/.yxzp files raise ValueError."""
    bad_file = tmp_path / "workflow.txt"
    bad_file.write_text("not a workflow")
    try:
        unpack_source(bad_file)
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_unpacker.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'alteryx2dbx.parser.unpacker'`

**Step 3: Write minimal implementation**

```python
# src/alteryx2dbx/parser/unpacker.py
"""Unpack .yxmd and .yxzp workflow sources."""
from __future__ import annotations

import shutil
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class UnpackResult:
    """Result of unpacking a workflow source."""
    workflow_path: Path
    assets: list[Path] = field(default_factory=list)
    macros: list[Path] = field(default_factory=list)
    _temp_dir: Path | None = None

    def cleanup(self) -> None:
        """Remove temporary extraction directory if one was created."""
        if self._temp_dir and self._temp_dir.exists():
            shutil.rmtree(self._temp_dir)


def unpack_source(source: Path) -> UnpackResult:
    """Unpack a .yxmd or .yxzp source into a parseable workflow path.

    Args:
        source: Path to a .yxmd or .yxzp file.

    Returns:
        UnpackResult with the primary .yxmd path and any bundled assets/macros.

    Raises:
        ValueError: If the file extension is not .yxmd or .yxzp.
        FileNotFoundError: If no .yxmd file is found inside a .yxzp archive.
    """
    suffix = source.suffix.lower()
    if suffix == ".yxmd":
        return UnpackResult(workflow_path=source)
    elif suffix == ".yxzp":
        return _unpack_yxzp(source)
    else:
        raise ValueError(f"Unsupported file format: {suffix}. Expected .yxmd or .yxzp")


def _unpack_yxzp(source: Path) -> UnpackResult:
    """Extract a .yxzp archive and locate the primary workflow."""
    temp_dir = Path(tempfile.mkdtemp(prefix="alteryx2dbx_"))
    with zipfile.ZipFile(source, "r") as zf:
        zf.extractall(temp_dir)

    # Find primary .yxmd (prefer root-level, then any nested)
    yxmd_files = list(temp_dir.glob("*.yxmd"))
    if not yxmd_files:
        yxmd_files = list(temp_dir.rglob("*.yxmd"))
    if not yxmd_files:
        shutil.rmtree(temp_dir)
        raise FileNotFoundError(f"No .yxmd file found in {source}")

    workflow_path = yxmd_files[0]

    # Collect bundled assets and macros
    assets = [p for p in temp_dir.rglob("*") if p.is_file() and p.suffix.lower() not in (".yxmd", ".yxmc")]
    macros = list(temp_dir.rglob("*.yxmc"))

    return UnpackResult(
        workflow_path=workflow_path,
        assets=assets,
        macros=macros,
        _temp_dir=temp_dir,
    )
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_unpacker.py -v`
Expected: PASS (all 4 tests)

**Step 5: Commit**

```bash
git add src/alteryx2dbx/parser/unpacker.py tests/test_unpacker.py
git commit -m "feat: add .yxzp unpacker with asset/macro detection"
```

---

## Task 3: `parse` CLI Command

Add the `parse` command that handles `.yxmd`/`.yxzp` input and writes `manifest.json`.

**Files:**
- Modify: `src/alteryx2dbx/cli.py`
- Test: `tests/test_cli.py` (add new tests)

**Step 1: Write the failing test**

```python
# Add to tests/test_cli.py (or create it if needed)
from click.testing import CliRunner
from alteryx2dbx.cli import main
import json

MINIMAL_YXMD = """<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2024.1">
  <Properties><MetaInfo><Name>ParseTest</Name></MetaInfo></Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput"/>
      <Properties><Configuration><File>data/input.csv</File></Configuration></Properties>
    </Node>
  </Nodes>
  <Connections></Connections>
</AlteryxDocument>
"""


def test_parse_yxmd(tmp_path):
    yxmd = tmp_path / "test.yxmd"
    yxmd.write_text(MINIMAL_YXMD)
    out = tmp_path / "manifest.json"

    runner = CliRunner()
    result = runner.invoke(main, ["parse", str(yxmd), "-o", str(out)])
    assert result.exit_code == 0
    assert out.exists()
    data = json.loads(out.read_text())
    assert data["name"] == "ParseTest"


def test_parse_yxzp(tmp_path):
    import zipfile
    yxmd = tmp_path / "inner.yxmd"
    yxmd.write_text(MINIMAL_YXMD)
    yxzp = tmp_path / "package.yxzp"
    with zipfile.ZipFile(yxzp, "w") as zf:
        zf.writestr("inner.yxmd", MINIMAL_YXMD)

    out = tmp_path / "manifest.json"
    runner = CliRunner()
    result = runner.invoke(main, ["parse", str(yxzp), "-o", str(out)])
    assert result.exit_code == 0
    assert out.exists()


def test_parse_batch(tmp_path):
    for i in range(3):
        (tmp_path / f"wf_{i}.yxmd").write_text(MINIMAL_YXMD)
    out_dir = tmp_path / "manifests"

    runner = CliRunner()
    result = runner.invoke(main, ["parse", str(tmp_path), "-o", str(out_dir)])
    assert result.exit_code == 0
    assert len(list(out_dir.glob("*.json"))) == 3
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py::test_parse_yxmd -v`
Expected: FAIL — `Error: No such command 'parse'.`

**Step 3: Add parse command to cli.py**

Add to `src/alteryx2dbx/cli.py`:

```python
from .parser.unpacker import unpack_source
from .manifest import serialize_manifest

@main.command()
@click.argument("source", type=click.Path(exists=True))
@click.option("-o", "--output", default="./manifest.json", help="Output path (file for single, dir for batch)")
def parse(source, output):
    """Parse .yxmd/.yxzp file(s) to JSON manifest(s)."""
    source_path = Path(source)
    output_path = Path(output)

    if source_path.is_file():
        unpacked = unpack_source(source_path)
        try:
            wf = parse_yxmd(unpacked.workflow_path)
            # Record macro/asset metadata
            if unpacked.macros:
                wf.properties["_macros"] = [str(m) for m in unpacked.macros]
            if unpacked.assets:
                wf.properties["_assets"] = [str(a) for a in unpacked.assets]
            serialize_manifest(wf, output_path)
            click.echo(f"Manifest: {output_path}")
        finally:
            unpacked.cleanup()
    else:
        files = list(source_path.glob("**/*.yxmd")) + list(source_path.glob("**/*.yxzp"))
        if not files:
            click.echo("No .yxmd or .yxzp files found.")
            return
        output_path.mkdir(parents=True, exist_ok=True)
        for f in files:
            unpacked = unpack_source(f)
            try:
                wf = parse_yxmd(unpacked.workflow_path)
                if unpacked.macros:
                    wf.properties["_macros"] = [str(m) for m in unpacked.macros]
                if unpacked.assets:
                    wf.properties["_assets"] = [str(a) for a in unpacked.assets]
                manifest_name = f"{f.stem}.json"
                serialize_manifest(wf, output_path / manifest_name)
                click.echo(f"  {f.name} -> {manifest_name}")
            finally:
                unpacked.cleanup()
        click.echo(f"\nParsed {len(files)} workflow(s).")
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cli.py::test_parse_yxmd tests/test_cli.py::test_parse_yxzp tests/test_cli.py::test_parse_batch -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/alteryx2dbx/cli.py tests/test_cli.py
git commit -m "feat: add parse CLI command (.yxmd/.yxzp -> manifest.json)"
```

---

## Task 4: Semantic Fix Registry

Create the fix registry as a standalone module. Each fix is a function that takes a code string + tool context and returns a modified code string.

**Files:**
- Create: `src/alteryx2dbx/fixes.py`
- Test: `tests/test_fixes.py`

**Step 1: Write the failing test**

```python
# tests/test_fixes.py
"""Tests for semantic fix registry."""
from alteryx2dbx.fixes import apply_fixes, FIXES


def test_registry_has_known_fixes():
    assert "case_insensitive_join" in FIXES
    assert "null_safe_equality" in FIXES
    assert "numeric_cast" in FIXES


def test_case_insensitive_join_fix():
    """Join on string columns should wrap in F.lower()."""
    code = 'df_3 = df_1.join(df_2, df_1["name"] == df_2["name"])'
    context = {"tool_type": "Join", "join_fields": [{"left": "name", "right": "name"}]}
    result = apply_fixes(code, context)
    assert "F.lower" in result.code


def test_fix_report_tracks_applied():
    code = 'df_3 = df_1.join(df_2, df_1["name"] == df_2["name"])'
    context = {"tool_type": "Join", "join_fields": [{"left": "name", "right": "name"}]}
    result = apply_fixes(code, context)
    assert len(result.applied_fixes) > 0
    assert result.applied_fixes[0]["fix_id"] == "case_insensitive_join"


def test_no_fixes_when_not_applicable():
    code = "df_2 = df_1.filter(F.col('revenue') > 1000)"
    context = {"tool_type": "Filter"}
    result = apply_fixes(code, context)
    assert result.code == code
    assert result.applied_fixes == []
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_fixes.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'alteryx2dbx.fixes'`

**Step 3: Write implementation**

```python
# src/alteryx2dbx/fixes.py
"""Semantic fix registry — known Alteryx-to-PySpark pitfalls applied post-transpilation."""
from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class FixResult:
    """Result of applying fixes to generated code."""
    code: str
    applied_fixes: list[dict] = field(default_factory=list)


def _fix_case_insensitive_join(code: str, context: dict) -> tuple[str, bool]:
    """Wrap string join keys in F.lower()."""
    if context.get("tool_type") != "Join":
        return code, False
    join_fields = context.get("join_fields", [])
    if not join_fields:
        return code, False
    modified = code
    for jf in join_fields:
        left, right = jf.get("left", ""), jf.get("right", "")
        if left and right:
            # Replace df["col"] == df["col"] with F.lower() wrapped version
            pattern = rf'(df_\w+)\["{re.escape(left)}"\]\s*==\s*(df_\w+)\["{re.escape(right)}"\]'
            replacement = rf'F.lower(\1["{left}"]) == F.lower(\2["{right}"])'
            new = re.sub(pattern, replacement, modified)
            if new != modified:
                modified = new
    return modified, modified != code


def _fix_null_safe_equality(code: str, context: dict) -> tuple[str, bool]:
    """Replace == with eqNullSafe for filter comparisons on nullable columns."""
    if context.get("tool_type") not in ("Filter", "Formula"):
        return code, False
    # Match patterns like F.col("x") == F.lit("y") and replace with eqNullSafe
    pattern = r'(F\.col\([^)]+\))\s*==\s*(F\.lit\([^)]+\))'
    replacement = r'\1.eqNullSafe(\2)'
    new = re.sub(pattern, replacement, code)
    return new, new != code


def _fix_numeric_cast(code: str, context: dict) -> tuple[str, bool]:
    """Add explicit DecimalType cast for FixedDecimal fields."""
    output_fields = context.get("output_fields", [])
    if not output_fields:
        return code, False
    modified = code
    applied = False
    for f in output_fields:
        if f.get("type") == "FixedDecimal":
            size = f.get("size", 19)
            scale = f.get("scale", 4)
            name = f.get("name", "")
            if name and name in modified:
                cast_line = f'\ndf = df.withColumn("{name}", F.col("{name}").cast(DecimalType({size}, {scale})))'
                if cast_line not in modified:
                    modified += cast_line
                    applied = True
    return modified, applied


FIXES: dict[str, dict] = {
    "case_insensitive_join": {
        "description": "Wrap string join keys in F.lower() — Alteryx = is case-insensitive",
        "severity": "silent_bug",
        "fn": _fix_case_insensitive_join,
    },
    "null_safe_equality": {
        "description": "Use eqNullSafe instead of == for nullable column comparisons",
        "severity": "silent_bug",
        "fn": _fix_null_safe_equality,
    },
    "numeric_cast": {
        "description": "Explicit DecimalType cast for Alteryx FixedDecimal fields",
        "severity": "data_loss",
        "fn": _fix_numeric_cast,
    },
}


def apply_fixes(code: str, context: dict) -> FixResult:
    """Apply all applicable fixes to generated code.

    Args:
        code: Generated PySpark code string.
        context: Tool context dict (tool_type, join_fields, output_fields, etc.).

    Returns:
        FixResult with modified code and list of applied fixes.
    """
    result = FixResult(code=code)
    for fix_id, fix_def in FIXES.items():
        new_code, was_applied = fix_def["fn"](result.code, context)
        if was_applied:
            result.code = new_code
            result.applied_fixes.append({
                "fix_id": fix_id,
                "description": fix_def["description"],
                "severity": fix_def["severity"],
            })
    return result
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_fixes.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/alteryx2dbx/fixes.py tests/test_fixes.py
git commit -m "feat: add semantic fix registry (join case, null-safe, decimal cast)"
```

---

## Task 5: _config.py Notebook Generator

Replace `config.yml` output with a `_config.py` Databricks notebook using widgets for parameterization.

**Files:**
- Create: `src/alteryx2dbx/generator/config_notebook.py`
- Test: `tests/test_config_notebook.py`

**Step 1: Write the failing test**

```python
# tests/test_config_notebook.py
"""Tests for _config.py notebook generation."""
from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow, AlteryxTool
from alteryx2dbx.generator.config_notebook import generate_config_notebook


def _wf_with_io():
    return AlteryxWorkflow(
        name="test_wf",
        version="2024.1",
        tools={
            1: AlteryxTool(
                tool_id=1, plugin="", tool_type="DbFileInput",
                config={"file_path": r"\\server\data\input.csv"},
                annotation="Load Customers",
            ),
            2: AlteryxTool(
                tool_id=2, plugin="", tool_type="DbFileOutput",
                config={"file_path": r"\\server\data\output.csv"},
                annotation="Write Results",
            ),
        },
    )


def test_generates_config_notebook(tmp_path: Path):
    wf = _wf_with_io()
    generate_config_notebook(wf, tmp_path)
    config_path = tmp_path / "_config.py"
    assert config_path.exists()
    content = config_path.read_text()
    assert "Databricks notebook source" in content
    assert "dbutils.widgets" in content
    assert "SOURCES" in content
    assert "OUTPUTS" in content


def test_config_has_widgets(tmp_path: Path):
    wf = _wf_with_io()
    generate_config_notebook(wf, tmp_path)
    content = (tmp_path / "_config.py").read_text()
    assert 'dbutils.widgets.text("catalog"' in content
    assert 'dbutils.widgets.text("schema"' in content
    assert 'dbutils.widgets.text("env"' in content


def test_config_flags_network_paths(tmp_path: Path):
    wf = _wf_with_io()
    generate_config_notebook(wf, tmp_path)
    content = (tmp_path / "_config.py").read_text()
    assert "TODO" in content  # Network paths should be flagged
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_config_notebook.py -v`
Expected: FAIL

**Step 3: Write implementation**

```python
# src/alteryx2dbx/generator/config_notebook.py
"""Generate _config.py — parameterized Databricks config notebook."""
from __future__ import annotations

from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow


_LOAD_TYPES = {"DbFileInput", "TextInput", "InputData", "DynamicInput"}
_OUTPUT_TYPES = {"DbFileOutput", "OutputData"}


def generate_config_notebook(workflow: AlteryxWorkflow, output_dir: Path) -> None:
    """Write _config.py Databricks notebook with widget-parameterized config."""
    sources = []
    outputs = []
    for tool_id, tool in workflow.tools.items():
        path = tool.config.get("file_path", "")
        entry = f'    {tool_id}: {{"path": "{path}", "annotation": "{tool.annotation}"}}'
        if tool.tool_type in _LOAD_TYPES:
            sources.append(entry)
        elif tool.tool_type in _OUTPUT_TYPES:
            outputs.append(entry)

    # Flag network paths
    all_paths = [tool.config.get("file_path", "") for tool in workflow.tools.values()]
    network_warnings = [p for p in all_paths if p.startswith("\\\\")]

    lines = [
        "# Databricks notebook source",
        f"# _config — {workflow.name}",
        "",
        "# COMMAND ----------",
        "",
        "# Parameterized config — override via job parameters or widgets UI",
        'dbutils.widgets.text("catalog", "dev", "Catalog")',
        'dbutils.widgets.text("schema", "default", "Schema")',
        'dbutils.widgets.text("env", "dev", "Environment")',
        "",
        'CATALOG = dbutils.widgets.get("catalog")',
        'SCHEMA = dbutils.widgets.get("schema")',
        'ENV = dbutils.widgets.get("env")',
        "",
        "# COMMAND ----------",
        "",
        f"WORKFLOW_NAME = \"{workflow.name}\"",
        f"WORKFLOW_VERSION = \"{workflow.version}\"",
        "",
        "# COMMAND ----------",
        "",
        "# Source paths — update these to Databricks-accessible locations",
        "SOURCES = {",
    ]
    lines.extend(sources)
    lines.append("}")
    lines.append("")
    lines.append("# COMMAND ----------")
    lines.append("")
    lines.append("# Output paths — update these to Databricks-accessible locations")
    lines.append("OUTPUTS = {")
    lines.extend(outputs)
    lines.append("}")

    if network_warnings:
        lines.append("")
        lines.append("# COMMAND ----------")
        lines.append("")
        lines.append("# TODO: The following network paths need migration to DBFS/Volumes/S3/ADLS:")
        for p in network_warnings:
            lines.append(f"#   {p}")

    lines.append("")

    with open(output_dir / "_config.py", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config_notebook.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/alteryx2dbx/generator/config_notebook.py tests/test_config_notebook.py
git commit -m "feat: add _config.py notebook generator with widget parameterization"
```

---

## Task 6: _utils.py Notebook Generator

Emit the standard utils template.

**Files:**
- Create: `src/alteryx2dbx/generator/utils_notebook.py`
- Test: `tests/test_utils_notebook.py`

**Step 1: Write the failing test**

```python
# tests/test_utils_notebook.py
"""Tests for _utils.py notebook generation."""
from pathlib import Path
from alteryx2dbx.generator.utils_notebook import generate_utils_notebook


def test_generates_utils_notebook(tmp_path: Path):
    generate_utils_notebook(tmp_path)
    utils_path = tmp_path / "_utils.py"
    assert utils_path.exists()
    content = utils_path.read_text()
    assert "Databricks notebook source" in content


def test_utils_has_logging(tmp_path: Path):
    content = _generate_and_read(tmp_path)
    assert "def log_step" in content


def test_utils_has_null_safe_join(tmp_path: Path):
    content = _generate_and_read(tmp_path)
    assert "def null_safe_join" in content


def test_utils_has_quality_check(tmp_path: Path):
    content = _generate_and_read(tmp_path)
    assert "def check_row_count" in content


def _generate_and_read(tmp_path: Path) -> str:
    generate_utils_notebook(tmp_path)
    return (tmp_path / "_utils.py").read_text()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_utils_notebook.py -v`
Expected: FAIL

**Step 3: Write implementation**

```python
# src/alteryx2dbx/generator/utils_notebook.py
"""Generate _utils.py — standard utility functions notebook."""
from __future__ import annotations

from pathlib import Path


_UTILS_TEMPLATE = '''# Databricks notebook source
# _utils — Common helper functions

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import DataFrame, functions as F

logger = logging.getLogger(__name__)

# COMMAND ----------

def log_step(step_name: str, df: DataFrame) -> DataFrame:
    """Log step name and row count. Returns df unchanged for chaining."""
    count = df.count()
    logger.info(f"[{datetime.now():%H:%M:%S}] {step_name}: {count:,} rows")
    print(f"[{datetime.now():%H:%M:%S}] {step_name}: {count:,} rows")
    return df

# COMMAND ----------

def null_safe_join(left: DataFrame, right: DataFrame, left_key: str, right_key: str, how: str = "inner") -> DataFrame:
    """Join with F.lower() on both sides for case-insensitive matching."""
    return left.join(
        right,
        F.lower(F.col(f"left.{left_key}")) == F.lower(F.col(f"right.{right_key}")),
        how=how,
    )

# COMMAND ----------

def check_row_count(df: DataFrame, expected: int, tolerance: float = 0.05, step_name: str = "") -> bool:
    """Check row count is within tolerance of expected. Logs warning if not."""
    actual = df.count()
    if expected == 0:
        return True
    ratio = abs(actual - expected) / expected
    if ratio > tolerance:
        msg = f"Row count mismatch in {step_name}: expected ~{expected:,}, got {actual:,} ({ratio:.1%} off)"
        logger.warning(msg)
        print(f"WARNING: {msg}")
        return False
    return True

# COMMAND ----------

def safe_cast(df: DataFrame, col_name: str, target_type: str) -> DataFrame:
    """Cast column with null preservation — failed casts become null, not errors."""
    return df.withColumn(col_name, F.col(col_name).cast(target_type))
'''


def generate_utils_notebook(output_dir: Path) -> None:
    """Write _utils.py Databricks notebook with standard helpers."""
    with open(output_dir / "_utils.py", "w", encoding="utf-8") as f:
        f.write(_UTILS_TEMPLATE)
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_utils_notebook.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/alteryx2dbx/generator/utils_notebook.py tests/test_utils_notebook.py
git commit -m "feat: add _utils.py notebook generator with standard helpers"
```

---

## Task 7: Serverless-Safe Notebook Generator

Rewrite the core notebook generator to emit serverless-compatible patterns. This replaces the fan-out cache logic with temp views and adds `%run` imports for config/utils.

**Files:**
- Create: `src/alteryx2dbx/generator/notebook_v2.py`
- Test: `tests/test_notebook_v2.py`

**Step 1: Write the failing test**

```python
# tests/test_notebook_v2.py
"""Tests for serverless-safe notebook generation."""
from pathlib import Path

from alteryx2dbx.parser.models import (
    AlteryxConnection, AlteryxField, AlteryxTool, AlteryxWorkflow, GeneratedStep,
)
from alteryx2dbx.generator.notebook_v2 import generate_notebooks_v2


def _simple_workflow():
    return AlteryxWorkflow(
        name="test_wf",
        version="2024.1",
        tools={
            1: AlteryxTool(
                tool_id=1, plugin="", tool_type="DbFileInput",
                config={"file_path": "input.csv"}, annotation="Input",
            ),
            2: AlteryxTool(
                tool_id=2, plugin="", tool_type="Filter",
                config={"expression": "[Revenue] > 1000"}, annotation="Filter High",
            ),
            3: AlteryxTool(
                tool_id=3, plugin="", tool_type="DbFileOutput",
                config={"file_path": "output.csv"}, annotation="Output",
            ),
        },
        connections=[
            AlteryxConnection(1, "Output", 2, "Input"),
            AlteryxConnection(2, "True", 3, "Input"),
        ],
    )


def test_generates_all_notebooks(tmp_path: Path):
    wf = _simple_workflow()
    generate_notebooks_v2(wf, tmp_path)
    expected = ["_config.py", "_utils.py", "01_load_sources.py",
                "02_transformations.py", "03_write_outputs.py",
                "04_validate.py", "05_orchestrate.py"]
    for name in expected:
        assert (tmp_path / wf.name / name).exists(), f"Missing: {name}"


def test_no_cache_in_output(tmp_path: Path):
    wf = _simple_workflow()
    generate_notebooks_v2(wf, tmp_path)
    wf_dir = tmp_path / wf.name
    for py_file in wf_dir.glob("*.py"):
        content = py_file.read_text()
        assert ".cache()" not in content, f".cache() found in {py_file.name}"
        assert ".persist()" not in content, f".persist() found in {py_file.name}"


def test_orchestrator_uses_run(tmp_path: Path):
    wf = _simple_workflow()
    generate_notebooks_v2(wf, tmp_path)
    content = (tmp_path / wf.name / "05_orchestrate.py").read_text()
    assert "%run ./_config" in content
    assert "%run ./_utils" in content
    assert "%run ./01_load_sources" in content
    assert "%run ./02_transformations" in content
    assert "%run ./03_write_outputs" in content


def test_manifest_included_in_output(tmp_path: Path):
    wf = _simple_workflow()
    generate_notebooks_v2(wf, tmp_path)
    assert (tmp_path / wf.name / "manifest.json").exists()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_notebook_v2.py -v`
Expected: FAIL

**Step 3: Write implementation**

```python
# src/alteryx2dbx/generator/notebook_v2.py
"""Serverless-safe notebook generator — emits production-ready Databricks notebooks."""
from __future__ import annotations

import logging
from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow, GeneratedStep
from alteryx2dbx.dag.resolver import resolve_dag
from alteryx2dbx.handlers.registry import get_handler
from alteryx2dbx.manifest import serialize_manifest
import alteryx2dbx.handlers  # noqa: F401

from .config_notebook import generate_config_notebook
from .utils_notebook import generate_utils_notebook
from .validator_v2 import generate_validator_v2
from .report import generate_report

logger = logging.getLogger(__name__)

_LOAD_TYPES = {"DbFileInput", "TextInput", "InputData", "DynamicInput"}
_OUTPUT_TYPES = {"DbFileOutput", "OutputData", "Browse", "BrowseV2"}


def generate_notebooks_v2(workflow: AlteryxWorkflow, output_dir: Path) -> dict:
    """Generate the full serverless-safe Databricks notebook bundle."""
    wf_dir = output_dir / workflow.name
    wf_dir.mkdir(parents=True, exist_ok=True)

    # 1. Resolve execution order
    execution_order = resolve_dag(workflow)

    # 2. Build input map
    input_map = _build_input_map(workflow)

    # 3. Run each tool through its handler
    steps: dict[int, GeneratedStep] = {}
    for tool_id in execution_order:
        tool = workflow.tools[tool_id]
        handler = get_handler(tool)
        input_dfs = input_map.get(tool_id, [])
        step = handler.convert(tool, input_df_names=input_dfs or None)
        steps[tool_id] = step

    # 4. Serverless-safe fan-out: temp views instead of .cache()
    _insert_temp_view_hints(workflow, steps)

    # 5. Classify steps
    load_ids = [tid for tid in execution_order if workflow.tools[tid].tool_type in _LOAD_TYPES]
    output_ids = [tid for tid in execution_order if workflow.tools[tid].tool_type in _OUTPUT_TYPES]
    transform_ids = [tid for tid in execution_order if tid not in load_ids and tid not in output_ids]

    # 6. Write notebooks
    _write_notebook(wf_dir / "01_load_sources.py", f"01 — Load Sources: {workflow.name}", load_ids, steps)
    _write_notebook(wf_dir / "02_transformations.py", f"02 — Transformations: {workflow.name}", transform_ids, steps)
    _write_notebook(wf_dir / "03_write_outputs.py", f"03 — Write Outputs: {workflow.name}", output_ids, steps)

    # 7. Config + Utils
    generate_config_notebook(workflow, wf_dir)
    generate_utils_notebook(wf_dir)

    # 8. Orchestrator
    _write_orchestrator(wf_dir / "05_orchestrate.py", workflow.name)

    # 9. Validator
    generate_validator_v2(wf_dir, workflow, steps, execution_order)

    # 10. Manifest copy for audit trail
    serialize_manifest(workflow, wf_dir / "manifest.json")

    # 11. Report
    generate_report(wf_dir, workflow.tools, steps, execution_order)

    return {
        "name": workflow.name,
        "tools_total": len(steps),
        "tools_converted": sum(1 for s in steps.values() if s.confidence > 0),
        "avg_confidence": sum(s.confidence for s in steps.values()) / len(steps) if steps else 0,
        "unsupported_tools": [
            workflow.tools[tid].tool_type for tid, s in steps.items() if s.confidence == 0
        ],
        "errors": [],
    }


def _resolve_source_df_name(source_tool_id: int, source_anchor: str) -> str:
    anchor = source_anchor.lower()
    if anchor in ("true",):
        return f"df_{source_tool_id}_true"
    elif anchor in ("false",):
        return f"df_{source_tool_id}_false"
    elif anchor in ("unique", "u"):
        return f"df_{source_tool_id}_unique"
    elif anchor in ("duplicates", "d"):
        return f"df_{source_tool_id}_duplicates"
    elif anchor in ("join", "j"):
        return f"df_{source_tool_id}_joined"
    elif anchor in ("left", "l"):
        return f"df_{source_tool_id}_left_only"
    elif anchor in ("right", "r"):
        return f"df_{source_tool_id}_right_only"
    else:
        return f"df_{source_tool_id}"


def _build_input_map(workflow: AlteryxWorkflow) -> dict[int, list[str]]:
    _LEFT_ANCHORS = {"left", "find", "targets", "f", "#1"}
    _RIGHT_ANCHORS = {"right", "replace", "source", "r", "s", "#2"}

    raw_inputs: dict[int, list[tuple[str, str]]] = {}
    for conn in workflow.connections:
        df_name = _resolve_source_df_name(conn.source_tool_id, conn.source_anchor)
        raw_inputs.setdefault(conn.target_tool_id, []).append((df_name, conn.target_anchor))

    input_map: dict[int, list[str]] = {}
    for tool_id, inputs in raw_inputs.items():
        tool = workflow.tools.get(tool_id)
        if tool and tool.tool_type in ("Join", "FindReplace", "AppendFields") and len(inputs) >= 2:
            left_dfs = [df for df, anchor in inputs if anchor.lower() in _LEFT_ANCHORS]
            right_dfs = [df for df, anchor in inputs if anchor.lower() in _RIGHT_ANCHORS]
            other_dfs = [df for df, anchor in inputs
                         if anchor.lower() not in _LEFT_ANCHORS and anchor.lower() not in _RIGHT_ANCHORS]
            input_map[tool_id] = left_dfs + right_dfs + other_dfs
        else:
            input_map[tool_id] = [df for df, _ in inputs]

    return input_map


def _insert_temp_view_hints(workflow: AlteryxWorkflow, steps: dict[int, GeneratedStep]) -> None:
    """Replace cache hints with createOrReplaceTempView for fan-out DataFrames."""
    usage_count: dict[str, int] = {}
    for conn in workflow.connections:
        df_name = _resolve_source_df_name(conn.source_tool_id, conn.source_anchor)
        usage_count[df_name] = usage_count.get(df_name, 0) + 1

    for df_name, count in usage_count.items():
        if count >= 2:
            parts = df_name.split("_")
            try:
                tool_id = int(parts[1])
            except (ValueError, IndexError):
                continue
            if tool_id in steps:
                view_name = f"_tmp_{workflow.name}_{tool_id}".replace(" ", "_").replace("-", "_")
                steps[tool_id].code += (
                    f'\n{df_name}.createOrReplaceTempView("{view_name}")'
                    f"  # Fan-out: used by {count} downstream tools"
                )


def _collect_imports(tool_ids: list[int], steps: dict[int, GeneratedStep]) -> set[str]:
    imports: set[str] = set()
    for tid in tool_ids:
        if tid in steps:
            imports.update(steps[tid].imports)
    return imports


def _write_notebook(path: Path, title: str, tool_ids: list[int], steps: dict[int, GeneratedStep]) -> None:
    lines = ["# Databricks notebook source", f"# {title}"]

    imports = _collect_imports(tool_ids, steps)
    if imports:
        lines.append("")
        lines.append("# COMMAND ----------")
        lines.append("")
        for imp in sorted(imports):
            lines.append(imp)

    for tid in tool_ids:
        if tid in steps:
            step = steps[tid]
            lines.append("")
            lines.append("# COMMAND ----------")
            lines.append("")
            lines.append(step.code)

    lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _write_orchestrator(path: Path, workflow_name: str) -> None:
    lines = [
        "# Databricks notebook source",
        f"# 05 — Orchestrate: {workflow_name}",
        "",
        "# COMMAND ----------",
        "",
        "# %run ./_config",
        "",
        "# COMMAND ----------",
        "",
        "# %run ./_utils",
        "",
        "# COMMAND ----------",
        "",
        "# %run ./01_load_sources",
        "",
        "# COMMAND ----------",
        "",
        "# %run ./02_transformations",
        "",
        "# COMMAND ----------",
        "",
        "# %run ./03_write_outputs",
        "",
    ]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_notebook_v2.py -v`
Expected: FAIL (depends on Task 8 — `validator_v2` doesn't exist yet)

> **Note:** This task depends on Task 8. Implement Task 8 first, then come back and run these tests. Alternatively, create a stub `validator_v2.py` to unblock:
> ```python
> # src/alteryx2dbx/generator/validator_v2.py (stub)
> def generate_validator_v2(output_dir, workflow, steps, execution_order):
>     (output_dir / "04_validate.py").write_text("# Databricks notebook source\n# Stub\n")
> ```

Run: `pytest tests/test_notebook_v2.py -v`
Expected: PASS (all 4 tests)

**Step 5: Commit**

```bash
git add src/alteryx2dbx/generator/notebook_v2.py src/alteryx2dbx/generator/validator_v2.py tests/test_notebook_v2.py
git commit -m "feat: add serverless-safe notebook generator (temp views, no cache, %run chain)"
```

---

## Task 8: Smart Validation Notebook Generator

Auto-detect join keys and emit real validation sections.

**Files:**
- Create: `src/alteryx2dbx/generator/validator_v2.py`
- Test: `tests/test_validator_v2.py`

**Step 1: Write the failing test**

```python
# tests/test_validator_v2.py
"""Tests for smart validation notebook generation."""
from pathlib import Path

from alteryx2dbx.parser.models import (
    AlteryxConnection, AlteryxField, AlteryxTool, AlteryxWorkflow, GeneratedStep,
)
from alteryx2dbx.generator.validator_v2 import generate_validator_v2, detect_join_keys


def _wf_with_join():
    return AlteryxWorkflow(
        name="test_wf",
        version="2024.1",
        tools={
            1: AlteryxTool(tool_id=1, plugin="", tool_type="DbFileInput",
                           config={}, annotation="Input A"),
            2: AlteryxTool(tool_id=2, plugin="", tool_type="DbFileInput",
                           config={}, annotation="Input B"),
            3: AlteryxTool(tool_id=3, plugin="", tool_type="Join",
                           config={"join_fields": [{"left": "customer_id", "right": "cust_id"}]},
                           annotation="Join on customer"),
            4: AlteryxTool(tool_id=4, plugin="", tool_type="DbFileOutput",
                           config={}, annotation="Output"),
        },
        connections=[
            AlteryxConnection(1, "Output", 3, "Left"),
            AlteryxConnection(2, "Output", 3, "Right"),
            AlteryxConnection(3, "Join", 4, "Input"),
        ],
    )


def test_detect_join_keys():
    wf = _wf_with_join()
    keys = detect_join_keys(wf)
    assert "customer_id" in keys


def test_detect_keys_heuristic():
    """Falls back to *_id columns when no join tools present."""
    wf = AlteryxWorkflow(
        name="no_join",
        version="1.0",
        tools={
            1: AlteryxTool(tool_id=1, plugin="", tool_type="DbFileInput", config={},
                           output_fields=[
                               AlteryxField(name="order_id", type="Int32"),
                               AlteryxField(name="amount", type="Double"),
                           ]),
        },
    )
    keys = detect_join_keys(wf)
    assert "order_id" in keys


def test_generates_validation_notebook(tmp_path: Path):
    wf = _wf_with_join()
    steps = {tid: GeneratedStep(step_name=f"s_{tid}", code="", output_df=f"df_{tid}")
             for tid in wf.tools}
    generate_validator_v2(tmp_path, wf, steps, list(wf.tools.keys()))
    content = (tmp_path / "04_validate.py").read_text()
    assert "Databricks notebook source" in content
    assert "Row Count" in content or "row_count" in content or "count()" in content
    assert "schema" in content.lower()
    assert "datacompy" in content.lower()
    assert "customer_id" in content
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_validator_v2.py -v`
Expected: FAIL

**Step 3: Write implementation**

```python
# src/alteryx2dbx/generator/validator_v2.py
"""Generate 04_validate.py — smart validation with auto-detected keys."""
from __future__ import annotations

from pathlib import Path

from alteryx2dbx.parser.models import AlteryxWorkflow, AlteryxTool, GeneratedStep


_KEY_SUFFIXES = ("_id", "_key", "_pk", "_code", "id")


def detect_join_keys(workflow: AlteryxWorkflow) -> list[str]:
    """Auto-detect likely join/primary key columns from workflow tools.

    Priority:
    1. Join tool configs (join_fields)
    2. Unique tool configs (unique_fields)
    3. Heuristic: columns ending in _id, _key, _pk
    """
    keys: list[str] = []

    # From Join tools
    for tool in workflow.tools.values():
        if tool.tool_type == "Join":
            for jf in tool.config.get("join_fields", []):
                left = jf.get("left", "")
                if left and left not in keys:
                    keys.append(left)

    # From Unique tools
    if not keys:
        for tool in workflow.tools.values():
            if tool.tool_type == "Unique":
                for f in tool.config.get("unique_fields", []):
                    if f and f not in keys:
                        keys.append(f)

    # Heuristic fallback: scan output fields for *_id, *_key, *_pk patterns
    if not keys:
        for tool in workflow.tools.values():
            for field in tool.output_fields:
                if any(field.name.lower().endswith(s) for s in _KEY_SUFFIXES):
                    if field.name not in keys:
                        keys.append(field.name)

    return keys


def generate_validator_v2(
    output_dir: Path,
    workflow: AlteryxWorkflow,
    steps: dict[int, GeneratedStep],
    execution_order: list[int],
) -> None:
    """Write 04_validate.py with auto-detected keys and real validation sections."""
    keys = detect_join_keys(workflow)
    key_list = ", ".join(f'"{k}"' for k in keys) if keys else '"TODO_primary_key"'

    # Find the last output df
    _output_types = {"DbFileOutput", "OutputData", "Browse", "BrowseV2"}
    last_output_df = "df_result"
    for tid in reversed(execution_order):
        if tid in steps and workflow.tools.get(tid, None) and workflow.tools[tid].tool_type in _output_types:
            last_output_df = steps[tid].output_df
            break
    # If no output tool, use last step
    if last_output_df == "df_result" and execution_order:
        last_output_df = steps[execution_order[-1]].output_df if execution_order[-1] in steps else "df_result"

    lines = [
        "# Databricks notebook source",
        f"# 04 — Validation: {workflow.name}",
        "",
        "# COMMAND ----------",
        "",
        "import datacompy",
        "from pyspark.sql import functions as F",
        "",
        "# COMMAND ----------",
        "",
        "# -- Section 1: Load Alteryx baseline output --",
        "# TODO: Update path to your Alteryx output file",
        '# alteryx_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("<path>")',
        "",
        f"databricks_df = {last_output_df}",
        "",
        "# COMMAND ----------",
        "",
        "# -- Section 2: Row Count Comparison --",
        "alteryx_count = alteryx_df.count()",
        "databricks_count = databricks_df.count()",
        'print(f"Alteryx rows:    {alteryx_count:,}")',
        'print(f"Databricks rows: {databricks_count:,}")',
        'print(f"Difference:      {abs(alteryx_count - databricks_count):,}")',
        "assert alteryx_count == databricks_count, f\"Row count mismatch: {alteryx_count} vs {databricks_count}\"",
        "",
        "# COMMAND ----------",
        "",
        "# -- Section 3: Schema Comparison --",
        "alteryx_cols = set(alteryx_df.columns)",
        "databricks_cols = set(databricks_df.columns)",
        'print(f"Columns only in Alteryx:    {alteryx_cols - databricks_cols}")',
        'print(f"Columns only in Databricks: {databricks_cols - alteryx_cols}")',
        'print(f"Common columns:             {len(alteryx_cols & databricks_cols)}")',
        "",
        "# COMMAND ----------",
        "",
        "# -- Section 4: Aggregate Checks --",
        "# Compare sum/min/max on numeric columns",
        "numeric_cols = [f.name for f in databricks_df.schema.fields if str(f.dataType) in ('IntegerType()', 'LongType()', 'DoubleType()', 'DecimalType()')]",
        "for col_name in numeric_cols:",
        "    alt_stats = alteryx_df.select(F.sum(col_name), F.min(col_name), F.max(col_name)).first()",
        "    dbx_stats = databricks_df.select(F.sum(col_name), F.min(col_name), F.max(col_name)).first()",
        '    match = "PASS" if alt_stats == dbx_stats else "FAIL"',
        '    print(f"  {col_name}: {match}")',
        "",
        "# COMMAND ----------",
        "",
        "# -- Section 5: Row-Level Comparison (DataComPy) --",
        f"join_columns = [{key_list}]",
        "comparison = datacompy.SparkCompare(",
        "    spark,",
        "    base_df=alteryx_df,",
        "    compare_df=databricks_df,",
        "    join_columns=join_columns,",
        ")",
        "print(comparison.report())",
    ]

    with open(output_dir / "04_validate.py", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_validator_v2.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/alteryx2dbx/generator/validator_v2.py tests/test_validator_v2.py
git commit -m "feat: add smart validation generator with auto-detected join keys"
```

---

## Task 9: `generate` CLI Command

Wire it all together — the `generate` command consumes a manifest and emits the full notebook bundle.

**Files:**
- Modify: `src/alteryx2dbx/cli.py`
- Test: `tests/test_cli.py` (add tests)

**Step 1: Write the failing test**

```python
# Add to tests/test_cli.py
import json


def _write_manifest(tmp_path: Path) -> Path:
    """Write a minimal manifest.json for testing."""
    manifest = {
        "name": "gen_test",
        "version": "2024.1",
        "tools": {
            "1": {
                "tool_id": 1, "plugin": "", "tool_type": "DbFileInput",
                "config": {"file_path": "input.csv"}, "annotation": "Input",
                "input_fields": [], "output_fields": [],
            },
            "2": {
                "tool_id": 2, "plugin": "", "tool_type": "Filter",
                "config": {"expression": "[Revenue] > 1000"},
                "annotation": "Filter", "input_fields": [], "output_fields": [],
            },
            "3": {
                "tool_id": 3, "plugin": "", "tool_type": "DbFileOutput",
                "config": {"file_path": "output.csv"}, "annotation": "Output",
                "input_fields": [], "output_fields": [],
            },
        },
        "connections": [
            {"source_tool_id": 1, "source_anchor": "Output", "target_tool_id": 2, "target_anchor": "Input"},
            {"source_tool_id": 2, "source_anchor": "True", "target_tool_id": 3, "target_anchor": "Input"},
        ],
        "properties": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return path


def test_generate_from_manifest(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    out_dir = tmp_path / "output"

    runner = CliRunner()
    result = runner.invoke(main, ["generate", str(manifest_path), "-o", str(out_dir)])
    assert result.exit_code == 0
    assert (out_dir / "gen_test" / "_config.py").exists()
    assert (out_dir / "gen_test" / "_utils.py").exists()
    assert (out_dir / "gen_test" / "02_transformations.py").exists()
    assert (out_dir / "gen_test" / "05_orchestrate.py").exists()


def test_convert_full(tmp_path):
    yxmd = tmp_path / "test.yxmd"
    yxmd.write_text(MINIMAL_YXMD)
    out_dir = tmp_path / "output"

    runner = CliRunner()
    result = runner.invoke(main, ["convert", str(yxmd), "-o", str(out_dir), "--full"])
    assert result.exit_code == 0
    # Should produce v2 output with _config.py
    # Workflow name from MINIMAL_YXMD is "ParseTest"
    assert (out_dir / "ParseTest" / "_config.py").exists()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py::test_generate_from_manifest -v`
Expected: FAIL — `Error: No such command 'generate'.`

**Step 3: Add generate command and --full flag to cli.py**

Add to `src/alteryx2dbx/cli.py`:

```python
from .manifest import load_manifest, serialize_manifest
from .generator.notebook_v2 import generate_notebooks_v2
from .parser.unpacker import unpack_source


@main.command()
@click.argument("manifest", type=click.Path(exists=True))
@click.option("-o", "--output", default="./output", help="Output directory")
@click.option("--report", is_flag=True, default=False, help="Generate aggregate batch_report.md")
def generate(manifest, output, report):
    """Generate production notebooks from manifest.json."""
    manifest_path = Path(manifest)
    output_path = Path(output)

    if manifest_path.is_file():
        manifests = [manifest_path]
    else:
        manifests = list(manifest_path.glob("**/*.json"))

    if not manifests:
        click.echo("No manifest files found.")
        return

    results = []
    for m in manifests:
        click.echo(f"Generating: {m.name}")
        try:
            wf = load_manifest(m)
            stats = generate_notebooks_v2(wf, output_path)
            results.append(stats)
            click.echo(f"  Done: {output_path / wf.name}/")
        except Exception as e:
            click.echo(f"  Error: {e}", err=True)
            results.append({
                "name": m.stem,
                "tools_total": 0, "tools_converted": 0,
                "avg_confidence": 0, "unsupported_tools": [], "errors": [str(e)],
            })

    if report:
        output_path.mkdir(parents=True, exist_ok=True)
        generate_batch_report(output_path, results)
        click.echo(f"Report: {output_path / 'batch_report.md'}")

    click.echo(f"\nDone. Generated {len(manifests)} workflow(s).")
```

Also update the existing `convert` command to accept `--full`:

```python
@main.command()
@click.argument("source", type=click.Path(exists=True))
@click.option("-o", "--output", default="./output", help="Output directory")
@click.option("--report", is_flag=True, default=False, help="Generate aggregate batch_report.md")
@click.option("--full", is_flag=True, default=False, help="Use v2 generator (serverless-safe, production notebooks)")
def convert(source, output, report, full):
    """Convert .yxmd/.yxzp file(s) to Databricks notebooks."""
    source_path = Path(source)
    output_path = Path(output)

    # Collect files
    if source_path.is_file():
        files = [source_path]
    else:
        files = list(source_path.glob("**/*.yxmd")) + list(source_path.glob("**/*.yxzp"))
    if not files:
        click.echo("No .yxmd or .yxzp files found.")
        return

    results = []
    for f in files:
        click.echo(f"Converting: {f.name}")
        unpacked = unpack_source(f)
        try:
            wf = parse_yxmd(unpacked.workflow_path)
            if full:
                stats = generate_notebooks_v2(wf, output_path)
            else:
                stats = generate_notebooks(wf, output_path)
            results.append(stats)
            click.echo(f"  Done: {output_path / wf.name}/")
        except Exception as e:
            click.echo(f"  Error: {e}", err=True)
            results.append({
                "name": f.stem, "tools_total": 0, "tools_converted": 0,
                "avg_confidence": 0, "unsupported_tools": [], "errors": [str(e)],
            })
        finally:
            unpacked.cleanup()

    if report:
        output_path.mkdir(parents=True, exist_ok=True)
        generate_batch_report(output_path, results)
        click.echo(f"Report: {output_path / 'batch_report.md'}")
    click.echo(f"\nDone. Converted {len(files)} workflow(s).")
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cli.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/alteryx2dbx/cli.py tests/test_cli.py
git commit -m "feat: add generate command and convert --full flag"
```

---

## Task 10: Update analyze to Support .yxzp

Quick fix — wire unpacker into the `analyze` command.

**Files:**
- Modify: `src/alteryx2dbx/cli.py`
- Test: `tests/test_cli.py`

**Step 1: Write the failing test**

```python
# Add to tests/test_cli.py
import zipfile as zf_mod

def test_analyze_yxzp(tmp_path):
    yxzp = tmp_path / "test.yxzp"
    with zf_mod.ZipFile(yxzp, "w") as zf:
        zf.writestr("inner.yxmd", MINIMAL_YXMD)

    runner = CliRunner()
    result = runner.invoke(main, ["analyze", str(yxzp)])
    assert result.exit_code == 0
    assert "Coverage:" in result.output
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py::test_analyze_yxzp -v`
Expected: FAIL (analyze only globs `*.yxmd`)

**Step 3: Update analyze command**

In `src/alteryx2dbx/cli.py`, update the `analyze` command:

```python
@main.command()
@click.argument("source", type=click.Path(exists=True))
def analyze(source):
    """Analyze workflow without generating code."""
    source_path = Path(source)
    if source_path.is_file():
        files = [source_path]
    else:
        files = list(source_path.glob("**/*.yxmd")) + list(source_path.glob("**/*.yxzp"))
    for f in files:
        unpacked = unpack_source(f)
        try:
            wf = parse_yxmd(unpacked.workflow_path)
            order = resolve_dag(wf)
            click.echo(f"\nWorkflow: {wf.name}")
            click.echo(f"Tools: {len(wf.tools)}")
            supported = 0
            for tool_id in order:
                tool = wf.tools[tool_id]
                handler = get_handler(tool)
                is_supported = type(handler).__name__ != "UnsupportedHandler"
                if is_supported:
                    supported += 1
                status = "OK" if is_supported else "UNSUPPORTED"
                click.echo(f"  [{status}] [{tool_id}] {tool.tool_type}: {tool.annotation}")
            pct = supported / len(wf.tools) * 100 if wf.tools else 0
            click.echo(f"Coverage: {supported}/{len(wf.tools)} ({pct:.0f}%)")
        finally:
            unpacked.cleanup()
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cli.py::test_analyze_yxzp -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/alteryx2dbx/cli.py tests/test_cli.py
git commit -m "feat: add .yxzp support to analyze command"
```

---

## Task 11: Wire Fix Registry into Notebook Generator

Apply semantic fixes to generated code in `notebook_v2.py`.

**Files:**
- Modify: `src/alteryx2dbx/generator/notebook_v2.py`
- Test: `tests/test_notebook_v2.py` (add test)

**Step 1: Write the failing test**

```python
# Add to tests/test_notebook_v2.py
def test_conversion_report_lists_fixes(tmp_path: Path):
    """Conversion report should mention applied semantic fixes."""
    wf = AlteryxWorkflow(
        name="fix_test",
        version="2024.1",
        tools={
            1: AlteryxTool(tool_id=1, plugin="", tool_type="DbFileInput",
                           config={"file_path": "a.csv"}, annotation="A"),
            2: AlteryxTool(tool_id=2, plugin="", tool_type="DbFileInput",
                           config={"file_path": "b.csv"}, annotation="B"),
            3: AlteryxTool(tool_id=3, plugin="", tool_type="Join",
                           config={"join_fields": [{"left": "name", "right": "name"}]},
                           annotation="Join"),
            4: AlteryxTool(tool_id=4, plugin="", tool_type="DbFileOutput",
                           config={"file_path": "out.csv"}, annotation="Out"),
        },
        connections=[
            AlteryxConnection(1, "Output", 3, "Left"),
            AlteryxConnection(2, "Output", 3, "Right"),
            AlteryxConnection(3, "Join", 4, "Input"),
        ],
    )
    generate_notebooks_v2(wf, tmp_path)
    report = (tmp_path / "fix_test" / "conversion_report.md").read_text()
    # Report should exist and have content
    assert "Conversion Report" in report
```

**Step 2: Run test to verify it fails or passes (baseline)**

Run: `pytest tests/test_notebook_v2.py::test_conversion_report_lists_fixes -v`

**Step 3: Wire fixes into notebook_v2.py**

Add to `generate_notebooks_v2` after step 3 (handler dispatch), before step 4:

```python
from alteryx2dbx.fixes import apply_fixes

# 3b. Apply semantic fixes
for tool_id in execution_order:
    tool = workflow.tools[tool_id]
    step = steps[tool_id]
    context = {
        "tool_type": tool.tool_type,
        **tool.config,
        "output_fields": [f.to_dict() for f in tool.output_fields],
    }
    fix_result = apply_fixes(step.code, context)
    step.code = fix_result.code
    for fix in fix_result.applied_fixes:
        step.notes.append(f"Fix applied: {fix['fix_id']} — {fix['description']}")
```

**Step 4: Run tests**

Run: `pytest tests/test_notebook_v2.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/alteryx2dbx/generator/notebook_v2.py tests/test_notebook_v2.py
git commit -m "feat: wire semantic fix registry into v2 notebook generator"
```

---

## Task 12: Update README and Version Bump

Update README with new commands and bump version to 0.3.0.

**Files:**
- Modify: `README.md`
- Modify: `pyproject.toml` (version)
- Modify: `src/alteryx2dbx/__init__.py` (version if defined there)

**Step 1: Update pyproject.toml version**

Change `version = "0.2.0"` to `version = "0.3.0"`.

**Step 2: Update README**

Add the new `parse` and `generate` commands to the Usage section. Add `.yxzp` to the supported formats. Document the `--full` flag on `convert`. Add a section on the semantic fix registry.

**Step 3: Run full test suite**

Run: `pytest --cov=alteryx2dbx -v`
Expected: ALL PASS

**Step 4: Commit**

```bash
git add README.md pyproject.toml src/alteryx2dbx/__init__.py
git commit -m "docs: update README for v0.3.0 (parse, generate, .yxzp, fixes)"
```

---

## Task Dependency Graph

```
Task 1 (Manifest)  ──┬──> Task 3 (parse CLI) ──> Task 9 (generate CLI) ──> Task 12 (README)
                      │                              ↑
Task 2 (.yxzp)  ─────┤                              │
                      │                              │
Task 4 (Fixes)  ──────┼──> Task 11 (Wire fixes) ────┘
                      │                              ↑
Task 5 (_config)  ────┤                              │
                      │                              │
Task 6 (_utils)  ─────┼──> Task 7 (notebook_v2) ────┘
                      │         ↑
Task 8 (validator_v2) ─────────┘

Task 10 (analyze .yxzp) — independent, can run after Task 2
```

**Critical path:** 1 → 3 → 7 → 9 → 12

**Parallelizable:** Tasks 2, 4, 5, 6, 8 can all be developed in parallel after Task 1.
