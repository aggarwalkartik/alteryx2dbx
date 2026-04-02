# Box Tool Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Box.com connector support so workflows containing Box Input/Output tools generate working PySpark code that reads from and writes to Box via the Box Python SDK.

**Architecture:** Add prefix matching to the handler registry (third lookup tier after exact plugin and tool_type). Create BoxInputHandler and BoxOutputHandler that generate `boxsdk` code using Databricks Secrets for auth. Conditionally add Box helpers to `_config.py` and `_utils.py` when workflows contain Box tools.

**Tech Stack:** Python, boxsdk (generated code only — not a CLI dependency), Databricks Secrets, pytest, click

**Spec:** `docs/specs/2026-04-02-box-tool-support-design.md`

---

### Task 1: Add Prefix Matching to Handler Registry

**Files:**
- Modify: `src/alteryx2dbx/handlers/registry.py`
- Test: `tests/test_registry_prefix.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_registry_prefix.py`:

```python
from alteryx2dbx.handlers.registry import HandlerRegistry
from alteryx2dbx.handlers.base import ToolHandler
from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep


class FakeBoxHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        return GeneratedStep(
            step_name="fake_box",
            code="# fake",
            imports=set(),
            input_dfs=[],
            output_df="df_1",
            notes=[],
            confidence=0.8,
        )


def _make_tool(plugin: str, tool_type: str = "") -> AlteryxTool:
    return AlteryxTool(
        tool_id=1,
        plugin=plugin,
        tool_type=tool_type or plugin,
        config={},
        annotation="",
        output_fields=[],
    )


def test_prefix_match_box_input():
    reg = HandlerRegistry()
    reg.register_prefix("box_input_v", FakeBoxHandler)
    handler = reg.get(_make_tool("box_input_v1.0.3"))
    assert type(handler).__name__ == "FakeBoxHandler"


def test_prefix_match_box_input_different_version():
    reg = HandlerRegistry()
    reg.register_prefix("box_input_v", FakeBoxHandler)
    handler = reg.get(_make_tool("box_input_v2.5.7"))
    assert type(handler).__name__ == "FakeBoxHandler"


def test_prefix_does_not_match_unrelated():
    reg = HandlerRegistry()
    reg.register_prefix("box_input_v", FakeBoxHandler)
    handler = reg.get(_make_tool("AlteryxBasePluginsGui.Filter.Filter", "Filter"))
    assert type(handler).__name__ == "UnsupportedHandler"


def test_exact_match_takes_priority_over_prefix():
    """Exact plugin match should win over prefix match."""
    reg = HandlerRegistry()

    class ExactHandler(ToolHandler):
        def convert(self, tool, input_df_names=None):
            return GeneratedStep("exact", "# exact", set(), [], "df_1", [], 1.0)

    reg.register("box_input_v1.0.3", ExactHandler)
    reg.register_prefix("box_input_v", FakeBoxHandler)
    handler = reg.get(_make_tool("box_input_v1.0.3"))
    assert type(handler).__name__ == "ExactHandler"


def test_type_match_takes_priority_over_prefix():
    """tool_type exact match should win over prefix match."""
    reg = HandlerRegistry()

    class TypeHandler(ToolHandler):
        def convert(self, tool, input_df_names=None):
            return GeneratedStep("type", "# type", set(), [], "df_1", [], 1.0)

    reg.register_type("box_input_v1.0.3", TypeHandler)
    reg.register_prefix("box_input_v", FakeBoxHandler)
    handler = reg.get(_make_tool("box_input_v1.0.3"))
    assert type(handler).__name__ == "TypeHandler"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_registry_prefix.py -v`
Expected: FAIL — `HandlerRegistry` has no `register_prefix` method.

- [ ] **Step 3: Implement prefix matching**

Edit `src/alteryx2dbx/handlers/registry.py` — add `_prefix_handlers` dict, `register_prefix` method, and prefix lookup in `get()`:

```python
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool

from .base import ToolHandler, UnsupportedHandler


class HandlerRegistry:
    def __init__(self):
        self._handlers: dict[str, type[ToolHandler]] = {}
        self._type_handlers: dict[str, type[ToolHandler]] = {}
        self._prefix_handlers: dict[str, type[ToolHandler]] = {}

    def register(self, plugin: str, handler_cls: type[ToolHandler]):
        self._handlers[plugin] = handler_cls

    def register_type(self, tool_type: str, handler_cls: type[ToolHandler]):
        self._type_handlers[tool_type] = handler_cls

    def register_prefix(self, prefix: str, handler_cls: type[ToolHandler]):
        self._prefix_handlers[prefix] = handler_cls

    def get(self, tool: AlteryxTool) -> ToolHandler:
        handler_cls = self._handlers.get(tool.plugin)
        if handler_cls:
            return handler_cls()
        handler_cls = self._type_handlers.get(tool.tool_type)
        if handler_cls:
            return handler_cls()
        for prefix, cls in self._prefix_handlers.items():
            if tool.plugin.startswith(prefix):
                return cls()
        return UnsupportedHandler()


_registry = HandlerRegistry()


def get_handler(tool: AlteryxTool) -> ToolHandler:
    return _registry.get(tool)


def register_handler(plugin: str, handler_cls: type[ToolHandler]):
    _registry.register(plugin, handler_cls)


def register_type_handler(tool_type: str, handler_cls: type[ToolHandler]):
    _registry.register_type(tool_type, handler_cls)


def register_prefix_handler(prefix: str, handler_cls: type[ToolHandler]):
    _registry.register_prefix(prefix, handler_cls)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_registry_prefix.py -v`
Expected: All 5 tests PASS.

- [ ] **Step 5: Run full test suite to check for regressions**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest --tb=short`
Expected: All existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/alteryx2dbx/handlers/registry.py tests/test_registry_prefix.py
git commit -m "feat: add prefix matching to handler registry"
```

---

### Task 2: Add Box Config Extraction to Parser

**Files:**
- Modify: `src/alteryx2dbx/parser/xml_parser.py`
- Test: `tests/test_box_parser.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_box_parser.py`:

```python
from alteryx2dbx.parser.xml_parser import parse_yxmd
from pathlib import Path

BOX_INPUT_YXMD = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2024.1">
  <Properties><MetaInfo><Name>BoxTest</Name></MetaInfo></Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="box_input_v1.0.3">
        <Position x="78" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <authType>ServicePrincipal</authType>
          <FilePath>/reports/monthly/sales.csv</FilePath>
          <boxFileId>123456789</boxFileId>
          <boxParentId>987654321</boxParentId>
          <fileName>sales.csv</fileName>
          <FileFormat>Delimited</FileFormat>
          <DelimitedHasHeader>True</DelimitedHasHeader>
          <Delimiter>COMMA</Delimiter>
        </Configuration>
        <Annotation DisplayMode="0">
          <Name>Box Sales Data</Name>
        </Annotation>
      </Properties>
    </Node>
  </Nodes>
  <Connections></Connections>
</AlteryxDocument>
'''

BOX_OUTPUT_YXMD = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2024.1">
  <Properties><MetaInfo><Name>BoxOutputTest</Name></MetaInfo></Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="box_output_v1.0.3">
        <Position x="78" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <authType>EndUser</authType>
          <FilePath>/reports/output/results.xlsx</FilePath>
          <boxFileId>111222333</boxFileId>
          <boxParentId>444555666</boxParentId>
          <fileName>results.xlsx</fileName>
          <FileFormat>Excel</FileFormat>
          <ExcelSheetNameValues>Sheet1</ExcelSheetNameValues>
          <ExistingFileBehavior>Overwrite</ExistingFileBehavior>
        </Configuration>
        <Annotation DisplayMode="0">
          <Name>Box Output Results</Name>
        </Annotation>
      </Properties>
    </Node>
  </Nodes>
  <Connections></Connections>
</AlteryxDocument>
'''


def test_box_input_config_extraction(tmp_path):
    wf_file = tmp_path / "box_test.yxmd"
    wf_file.write_text(BOX_INPUT_YXMD, encoding="utf-8")
    wf = parse_yxmd(wf_file)
    tool = wf.tools[1]
    assert tool.tool_type == "box_input_v1.0.3"
    assert tool.config["box_file_id"] == "123456789"
    assert tool.config["box_parent_id"] == "987654321"
    assert tool.config["file_name"] == "sales.csv"
    assert tool.config["file_format"] == "Delimited"
    assert tool.config["auth_type"] == "ServicePrincipal"
    assert tool.config["has_header"] is True
    assert tool.config["delimiter"] == "COMMA"
    assert tool.config["file_path"] == "/reports/monthly/sales.csv"


def test_box_output_config_extraction(tmp_path):
    wf_file = tmp_path / "box_out_test.yxmd"
    wf_file.write_text(BOX_OUTPUT_YXMD, encoding="utf-8")
    wf = parse_yxmd(wf_file)
    tool = wf.tools[1]
    assert tool.tool_type == "box_output_v1.0.3"
    assert tool.config["box_file_id"] == "111222333"
    assert tool.config["file_format"] == "Excel"
    assert tool.config["excel_sheet"] == "Sheet1"
    assert tool.config["existing_file_behavior"] == "Overwrite"
    assert tool.config["auth_type"] == "EndUser"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_box_parser.py -v`
Expected: FAIL — `box_file_id` key not in config (no Box extraction logic yet).

- [ ] **Step 3: Add Box config extraction to parser**

Edit `src/alteryx2dbx/parser/xml_parser.py`. Add at the end of the `_extract_config` function's elif chain (before the `return config` line):

```python
    elif tool_type.startswith("box_input_v") or tool_type.startswith("box_output_v"):
        _extract_box_config(config_el, config)
```

Add the new function after `_extract_text_input_config`:

```python
def _extract_box_config(config_el: ET.Element, config: dict) -> None:
    """Extract Box.com connector configuration."""
    _simple_fields = {
        "FilePath": "file_path",
        "boxFileId": "box_file_id",
        "boxParentId": "box_parent_id",
        "fileName": "file_name",
        "FileFormat": "file_format",
        "authType": "auth_type",
        "Delimiter": "delimiter",
        "ExistingFileBehavior": "existing_file_behavior",
    }
    for xml_name, config_key in _simple_fields.items():
        el = config_el.find(xml_name)
        if el is not None and el.text:
            config[config_key] = el.text

    # Boolean: header row
    header_el = config_el.find("DelimitedHasHeader")
    if header_el is not None and header_el.text:
        config["has_header"] = header_el.text.lower() == "true"

    # Excel sheet name
    sheet_el = config_el.find("ExcelSheetNameValues")
    if sheet_el is not None and sheet_el.text:
        config["excel_sheet"] = sheet_el.text
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_box_parser.py -v`
Expected: Both tests PASS.

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest --tb=short`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/alteryx2dbx/parser/xml_parser.py tests/test_box_parser.py
git commit -m "feat: add Box tool config extraction to XML parser"
```

---

### Task 3: Create BoxInputHandler

**Files:**
- Create: `src/alteryx2dbx/handlers/box_input.py`
- Modify: `src/alteryx2dbx/handlers/__init__.py`
- Test: `tests/test_handlers/test_box_input.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_handlers/test_box_input.py`:

```python
from alteryx2dbx.parser.models import AlteryxTool
from alteryx2dbx.handlers.registry import get_handler
import alteryx2dbx.handlers  # noqa: F401 — trigger registration


def _make_box_input_tool(
    tool_id=1,
    file_format="Delimited",
    box_file_id="123456789",
    file_name="sales.csv",
    has_header=True,
    delimiter="COMMA",
    excel_sheet=None,
):
    config = {
        "box_file_id": box_file_id,
        "box_parent_id": "987654321",
        "file_name": file_name,
        "file_format": file_format,
        "file_path": f"/reports/{file_name}",
        "auth_type": "ServicePrincipal",
        "has_header": has_header,
        "delimiter": delimiter,
        "_raw_xml": "<Configuration/>",
    }
    if excel_sheet:
        config["excel_sheet"] = excel_sheet
    return AlteryxTool(
        tool_id=tool_id,
        plugin="box_input_v1.0.3",
        tool_type="box_input_v1.0.3",
        config=config,
        annotation="Box Sales Data",
        output_fields=[],
    )


def test_box_input_csv():
    tool = _make_box_input_tool()
    handler = get_handler(tool)
    step = handler.convert(tool)
    assert "box_client.file" in step.code
    assert "123456789" in step.code
    assert "pd.read_csv" in step.code
    assert step.confidence == 0.8
    assert step.output_df == "df_1"
    assert "from boxsdk" not in step.code  # boxsdk import is in _utils.py


def test_box_input_excel():
    tool = _make_box_input_tool(file_format="Excel", file_name="data.xlsx", excel_sheet="Sheet1")
    step = get_handler(tool).convert(tool)
    assert "pd.read_excel" in step.code
    assert "Sheet1" in step.code


def test_box_input_json():
    tool = _make_box_input_tool(file_format="JSON", file_name="data.json")
    step = get_handler(tool).convert(tool)
    assert "pd.read_json" in step.code


def test_box_input_avro_unsupported():
    tool = _make_box_input_tool(file_format="Avro", file_name="data.avro")
    step = get_handler(tool).convert(tool)
    assert "TODO" in step.code
    assert step.confidence < 0.8


def test_box_input_delimiter_tab():
    tool = _make_box_input_tool(delimiter="TAB")
    step = get_handler(tool).convert(tool)
    assert "\\t" in step.code


def test_box_input_no_header():
    tool = _make_box_input_tool(has_header=False)
    step = get_handler(tool).convert(tool)
    assert "header=False" in step.code or "header=None" in step.code
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_handlers/test_box_input.py -v`
Expected: FAIL — handler resolves to `UnsupportedHandler` (no BoxInputHandler registered).

- [ ] **Step 3: Implement BoxInputHandler**

Create `src/alteryx2dbx/handlers/box_input.py`:

```python
"""Handler for Alteryx Box Input tool (box_input_v*)."""
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep

from .base import ToolHandler
from .registry import register_prefix_handler


_DELIMITER_MAP = {
    "COMMA": ",",
    "TAB": "\\t",
    "PIPE": "|",
    "SPACE": " ",
}


class BoxInputHandler(ToolHandler):
    def convert(
        self, tool: AlteryxTool, input_df_names: list[str] | None = None
    ) -> GeneratedStep:
        config = tool.config
        box_file_id = config.get("box_file_id", "UNKNOWN_FILE_ID")
        file_name = config.get("file_name", "unknown")
        file_format = config.get("file_format", "Delimited")
        output_df = f"df_{tool.tool_id}"

        imports = {"from io import BytesIO", "import pandas as pd"}
        notes = [
            f"Box source: {file_name} (ID: {box_file_id})",
            "Box auth requires Databricks Secret scope — see _config.py",
        ]

        if file_format == "Avro":
            code = (
                f"# Box Input: {tool.annotation or file_name}\n"
                f"# TODO: Avro format from Box not auto-converted — implement manually\n"
                f"# Box file ID: {box_file_id}, file: {file_name}\n"
                f"{output_df} = spark.createDataFrame([], schema=None)  # PLACEHOLDER"
            )
            return GeneratedStep(
                step_name=f"box_input_{tool.tool_id}",
                code=code,
                imports=set(),
                input_dfs=[],
                output_df=output_df,
                notes=notes + ["Avro from Box: manual implementation required"],
                confidence=0.3,
            )

        read_expr = self._read_expression(config, file_format)

        code = (
            f"# Box Input: {tool.annotation or file_name} (file: {file_name})\n"
            f'_box_bytes_{tool.tool_id} = BytesIO(box_client.file("{box_file_id}").content())\n'
            f"{output_df} = spark.createDataFrame({read_expr})"
        )

        return GeneratedStep(
            step_name=f"box_input_{tool.tool_id}",
            code=code,
            imports=imports,
            input_dfs=[],
            output_df=output_df,
            notes=notes,
            confidence=0.8,
        )

    @staticmethod
    def _read_expression(config: dict, file_format: str) -> str:
        if file_format == "Excel":
            sheet = config.get("excel_sheet", "Sheet1")
            return f'pd.read_excel(_box_bytes_{config.get("tool_id", "")}, sheet_name="{sheet}")'
        elif file_format == "JSON":
            return f'pd.read_json(_box_bytes_{config.get("tool_id", "")})'
        else:
            # Delimited (CSV default)
            delimiter = _DELIMITER_MAP.get(config.get("delimiter", "COMMA"), ",")
            has_header = config.get("has_header", True)
            header_param = "0" if has_header else "None"
            return f'pd.read_csv(_box_bytes_{config.get("tool_id", "")}, sep="{delimiter}", header={header_param})'


register_prefix_handler("box_input_v", BoxInputHandler)
```

Wait — there's a bug: `config` doesn't have `tool_id`. Fix the `_read_expression` to take `tool_id` as a parameter instead:

```python
"""Handler for Alteryx Box Input tool (box_input_v*)."""
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep

from .base import ToolHandler
from .registry import register_prefix_handler


_DELIMITER_MAP = {
    "COMMA": ",",
    "TAB": "\\t",
    "PIPE": "|",
    "SPACE": " ",
}


class BoxInputHandler(ToolHandler):
    def convert(
        self, tool: AlteryxTool, input_df_names: list[str] | None = None
    ) -> GeneratedStep:
        config = tool.config
        box_file_id = config.get("box_file_id", "UNKNOWN_FILE_ID")
        file_name = config.get("file_name", "unknown")
        file_format = config.get("file_format", "Delimited")
        output_df = f"df_{tool.tool_id}"

        imports = {"from io import BytesIO", "import pandas as pd"}
        notes = [
            f"Box source: {file_name} (ID: {box_file_id})",
            "Box auth requires Databricks Secret scope — see _config.py",
        ]

        if file_format == "Avro":
            code = (
                f"# Box Input: {tool.annotation or file_name}\n"
                f"# TODO: Avro format from Box not auto-converted — implement manually\n"
                f"# Box file ID: {box_file_id}, file: {file_name}\n"
                f"{output_df} = spark.createDataFrame([], schema=None)  # PLACEHOLDER"
            )
            return GeneratedStep(
                step_name=f"box_input_{tool.tool_id}",
                code=code,
                imports=set(),
                input_dfs=[],
                output_df=output_df,
                notes=notes + ["Avro from Box: manual implementation required"],
                confidence=0.3,
            )

        read_expr = self._read_expression(tool.tool_id, config, file_format)

        code = (
            f"# Box Input: {tool.annotation or file_name} (file: {file_name})\n"
            f'_box_bytes_{tool.tool_id} = BytesIO(box_client.file("{box_file_id}").content())\n'
            f"{output_df} = spark.createDataFrame({read_expr})"
        )

        return GeneratedStep(
            step_name=f"box_input_{tool.tool_id}",
            code=code,
            imports=imports,
            input_dfs=[],
            output_df=output_df,
            notes=notes,
            confidence=0.8,
        )

    @staticmethod
    def _read_expression(tool_id: int, config: dict, file_format: str) -> str:
        if file_format == "Excel":
            sheet = config.get("excel_sheet", "Sheet1")
            return f'pd.read_excel(_box_bytes_{tool_id}, sheet_name="{sheet}")'
        elif file_format == "JSON":
            return f"pd.read_json(_box_bytes_{tool_id})"
        else:
            delimiter = _DELIMITER_MAP.get(config.get("delimiter", "COMMA"), ",")
            has_header = config.get("has_header", True)
            header_param = "0" if has_header else "None"
            return f'pd.read_csv(_box_bytes_{tool_id}, sep="{delimiter}", header={header_param})'


register_prefix_handler("box_input_v", BoxInputHandler)
```

Add to `src/alteryx2dbx/handlers/__init__.py` — append:

```python
from . import box_input
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_handlers/test_box_input.py -v`
Expected: All 6 tests PASS.

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest --tb=short`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/alteryx2dbx/handlers/box_input.py src/alteryx2dbx/handlers/__init__.py tests/test_handlers/test_box_input.py
git commit -m "feat: add BoxInputHandler with prefix registration"
```

---

### Task 4: Create BoxOutputHandler

**Files:**
- Create: `src/alteryx2dbx/handlers/box_output.py`
- Modify: `src/alteryx2dbx/handlers/__init__.py`
- Test: `tests/test_handlers/test_box_output.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_handlers/test_box_output.py`:

```python
from alteryx2dbx.parser.models import AlteryxTool
from alteryx2dbx.handlers.registry import get_handler
import alteryx2dbx.handlers  # noqa: F401


def _make_box_output_tool(
    tool_id=5,
    file_format="Delimited",
    file_name="output.csv",
    box_parent_id="444555666",
    existing_behavior="Overwrite",
):
    return AlteryxTool(
        tool_id=tool_id,
        plugin="box_output_v1.0.3",
        tool_type="box_output_v1.0.3",
        config={
            "box_file_id": "111222333",
            "box_parent_id": box_parent_id,
            "file_name": file_name,
            "file_format": file_format,
            "file_path": f"/reports/output/{file_name}",
            "auth_type": "ServicePrincipal",
            "existing_file_behavior": existing_behavior,
            "_raw_xml": "<Configuration/>",
        },
        annotation="Box Output Results",
        output_fields=[],
    )


def test_box_output_csv():
    tool = _make_box_output_tool()
    step = get_handler(tool).convert(tool, input_df_names=["df_4"])
    assert "toPandas()" in step.code
    assert "to_csv" in step.code
    assert "upload_stream" in step.code or "update_contents_with_stream" in step.code
    assert step.confidence == 0.7
    assert step.input_dfs == ["df_4"]
    assert step.output_df == "df_5"


def test_box_output_excel():
    tool = _make_box_output_tool(file_format="Excel", file_name="out.xlsx")
    step = get_handler(tool).convert(tool, input_df_names=["df_4"])
    assert "to_excel" in step.code


def test_box_output_json():
    tool = _make_box_output_tool(file_format="JSON", file_name="out.json")
    step = get_handler(tool).convert(tool, input_df_names=["df_4"])
    assert "to_json" in step.code


def test_box_output_abort_behavior():
    tool = _make_box_output_tool(existing_behavior="Abort")
    step = get_handler(tool).convert(tool, input_df_names=["df_4"])
    assert "TODO" in step.code or "Abort" in step.code


def test_box_output_overwrite():
    tool = _make_box_output_tool(existing_behavior="Overwrite")
    step = get_handler(tool).convert(tool, input_df_names=["df_4"])
    assert "update_contents_with_stream" in step.code
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_handlers/test_box_output.py -v`
Expected: FAIL — handler resolves to `UnsupportedHandler`.

- [ ] **Step 3: Implement BoxOutputHandler**

Create `src/alteryx2dbx/handlers/box_output.py`:

```python
"""Handler for Alteryx Box Output tool (box_output_v*)."""
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep

from .base import ToolHandler
from .registry import register_prefix_handler


class BoxOutputHandler(ToolHandler):
    def convert(
        self, tool: AlteryxTool, input_df_names: list[str] | None = None
    ) -> GeneratedStep:
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        config = tool.config
        box_file_id = config.get("box_file_id", "UNKNOWN_FILE_ID")
        box_parent_id = config.get("box_parent_id", "UNKNOWN_FOLDER_ID")
        file_name = config.get("file_name", "output.csv")
        file_format = config.get("file_format", "Delimited")
        existing_behavior = config.get("existing_file_behavior", "Overwrite")
        output_df = f"df_{tool.tool_id}"

        imports = {"from io import BytesIO", "import pandas as pd"}
        notes = [
            f"Box output: {file_name} (folder ID: {box_parent_id})",
            "Box auth requires Databricks Secret scope — see _config.py",
        ]

        write_code = self._write_code(tool.tool_id, input_df, file_format)
        upload_code = self._upload_code(
            tool.tool_id, box_file_id, box_parent_id, file_name, existing_behavior
        )

        code = (
            f"# Box Output: {tool.annotation or file_name} (file: {file_name})\n"
            f"{write_code}\n"
            f"{upload_code}\n"
            f"{output_df} = {input_df}  # Passthrough for downstream"
        )

        return GeneratedStep(
            step_name=f"box_output_{tool.tool_id}",
            code=code,
            imports=imports,
            input_dfs=[input_df],
            output_df=output_df,
            notes=notes,
            confidence=0.7,
        )

    @staticmethod
    def _write_code(tool_id: int, input_df: str, file_format: str) -> str:
        buf = f"_out_bytes_{tool_id}"
        if file_format == "Excel":
            return (
                f"{buf} = BytesIO()\n"
                f"{input_df}.toPandas().to_excel({buf}, index=False)\n"
                f"{buf}.seek(0)"
            )
        elif file_format == "JSON":
            return (
                f"{buf} = BytesIO()\n"
                f"{buf}.write({input_df}.toPandas().to_json(orient='records').encode())\n"
                f"{buf}.seek(0)"
            )
        else:
            return (
                f"{buf} = BytesIO()\n"
                f"{input_df}.toPandas().to_csv({buf}, index=False)\n"
                f"{buf}.seek(0)"
            )

    @staticmethod
    def _upload_code(
        tool_id: int,
        box_file_id: str,
        box_parent_id: str,
        file_name: str,
        existing_behavior: str,
    ) -> str:
        buf = f"_out_bytes_{tool_id}"
        if existing_behavior == "Overwrite":
            return f'box_client.file("{box_file_id}").update_contents_with_stream({buf})'
        elif existing_behavior == "Abort":
            return (
                f"# ExistingFileBehavior: Abort — TODO: check if file exists before upload\n"
                f'box_client.folder("{box_parent_id}").upload_stream({buf}, "{file_name}")'
            )
        else:
            return (
                f"# TODO: ExistingFileBehavior '{existing_behavior}' — implement manually\n"
                f'box_client.folder("{box_parent_id}").upload_stream({buf}, "{file_name}")'
            )


register_prefix_handler("box_output_v", BoxOutputHandler)
```

Add to `src/alteryx2dbx/handlers/__init__.py` — append:

```python
from . import box_output
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_handlers/test_box_output.py -v`
Expected: All 5 tests PASS.

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest --tb=short`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/alteryx2dbx/handlers/box_output.py src/alteryx2dbx/handlers/__init__.py tests/test_handlers/test_box_output.py
git commit -m "feat: add BoxOutputHandler with prefix registration"
```

---

### Task 5: Conditional Box Sections in Config and Utils Notebooks

**Files:**
- Modify: `src/alteryx2dbx/generator/config_notebook.py`
- Modify: `src/alteryx2dbx/generator/utils_notebook.py`
- Modify: `src/alteryx2dbx/generator/notebook_v2.py`
- Test: `tests/test_box_notebooks.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_box_notebooks.py`:

```python
from pathlib import Path
from alteryx2dbx.parser.xml_parser import parse_yxmd
from alteryx2dbx.generator.notebook_v2 import generate_notebooks_v2

BOX_WORKFLOW_YXMD = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2024.1">
  <Properties><MetaInfo><Name>BoxWorkflow</Name></MetaInfo></Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="box_input_v1.0.3">
        <Position x="78" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <authType>ServicePrincipal</authType>
          <FilePath>/data/input.csv</FilePath>
          <boxFileId>123456789</boxFileId>
          <boxParentId>987654321</boxParentId>
          <fileName>input.csv</fileName>
          <FileFormat>Delimited</FileFormat>
          <DelimitedHasHeader>True</DelimitedHasHeader>
          <Delimiter>COMMA</Delimiter>
        </Configuration>
        <Annotation DisplayMode="0"><Name>Box Input</Name></Annotation>
      </Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="box_output_v1.0.3">
        <Position x="258" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <authType>ServicePrincipal</authType>
          <FilePath>/data/output.csv</FilePath>
          <boxFileId>111222333</boxFileId>
          <boxParentId>444555666</boxParentId>
          <fileName>output.csv</fileName>
          <FileFormat>Delimited</FileFormat>
          <ExistingFileBehavior>Overwrite</ExistingFileBehavior>
        </Configuration>
        <Annotation DisplayMode="0"><Name>Box Output</Name></Annotation>
      </Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection>
      <Origin ToolID="1" Connection="Output"/>
      <Destination ToolID="2" Connection="Output"/>
    </Connection>
  </Connections>
</AlteryxDocument>
'''


def test_box_workflow_generates_config_with_box_scope(tmp_path):
    wf_file = tmp_path / "box_wf.yxmd"
    wf_file.write_text(BOX_WORKFLOW_YXMD, encoding="utf-8")
    wf = parse_yxmd(wf_file)
    output_dir = tmp_path / "output"
    generate_notebooks_v2(wf, output_dir)
    config_content = (output_dir / "BoxWorkflow" / "_config.py").read_text()
    assert "box_secret_scope" in config_content
    assert "BOX_SECRET_SCOPE" in config_content


def test_box_workflow_generates_utils_with_box_client(tmp_path):
    wf_file = tmp_path / "box_wf.yxmd"
    wf_file.write_text(BOX_WORKFLOW_YXMD, encoding="utf-8")
    wf = parse_yxmd(wf_file)
    output_dir = tmp_path / "output"
    generate_notebooks_v2(wf, output_dir)
    utils_content = (output_dir / "BoxWorkflow" / "_utils.py").read_text()
    assert "get_box_client" in utils_content
    assert "box_client" in utils_content


def test_non_box_workflow_has_no_box_config(tmp_path):
    """simple_filter.yxmd has no Box tools — config should not mention Box."""
    from tests import FIXTURES_DIR
    wf = parse_yxmd(FIXTURES_DIR / "simple_filter.yxmd")
    output_dir = tmp_path / "output"
    generate_notebooks_v2(wf, output_dir)
    config_content = (output_dir / "simple_filter" / "_config.py").read_text()
    assert "box_secret_scope" not in config_content
```

Note: The `FIXTURES_DIR` import may not exist. If it doesn't, use the path directly:

```python
def test_non_box_workflow_has_no_box_config(tmp_path):
    from alteryx2dbx.parser.xml_parser import parse_yxmd
    fixtures = Path(__file__).parent / "fixtures"
    wf = parse_yxmd(fixtures / "simple_filter.yxmd")
    output_dir = tmp_path / "output"
    generate_notebooks_v2(wf, output_dir)
    config_content = (output_dir / "simple_filter" / "_config.py").read_text()
    assert "box_secret_scope" not in config_content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_box_notebooks.py -v`
Expected: FAIL — `box_secret_scope` not in config.

- [ ] **Step 3: Add Box detection helper to notebook_v2.py**

Add near the top of `src/alteryx2dbx/generator/notebook_v2.py`, after the existing `_LOAD_TYPES` / `_OUTPUT_TYPES` sets:

```python
def _has_box_tools(workflow: AlteryxWorkflow) -> bool:
    """Return True if the workflow contains any Box connector tools."""
    return any(
        t.plugin.startswith("box_input_v") or t.plugin.startswith("box_output_v")
        for t in workflow.tools.values()
    )
```

Update the `_LOAD_TYPES` and `_OUTPUT_TYPES` sets to include Box tools by checking plugin prefix. The simplest approach: after step classification (step 6 in `generate_notebooks_v2`), also classify box tools:

In the classification section of `generate_notebooks_v2`, update `_LOAD_TYPES` and `_OUTPUT_TYPES`:

Add at the top of the file:

```python
_BOX_INPUT_PREFIX = "box_input_v"
_BOX_OUTPUT_PREFIX = "box_output_v"
```

And update step 6 (classify steps) in `generate_notebooks_v2` to:

```python
    # 6. Classify steps
    load_ids = [
        tid for tid in execution_order
        if workflow.tools[tid].tool_type in _LOAD_TYPES
        or workflow.tools[tid].plugin.startswith(_BOX_INPUT_PREFIX)
    ]
    output_ids = [
        tid for tid in execution_order
        if workflow.tools[tid].tool_type in _OUTPUT_TYPES
        or workflow.tools[tid].plugin.startswith(_BOX_OUTPUT_PREFIX)
    ]
    transform_ids = [tid for tid in execution_order if tid not in load_ids and tid not in output_ids]
```

Pass `has_box` flag to config and utils generators. Update calls in `generate_notebooks_v2`:

```python
    # 9. Config notebook
    has_box = _has_box_tools(workflow)
    generate_config_notebook(workflow, wf_dir, has_box=has_box)

    # 10. Utils notebook
    generate_utils_notebook(wf_dir, has_box=has_box)
```

- [ ] **Step 4: Add Box section to config_notebook.py**

In `src/alteryx2dbx/generator/config_notebook.py`, update the signature and add a Box cell:

Update `generate_config_notebook` signature to accept `has_box`:

```python
def generate_config_notebook(workflow: AlteryxWorkflow, output_dir: Path, *, has_box: bool = False) -> None:
```

After cell 5 (OUTPUTS dict), add conditionally:

```python
    # Cell 6 (conditional): Box configuration
    if has_box:
        cells.append(
            "# Box.com configuration\n"
            'dbutils.widgets.text("box_secret_scope", "box", "Databricks Secret scope for Box JWT credentials")\n'
            'BOX_SECRET_SCOPE = dbutils.widgets.get("box_secret_scope")'
        )
```

- [ ] **Step 5: Add Box section to utils_notebook.py**

In `src/alteryx2dbx/generator/utils_notebook.py`, update the function to conditionally append Box helper:

```python
_BOX_UTILS_TEMPLATE = r'''
# COMMAND ----------

# Box.com client — authenticated via Databricks Secrets
import json
from boxsdk import JWTAuth, Client

def get_box_client():
    """Return an authenticated Box client using JWT credentials from Databricks Secrets."""
    jwt_config = json.loads(dbutils.secrets.get(BOX_SECRET_SCOPE, "jwt_config"))
    auth = JWTAuth.from_settings_dictionary(jwt_config)
    return Client(auth)

box_client = get_box_client()
'''


def generate_utils_notebook(output_dir: Path, *, has_box: bool = False) -> Path:
    """Write the _utils.py Databricks notebook into *output_dir*."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "_utils.py"
    content = _UTILS_TEMPLATE
    if has_box:
        content += _BOX_UTILS_TEMPLATE
    path.write_text(content, encoding="utf-8")
    return path
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_box_notebooks.py -v`
Expected: All 3 tests PASS.

- [ ] **Step 7: Run full test suite**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest --tb=short`
Expected: All tests pass. (Existing tests for config/utils notebooks may need signature updates if they call these functions directly — check and fix if needed.)

- [ ] **Step 8: Commit**

```bash
git add src/alteryx2dbx/generator/config_notebook.py src/alteryx2dbx/generator/utils_notebook.py src/alteryx2dbx/generator/notebook_v2.py tests/test_box_notebooks.py
git commit -m "feat: conditional Box sections in _config.py and _utils.py notebooks"
```

---

### Task 6: E2E Test — Box Workflow Through Full Pipeline

**Files:**
- Create: `tests/fixtures/box_workflow.yxmd`
- Test: `tests/test_box_e2e.py`

- [ ] **Step 1: Create Box workflow fixture**

Create `tests/fixtures/box_workflow.yxmd`:

```xml
<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2024.1">
  <Properties>
    <MetaInfo>
      <Name>box_workflow</Name>
      <Author>Test Author</Author>
      <Description>Test workflow with Box Input, Filter, and Box Output</Description>
    </MetaInfo>
  </Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="box_input_v1.0.3">
        <Position x="78" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <authType>ServicePrincipal</authType>
          <FilePath>/data/sales.csv</FilePath>
          <boxFileId>123456789</boxFileId>
          <boxParentId>987654321</boxParentId>
          <fileName>sales.csv</fileName>
          <FileFormat>Delimited</FileFormat>
          <DelimitedHasHeader>True</DelimitedHasHeader>
          <Delimiter>COMMA</Delimiter>
        </Configuration>
        <Annotation DisplayMode="0"><Name>Box Sales Input</Name></Annotation>
        <MetaInfo connection="Output">
          <RecordInfo>
            <Field name="region" size="254" type="V_WString"/>
            <Field name="amount" type="Double"/>
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
          <Expression>[amount] &gt; 1000</Expression>
        </Configuration>
        <Annotation DisplayMode="0"><Name>High Value Filter</Name></Annotation>
        <MetaInfo connection="True">
          <RecordInfo>
            <Field name="region" size="254" type="V_WString"/>
            <Field name="amount" type="Double"/>
          </RecordInfo>
        </MetaInfo>
      </Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="box_output_v1.0.3">
        <Position x="438" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <authType>ServicePrincipal</authType>
          <FilePath>/data/output/high_value.csv</FilePath>
          <boxFileId>111222333</boxFileId>
          <boxParentId>444555666</boxParentId>
          <fileName>high_value.csv</fileName>
          <FileFormat>Delimited</FileFormat>
          <ExistingFileBehavior>Overwrite</ExistingFileBehavior>
        </Configuration>
        <Annotation DisplayMode="0"><Name>Box High Value Output</Name></Annotation>
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
      <Destination ToolID="3" Connection="Output"/>
    </Connection>
  </Connections>
</AlteryxDocument>
```

- [ ] **Step 2: Write the E2E test**

Create `tests/test_box_e2e.py`:

```python
from pathlib import Path
from click.testing import CliRunner
from alteryx2dbx.cli import main

FIXTURES = Path(__file__).parent / "fixtures"


def test_box_workflow_convert_full(tmp_path):
    """Full pipeline: Box Input → Filter → Box Output via convert --full."""
    runner = CliRunner()
    result = runner.invoke(main, [
        "convert", str(FIXTURES / "box_workflow.yxmd"),
        "-o", str(tmp_path), "--full",
    ])
    assert result.exit_code == 0, result.output
    wf_dir = tmp_path / "box_workflow"
    assert (wf_dir / "01_load_sources.py").exists()
    assert (wf_dir / "02_transformations.py").exists()
    assert (wf_dir / "03_write_outputs.py").exists()
    assert (wf_dir / "_config.py").exists()
    assert (wf_dir / "_utils.py").exists()

    # Box Input should be in load_sources
    load_content = (wf_dir / "01_load_sources.py").read_text()
    assert "box_client.file" in load_content
    assert "123456789" in load_content

    # Box Output should be in write_outputs
    output_content = (wf_dir / "03_write_outputs.py").read_text()
    assert "update_contents_with_stream" in output_content

    # Config should have Box scope
    config_content = (wf_dir / "_config.py").read_text()
    assert "BOX_SECRET_SCOPE" in config_content

    # Utils should have box_client
    utils_content = (wf_dir / "_utils.py").read_text()
    assert "get_box_client" in utils_content


def test_box_workflow_analyze():
    """Analyze should show Box tools as OK (not UNSUPPORTED)."""
    runner = CliRunner()
    result = runner.invoke(main, ["analyze", str(FIXTURES / "box_workflow.yxmd")])
    assert result.exit_code == 0
    assert "[OK]" in result.output
    assert "box_input" in result.output.lower() or "Box" in result.output
```

- [ ] **Step 3: Run the E2E test**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest tests/test_box_e2e.py -v`
Expected: Both tests PASS.

- [ ] **Step 4: Run full test suite**

Run: `cd /Users/kartikaggarwal/Projects/tools/alteryx2dbx && python -m pytest --tb=short`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/box_workflow.yxmd tests/test_box_e2e.py
git commit -m "test: add Box workflow E2E test through full pipeline"
```

---

### Task 7: Update pyproject.toml with Optional Box Dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add optional Box dependency**

Edit `pyproject.toml` — update `[project.optional-dependencies]`:

```toml
[project.optional-dependencies]
box = ["boxsdk[jwt]>=3.0"]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
]
```

- [ ] **Step 2: Commit**

```bash
git add pyproject.toml
git commit -m "chore: add boxsdk as optional dependency"
```
