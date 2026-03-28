# alteryx2dbx Phase 1-3 Comprehensive Upgrade

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Upgrade alteryx2dbx from 9 tools / 20 functions (67% coverage) to 30+ tools / 80+ functions (95% coverage) with production-quality optimizations.

**Architecture:** Extend the existing 5-stage pipeline. Each task adds handlers + parser config extraction + tests. Expression grammar gets IN/Switch support. Generator gets cache/batching/validation optimizations.

**Tech Stack:** Python 3.11+, lark, networkx, jinja2, click, pyspark (type stubs only)

---

## Phase 1: Close the Coverage Gap (67% → 90%)

### Task 1: Easy Handlers Batch — Sample, Unique, RecordID, AutoField, CountRecords, AppendFields

**Files:**
- Create: `src/alteryx2dbx/handlers/sample.py`
- Create: `src/alteryx2dbx/handlers/unique.py`
- Create: `src/alteryx2dbx/handlers/record_id.py`
- Create: `src/alteryx2dbx/handlers/auto_field.py`
- Create: `src/alteryx2dbx/handlers/count_records.py`
- Create: `src/alteryx2dbx/handlers/append_fields.py`
- Modify: `src/alteryx2dbx/handlers/__init__.py`
- Modify: `src/alteryx2dbx/parser/xml_parser.py`
- Create: `tests/test_handlers/test_easy_handlers.py`

**Step 1: Add parser config extraction for new tools**

In `xml_parser.py`, add to `_extract_config`:
```python
elif tool_type == "Sample":
    _extract_sample_config(config_el, config)
elif tool_type == "Unique":
    _extract_unique_config(config_el, config)
elif tool_type in ("RecordID", "AutoField", "CountRecords"):
    pass  # minimal config
elif tool_type == "AppendFields":
    pass  # uses default connections
```

Add extraction functions:
```python
def _extract_sample_config(config_el, config):
    mode = config_el.find("Mode")
    if mode is not None and mode.text:
        config["sample_mode"] = mode.text  # "First", "Last", "Random", "Percentage"
    n = config_el.find("N")
    if n is not None and n.text:
        config["sample_n"] = int(n.text)
    pct = config_el.find("Pct")
    if pct is not None and pct.text:
        config["sample_pct"] = float(pct.text)

def _extract_unique_config(config_el, config):
    fields = []
    for f in config_el.findall(".//UniqueField"):
        fields.append(f.get("field", ""))
    # Also check UniqueFields/Field pattern
    for f in config_el.findall(".//UniqueFields/Field"):
        fields.append(f.get("field", ""))
    if fields:
        config["unique_fields"] = fields
```

**Step 2: Write all 6 handlers**

```python
# handlers/sample.py
class SampleHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        mode = tool.config.get("sample_mode", "First")
        n = tool.config.get("sample_n", 100)
        pct = tool.config.get("sample_pct", 0.1)

        if mode == "Percentage":
            code = f"df_{tool.tool_id} = {input_df}.sample(fraction={pct})"
        elif mode == "Random":
            code = f"df_{tool.tool_id} = {input_df}.orderBy(F.rand()).limit({n})"
        elif mode == "Last":
            code = (
                f"_count_{tool.tool_id} = {input_df}.count()\n"
                f"df_{tool.tool_id} = {input_df}.tail({n})"
            )
        else:  # First (default)
            code = f"df_{tool.tool_id} = {input_df}.limit({n})"

        code = f"# {tool.annotation or 'Sample'} (Tool {tool.tool_id})\n" + code
        return GeneratedStep(
            step_name=f"sample_{tool.tool_id}", code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df], output_df=f"df_{tool.tool_id}",
            confidence=1.0,
        )
register_type_handler("Sample", SampleHandler)
```

```python
# handlers/unique.py — dual output (Unique + Duplicates)
class UniqueHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        fields = tool.config.get("unique_fields", [])
        tid = tool.tool_id

        if fields:
            cols = ", ".join(f'"{f}"' for f in fields)
            code = (
                f"# {tool.annotation or 'Unique'} (Tool {tid})\n"
                f"df_{tid}_unique = {input_df}.dropDuplicates([{cols}])\n"
                f"df_{tid}_duplicates = {input_df}.subtract(df_{tid}_unique)\n"
                f"df_{tid} = df_{tid}_unique  # Default: Unique output"
            )
        else:
            code = (
                f"# {tool.annotation or 'Unique'} (Tool {tid})\n"
                f"df_{tid}_unique = {input_df}.dropDuplicates()\n"
                f"df_{tid}_duplicates = {input_df}.subtract(df_{tid}_unique)\n"
                f"df_{tid} = df_{tid}_unique  # Default: Unique output"
            )

        return GeneratedStep(
            step_name=f"unique_{tid}", code=code, imports=set(),
            input_dfs=[input_df], output_df=f"df_{tid}", confidence=1.0,
        )
register_type_handler("Unique", UniqueHandler)
```

```python
# handlers/record_id.py
class RecordIDHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        field_name = tool.config.get("FieldName", "RecordID")
        code = (
            f"# {tool.annotation or 'Record ID'} (Tool {tool.tool_id})\n"
            f'df_{tool.tool_id} = {input_df}.withColumn("{field_name}", '
            f"F.monotonically_increasing_id() + 1)"
        )
        return GeneratedStep(
            step_name=f"record_id_{tool.tool_id}", code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df], output_df=f"df_{tool.tool_id}", confidence=1.0,
        )
register_type_handler("RecordID", RecordIDHandler)
```

```python
# handlers/auto_field.py — passthrough (Spark handles type optimization natively)
class AutoFieldHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        code = (
            f"# {tool.annotation or 'Auto Field'} (Tool {tool.tool_id})\n"
            f"# Auto Field is a no-op in Spark — type optimization is handled natively\n"
            f"df_{tool.tool_id} = {input_df}"
        )
        return GeneratedStep(
            step_name=f"auto_field_{tool.tool_id}", code=code, imports=set(),
            input_dfs=[input_df], output_df=f"df_{tool.tool_id}", confidence=1.0,
        )
register_type_handler("AutoField", AutoFieldHandler)
```

```python
# handlers/count_records.py
class CountRecordsHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        code = (
            f"# {tool.annotation or 'Count Records'} (Tool {tool.tool_id})\n"
            f"df_{tool.tool_id} = spark.createDataFrame("
            f'[({input_df}.count(),)], ["Count"])'
        )
        return GeneratedStep(
            step_name=f"count_records_{tool.tool_id}", code=code, imports=set(),
            input_dfs=[input_df], output_df=f"df_{tool.tool_id}", confidence=1.0,
        )
register_type_handler("CountRecords", CountRecordsHandler)
```

```python
# handlers/append_fields.py — cartesian join
class AppendFieldsHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        target = input_df_names[0] if input_df_names and len(input_df_names) > 0 else "df_target"
        source = input_df_names[1] if input_df_names and len(input_df_names) > 1 else "df_source"
        code = (
            f"# {tool.annotation or 'Append Fields'} (Tool {tool.tool_id})\n"
            f"df_{tool.tool_id} = {target}.crossJoin({source})"
        )
        return GeneratedStep(
            step_name=f"append_fields_{tool.tool_id}", code=code, imports=set(),
            input_dfs=[target, source], output_df=f"df_{tool.tool_id}", confidence=1.0,
        )
register_type_handler("AppendFields", AppendFieldsHandler)
```

**Step 3: Update `__init__.py` to import all new handlers**

```python
from . import sample
from . import unique
from . import record_id
from . import auto_field
from . import count_records
from . import append_fields
```

**Step 4: Write tests**

Test each handler produces valid code, correct output_df name, correct confidence.
- Sample: test First (limit), Percentage (sample), Random modes
- Unique: test with fields, test dual output variables exist in code
- RecordID: test monotonically_increasing_id in code
- AutoField: test passthrough
- CountRecords: test createDataFrame in code
- AppendFields: test crossJoin with two inputs

**Step 5: Run tests, commit**

```bash
pytest tests/ -v
git add -A && git commit -m "feat: add Sample, Unique, RecordID, AutoField, CountRecords, AppendFields handlers"
```

---

### Task 2: Data Cleansing Handler

**Files:**
- Create: `src/alteryx2dbx/handlers/data_cleansing.py`
- Modify: `src/alteryx2dbx/parser/xml_parser.py`
- Create: `tests/test_handlers/test_data_cleansing.py`

**Step 1: Add parser extraction**

```python
elif tool_type == "DataCleansing":
    _extract_data_cleansing_config(config_el, config)
```

```python
def _extract_data_cleansing_config(config_el, config):
    config["cleanse_options"] = {}
    for opt in ("RemoveNull", "RemoveWhitespace", "TrimWhitespace",
                "RemoveLetters", "RemoveNumbers", "RemovePunctuation",
                "ModifyCase"):
        el = config_el.find(opt)
        if el is not None:
            config["cleanse_options"][opt] = el.get("value", el.text or "True")
    # Fields to cleanse
    fields = []
    for f in config_el.findall(".//Fields/Field"):
        fields.append(f.get("field", ""))
    if fields:
        config["cleanse_fields"] = fields
```

**Step 2: Write handler**

DataCleansing applies per-column operations: trim whitespace, remove nulls, modify case, remove special characters. Use `withColumns({})` batching (PySpark 3.3+) for efficiency.

```python
class DataCleansingHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        opts = tool.config.get("cleanse_options", {})
        fields = tool.config.get("cleanse_fields", [])
        tid = tool.tool_id

        lines = [f"# {tool.annotation or 'Data Cleansing'} (Tool {tid})"]
        lines.append(f"df_{tid} = {input_df}")

        # Build per-field expression chain
        for field in fields:
            expr = f'F.col("{field}")'
            if opts.get("TrimWhitespace") == "True":
                expr = f"F.trim({expr})"
            if opts.get("RemoveWhitespace") == "True":
                expr = f'F.regexp_replace({expr}, r"\\s+", "")'
            case_mode = opts.get("ModifyCase", "")
            if case_mode == "Upper":
                expr = f"F.upper({expr})"
            elif case_mode == "Lower":
                expr = f"F.lower({expr})"
            elif case_mode == "Title":
                expr = f"F.initcap({expr})"
            if opts.get("RemoveNull") == "True":
                expr = f'F.coalesce({expr}, F.lit(""))'
            lines.append(f'df_{tid} = df_{tid}.withColumn("{field}", {expr})')

        code = "\n".join(lines)
        return GeneratedStep(
            step_name=f"data_cleansing_{tid}", code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df], output_df=f"df_{tid}", confidence=1.0,
        )
register_type_handler("DataCleansing", DataCleansingHandler)
```

**Step 3: Write tests, run, commit**

```bash
git commit -m "feat: add DataCleansing handler"
```

---

### Task 3: Find Replace Handler

**Files:**
- Create: `src/alteryx2dbx/handlers/find_replace.py`
- Modify: `src/alteryx2dbx/parser/xml_parser.py`
- Create: `tests/test_handlers/test_find_replace.py`

**Step 1: Parser extraction**

FindReplace is in the parser's existing `_extract_join_config` (shared with Join since `tool_type in ("Join", "FindReplace")`). Add dedicated extraction:

```python
elif tool_type == "FindReplace":
    _extract_find_replace_config(config_el, config)
```

```python
def _extract_find_replace_config(config_el, config):
    find_field = config_el.find(".//FindField")
    if find_field is not None:
        config["find_field"] = find_field.get("field", "")
    replace_field = config_el.find(".//ReplaceField")
    if replace_field is not None:
        config["replace_field"] = replace_field.get("field", "")
    find_mode = config_el.find(".//FindMode")
    if find_mode is not None:
        config["find_mode"] = find_mode.text or "Normal"  # Normal, RegEx, Contains
    append = config_el.find(".//AppendFields")
    if append is not None:
        config["fr_append_fields"] = append.get("value", "False") == "True"
```

**Step 2: Write handler**

FindReplace takes two inputs (F=data, R=lookup). It joins on find_field, replaces matched values, falls back to original.

```python
class FindReplaceHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        data_df = input_df_names[0] if input_df_names and len(input_df_names) > 0 else "df_data"
        lookup_df = input_df_names[1] if input_df_names and len(input_df_names) > 1 else "df_lookup"
        tid = tool.tool_id
        find_field = tool.config.get("find_field", "find")
        replace_field = tool.config.get("replace_field", "replace")

        code = (
            f"# {tool.annotation or 'Find Replace'} (Tool {tid})\n"
            f'df_{tid} = {data_df}.join(\n'
            f'    {lookup_df}.select(\n'
            f'        F.col("{find_field}").alias("_fr_find_{tid}"),\n'
            f'        F.col("{replace_field}").alias("_fr_replace_{tid}")\n'
            f'    ),\n'
            f'    F.lower({data_df}["{find_field}"]) == F.lower(F.col("_fr_find_{tid}")),\n'
            f'    "left"\n'
            f')\n'
            f'df_{tid} = df_{tid}.withColumn(\n'
            f'    "{find_field}",\n'
            f'    F.coalesce(F.col("_fr_replace_{tid}"), {data_df}["{find_field}"])\n'
            f')\n'
            f'df_{tid} = df_{tid}.drop("_fr_find_{tid}", "_fr_replace_{tid}")'
        )
        return GeneratedStep(
            step_name=f"find_replace_{tid}", code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[data_df, lookup_df], output_df=f"df_{tid}", confidence=0.9,
        )
register_type_handler("FindReplace", FindReplaceHandler)
```

**Step 3: Update `_build_input_map` in notebook.py for Find Replace dual-input**

Add `FindReplace` to the dual-input ordering logic alongside `Join`:

```python
if tool and tool.tool_type in ("Join", "FindReplace", "AppendFields") and len(inputs) >= 2:
```

**Step 4: Tests, run, commit**

```bash
git commit -m "feat: add FindReplace handler with lookup join"
```

---

### Task 4: Transform Handlers — CrossTab, Transpose, RunningTotal, GenerateRows, Tile

**Files:**
- Create: `src/alteryx2dbx/handlers/cross_tab.py`
- Create: `src/alteryx2dbx/handlers/transpose.py`
- Create: `src/alteryx2dbx/handlers/running_total.py`
- Create: `src/alteryx2dbx/handlers/generate_rows.py`
- Create: `src/alteryx2dbx/handlers/tile.py`
- Modify: `src/alteryx2dbx/parser/xml_parser.py`
- Create: `tests/test_handlers/test_transform_handlers.py`

**Step 1: Parser extraction for all 5**

```python
elif tool_type == "CrossTab":
    _extract_cross_tab_config(config_el, config)
elif tool_type == "Transpose":
    _extract_transpose_config(config_el, config)
elif tool_type == "RunningTotal":
    _extract_running_total_config(config_el, config)
elif tool_type == "GenerateRows":
    _extract_generate_rows_config(config_el, config)
elif tool_type == "Tile":
    _extract_tile_config(config_el, config)
```

Extraction functions:

```python
def _extract_cross_tab_config(config_el, config):
    group_fields = [f.get("field", "") for f in config_el.findall(".//GroupFields/Field")]
    header_field = config_el.findtext(".//HeaderField", "")
    data_field = config_el.findtext(".//DataField", "")
    method = config_el.findtext(".//Method", "Sum")
    config["ct_group_fields"] = group_fields
    config["ct_header_field"] = header_field
    config["ct_data_field"] = data_field
    config["ct_method"] = method

def _extract_transpose_config(config_el, config):
    key_fields = [f.get("field", "") for f in config_el.findall(".//KeyFields/Field")]
    data_fields = [f.get("field", "") for f in config_el.findall(".//DataFields/Field")]
    config["tp_key_fields"] = key_fields
    config["tp_data_fields"] = data_fields

def _extract_running_total_config(config_el, config):
    running_field = config_el.findtext(".//RunningField", "")
    group_fields = [f.get("field", "") for f in config_el.findall(".//GroupFields/Field")]
    config["rt_running_field"] = running_field
    config["rt_group_fields"] = group_fields

def _extract_generate_rows_config(config_el, config):
    init_expr = config_el.findtext(".//InitExpression", "1")
    cond_expr = config_el.findtext(".//ConditionExpression", "RowCount <= 100")
    loop_expr = config_el.findtext(".//LoopExpression", "RowCount + 1")
    config["gr_init"] = init_expr
    config["gr_condition"] = cond_expr
    config["gr_loop"] = loop_expr

def _extract_tile_config(config_el, config):
    method = config_el.findtext(".//Method", "EqualRecords")
    num_tiles = config_el.findtext(".//NumTiles", "4")
    field = config_el.findtext(".//Field", "")
    config["tile_method"] = method
    config["tile_num"] = int(num_tiles)
    config["tile_field"] = field
```

**Step 2: Write handlers**

```python
# handlers/cross_tab.py
class CrossTabHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        group = tool.config.get("ct_group_fields", [])
        header = tool.config.get("ct_header_field", "")
        data = tool.config.get("ct_data_field", "")
        method = tool.config.get("ct_method", "Sum")

        agg_map = {"Sum": "F.sum", "Count": "F.count", "Avg": "F.avg",
                    "Min": "F.min", "Max": "F.max", "First": "F.first"}
        agg_func = agg_map.get(method, "F.sum")
        group_str = ", ".join(f'"{g}"' for g in group)

        code = (
            f'# {tool.annotation or "Cross Tab"} (Tool {tid})\n'
            f'df_{tid} = {input_df}.groupBy({group_str})'
            f'.pivot("{header}").agg({agg_func}("{data}"))'
        )
        return GeneratedStep(
            step_name=f"cross_tab_{tid}", code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df], output_df=f"df_{tid}", confidence=1.0,
        )
register_type_handler("CrossTab", CrossTabHandler)
```

```python
# handlers/transpose.py
class TransposeHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        key_fields = tool.config.get("tp_key_fields", [])
        data_fields = tool.config.get("tp_data_fields", [])

        if data_fields:
            n = len(data_fields)
            values = ", ".join(f"'{f}', `{f}`" for f in data_fields)
            key_str = ", ".join(f'"{k}"' for k in key_fields)
            code = (
                f'# {tool.annotation or "Transpose"} (Tool {tid})\n'
                f'df_{tid} = {input_df}.selectExpr(\n'
                f'    {key_str},\n'
                f'    "stack({n}, {values}) as (Name, Value)"\n'
                f')'
            )
        else:
            code = (
                f'# {tool.annotation or "Transpose"} (Tool {tid})\n'
                f'# TODO: Transpose with dynamic columns — specify data fields\n'
                f'df_{tid} = {input_df}'
            )

        return GeneratedStep(
            step_name=f"transpose_{tid}", code=code, imports=set(),
            input_dfs=[input_df], output_df=f"df_{tid}",
            confidence=1.0 if data_fields else 0.5,
        )
register_type_handler("Transpose", TransposeHandler)
```

```python
# handlers/running_total.py
class RunningTotalHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        field = tool.config.get("rt_running_field", "")
        group_fields = tool.config.get("rt_group_fields", [])

        if group_fields:
            partition = ", ".join(f'"{g}"' for g in group_fields)
            window = f"Window.partitionBy({partition}).rowsBetween(Window.unboundedPreceding, Window.currentRow)"
        else:
            window = "Window.orderBy(F.monotonically_increasing_id()).rowsBetween(Window.unboundedPreceding, Window.currentRow)"

        code = (
            f'# {tool.annotation or "Running Total"} (Tool {tid})\n'
            f'_window_{tid} = {window}\n'
            f'df_{tid} = {input_df}.withColumn(\n'
            f'    "RunningTotal_{field}",\n'
            f'    F.sum("{field}").over(_window_{tid})\n'
            f')'
        )
        return GeneratedStep(
            step_name=f"running_total_{tid}", code=code,
            imports={"from pyspark.sql import functions as F", "from pyspark.sql.window import Window"},
            input_dfs=[input_df], output_df=f"df_{tid}", confidence=1.0,
        )
register_type_handler("RunningTotal", RunningTotalHandler)
```

```python
# handlers/generate_rows.py
class GenerateRowsHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        tid = tool.tool_id
        init = tool.config.get("gr_init", "1")
        condition = tool.config.get("gr_condition", "RowCount <= 100")
        loop = tool.config.get("gr_loop", "RowCount + 1")

        code = (
            f'# {tool.annotation or "Generate Rows"} (Tool {tid})\n'
            f'# Alteryx: init={init}, condition={condition}, loop={loop}\n'
            f'# TODO: Translate loop logic — using spark.range as approximation\n'
            f'df_{tid} = spark.range(1, 101).toDF("RowCount")'
        )
        return GeneratedStep(
            step_name=f"generate_rows_{tid}", code=code, imports=set(),
            input_dfs=[], output_df=f"df_{tid}",
            notes=["Generate Rows uses loop logic — verify range matches Alteryx output"],
            confidence=0.5,
        )
register_type_handler("GenerateRows", GenerateRowsHandler)
```

```python
# handlers/tile.py
class TileHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        n = tool.config.get("tile_num", 4)
        field = tool.config.get("tile_field", "")

        if field:
            code = (
                f'# {tool.annotation or "Tile"} (Tool {tid})\n'
                f'df_{tid} = {input_df}.withColumn(\n'
                f'    "Tile",\n'
                f'    F.ntile({n}).over(Window.orderBy("{field}"))\n'
                f')'
            )
        else:
            code = (
                f'# {tool.annotation or "Tile"} (Tool {tid})\n'
                f'df_{tid} = {input_df}.withColumn(\n'
                f'    "Tile",\n'
                f'    F.ntile({n}).over(Window.orderBy(F.monotonically_increasing_id()))\n'
                f')'
            )

        return GeneratedStep(
            step_name=f"tile_{tid}", code=code,
            imports={"from pyspark.sql import functions as F", "from pyspark.sql.window import Window"},
            input_dfs=[input_df], output_df=f"df_{tid}", confidence=1.0,
        )
register_type_handler("Tile", TileHandler)
```

**Step 3: Update __init__.py, tests, run, commit**

```bash
git commit -m "feat: add CrossTab, Transpose, RunningTotal, GenerateRows, Tile handlers"
```

---

### Task 5: Parse Handlers — RegEx, TextToColumns, DateTime

**Files:**
- Create: `src/alteryx2dbx/handlers/regex.py`
- Create: `src/alteryx2dbx/handlers/text_to_columns.py`
- Create: `src/alteryx2dbx/handlers/date_time.py`
- Modify: `src/alteryx2dbx/parser/xml_parser.py`
- Create: `tests/test_handlers/test_parse_handlers.py`

**Step 1: Parser extraction**

```python
elif tool_type == "RegEx":
    _extract_regex_config(config_el, config)
elif tool_type == "TextToColumns":
    _extract_text_to_columns_config(config_el, config)
elif tool_type == "DateTime":
    _extract_datetime_config(config_el, config)
```

```python
def _extract_regex_config(config_el, config):
    config["rx_field"] = config_el.findtext(".//Field", "")
    config["rx_expression"] = config_el.findtext(".//RegExExpression",
                                                  config_el.findtext(".//Expression", ""))
    config["rx_mode"] = config_el.findtext(".//Mode", "Replace")  # Replace, Parse, Match, Tokenize
    config["rx_replace"] = config_el.findtext(".//ReplaceExpression", "")
    config["rx_case_insensitive"] = config_el.findtext(".//CaseInsensitive", "False") == "True"
    output_fields = []
    for f in config_el.findall(".//OutputFields/Field"):
        output_fields.append(f.get("field", ""))
    config["rx_output_fields"] = output_fields

def _extract_text_to_columns_config(config_el, config):
    config["ttc_field"] = config_el.findtext(".//Field", "")
    config["ttc_delimiter"] = config_el.findtext(".//Delimiter", ",")
    config["ttc_num_columns"] = int(config_el.findtext(".//NumFields", "2"))
    config["ttc_output_root"] = config_el.findtext(".//RootName", "")
    config["ttc_split_to_rows"] = config_el.findtext(".//SplitToRows", "False") == "True"

def _extract_datetime_config(config_el, config):
    config["dt_field"] = config_el.findtext(".//Field", "")
    config["dt_format_in"] = config_el.findtext(".//FormatIn", "")
    config["dt_format_out"] = config_el.findtext(".//FormatOut", "")
    config["dt_conversion"] = config_el.findtext(".//Conversion", "")
```

**Step 2: Write handlers**

```python
# handlers/regex.py — 4 modes: Replace, Parse, Match, Tokenize
class RegExHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        field = tool.config.get("rx_field", "")
        pattern = tool.config.get("rx_expression", "")
        mode = tool.config.get("rx_mode", "Replace")
        replace = tool.config.get("rx_replace", "")
        output_fields = tool.config.get("rx_output_fields", [])

        lines = [f"# {tool.annotation or 'RegEx'} (Tool {tid}) — Mode: {mode}"]

        if mode == "Replace":
            lines.append(
                f'df_{tid} = {input_df}.withColumn("{field}", '
                f'F.regexp_replace(F.col("{field}"), r"{pattern}", "{replace}"))'
            )
        elif mode == "Parse":
            lines.append(f"df_{tid} = {input_df}")
            for i, of in enumerate(output_fields):
                lines.append(
                    f'df_{tid} = df_{tid}.withColumn("{of}", '
                    f'F.regexp_extract(F.col("{field}"), r"{pattern}", {i + 1}))'
                )
            if not output_fields:
                lines.append(
                    f'df_{tid} = {input_df}.withColumn("{field}_parsed", '
                    f'F.regexp_extract(F.col("{field}"), r"{pattern}", 1))'
                )
        elif mode == "Match":
            lines.append(
                f'df_{tid} = {input_df}.filter(F.col("{field}").rlike(r"{pattern}"))'
            )
        elif mode == "Tokenize":
            lines.append(
                f'df_{tid} = {input_df}.withColumn("{field}", '
                f'F.explode(F.split(F.col("{field}"), r"{pattern}")))'
            )
        else:
            lines.append(f"df_{tid} = {input_df}  # Unknown RegEx mode: {mode}")

        code = "\n".join(lines)
        return GeneratedStep(
            step_name=f"regex_{tid}", code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df], output_df=f"df_{tid}", confidence=0.9,
        )
register_type_handler("RegEx", RegExHandler)
```

```python
# handlers/text_to_columns.py
class TextToColumnsHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        field = tool.config.get("ttc_field", "")
        delimiter = tool.config.get("ttc_delimiter", ",")
        num_cols = tool.config.get("ttc_num_columns", 2)
        to_rows = tool.config.get("ttc_split_to_rows", False)
        root = tool.config.get("ttc_output_root", field)

        lines = [f"# {tool.annotation or 'Text To Columns'} (Tool {tid})"]

        if to_rows:
            lines.append(
                f'df_{tid} = {input_df}.withColumn("{root}", '
                f'F.explode(F.split(F.col("{field}"), "{delimiter}")))'
            )
        else:
            lines.append(f'_split_{tid} = F.split(F.col("{field}"), "{delimiter}")')
            lines.append(f"df_{tid} = {input_df}")
            for i in range(num_cols):
                col_name = f"{root}{i + 1}" if root else f"{field}_{i + 1}"
                lines.append(
                    f'df_{tid} = df_{tid}.withColumn("{col_name}", _split_{tid}.getItem({i}))'
                )

        code = "\n".join(lines)
        return GeneratedStep(
            step_name=f"text_to_columns_{tid}", code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df], output_df=f"df_{tid}", confidence=1.0,
        )
register_type_handler("TextToColumns", TextToColumnsHandler)
```

```python
# handlers/date_time.py
# Alteryx DateTime format strings use C-style (%Y-%m-%d), Spark uses Java-style (yyyy-MM-dd)
_DT_FORMAT_MAP = {
    "%Y": "yyyy", "%y": "yy", "%m": "MM", "%d": "dd",
    "%H": "HH", "%M": "mm", "%S": "ss", "%p": "a",
    "%B": "MMMM", "%b": "MMM", "%A": "EEEE", "%a": "EEE",
}

def _convert_date_format(alteryx_fmt):
    result = alteryx_fmt
    for ayx, spark in _DT_FORMAT_MAP.items():
        result = result.replace(ayx, spark)
    return result

class DateTimeHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        field = tool.config.get("dt_field", "")
        fmt_in = tool.config.get("dt_format_in", "")
        fmt_out = tool.config.get("dt_format_out", "")
        conversion = tool.config.get("dt_conversion", "")

        spark_fmt_in = _convert_date_format(fmt_in) if fmt_in else "yyyy-MM-dd"
        spark_fmt_out = _convert_date_format(fmt_out) if fmt_out else "yyyy-MM-dd"

        lines = [f"# {tool.annotation or 'DateTime'} (Tool {tid})"]

        if conversion == "DateTimeToString" or (fmt_out and not fmt_in):
            lines.append(
                f'df_{tid} = {input_df}.withColumn("{field}", '
                f'F.date_format(F.col("{field}"), "{spark_fmt_out}"))'
            )
        elif conversion == "StringToDateTime" or (fmt_in and not fmt_out):
            lines.append(
                f'df_{tid} = {input_df}.withColumn("{field}", '
                f'F.to_timestamp(F.col("{field}"), "{spark_fmt_in}"))'
            )
        else:
            # Convert via intermediate timestamp
            lines.append(
                f'df_{tid} = {input_df}.withColumn("{field}", '
                f'F.date_format(F.to_timestamp(F.col("{field}"), "{spark_fmt_in}"), "{spark_fmt_out}"))'
            )

        code = "\n".join(lines)
        return GeneratedStep(
            step_name=f"datetime_{tid}", code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df], output_df=f"df_{tid}", confidence=0.9,
            notes=["Verify date format conversion — Alteryx uses C-style, Spark uses Java-style"],
        )
register_type_handler("DateTime", DateTimeHandler)
```

**Step 3: Update __init__.py, tests, run, commit**

```bash
git commit -m "feat: add RegEx, TextToColumns, DateTime handlers"
```

---

### Task 6: Advanced Formula Handlers — MultiRowFormula, MultiFieldFormula

**Files:**
- Create: `src/alteryx2dbx/handlers/multi_row_formula.py`
- Create: `src/alteryx2dbx/handlers/multi_field_formula.py`
- Modify: `src/alteryx2dbx/parser/xml_parser.py`
- Create: `tests/test_handlers/test_advanced_formula_handlers.py`

**Step 1: Parser extraction**

```python
elif tool_type == "MultiRowFormula":
    _extract_multi_row_formula_config(config_el, config)
elif tool_type == "MultiFieldFormula":
    _extract_multi_field_formula_config(config_el, config)
```

```python
def _extract_multi_row_formula_config(config_el, config):
    config["mrf_expression"] = config_el.findtext(".//Expression", "")
    config["mrf_field"] = config_el.findtext(".//Field", "")
    config["mrf_type"] = config_el.findtext(".//FieldType", "V_WString")
    group_fields = [f.get("field", "") for f in config_el.findall(".//GroupByFields/Field")]
    config["mrf_group_fields"] = group_fields
    config["mrf_num_rows"] = config_el.findtext(".//NumRows", "1")

def _extract_multi_field_formula_config(config_el, config):
    config["mff_expression"] = config_el.findtext(".//Expression", "")
    config["mff_fields"] = [f.get("field", "") for f in config_el.findall(".//Fields/Field")]
    config["mff_change_type"] = config_el.findtext(".//ChangeFieldType", "")
```

**Step 2: Write handlers**

```python
# handlers/multi_row_formula.py
class MultiRowFormulaHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        expression = tool.config.get("mrf_expression", "")
        field = tool.config.get("mrf_field", "")
        group_fields = tool.config.get("mrf_group_fields", [])

        # Build window spec
        if group_fields:
            partition = ", ".join(f'"{g}"' for g in group_fields)
            window = f"Window.partitionBy({partition}).orderBy(F.monotonically_increasing_id())"
        else:
            window = "Window.orderBy(F.monotonically_increasing_id())"

        # Try to transpile the expression
        try:
            from alteryx2dbx.transpiler.expression_emitter import transpile_expression
            pyspark_expr = transpile_expression(expression)
            # Replace F.lag references with windowed versions
            pyspark_expr = pyspark_expr.replace("F.lag(", f"F.lag(")
            confidence = 0.8
            notes = ["Multi-Row Formula — verify window ordering matches Alteryx row order"]
        except Exception as e:
            pyspark_expr = f"F.lit(None)  # FAILED: {expression}"
            confidence = 0.3
            notes = [f"Multi-Row Formula transpilation failed: {e}"]

        code = (
            f'# {tool.annotation or "Multi-Row Formula"} (Tool {tid})\n'
            f'# Alteryx expression: {expression}\n'
            f'_window_{tid} = {window}\n'
            f'df_{tid} = {input_df}.withColumn("{field}", {pyspark_expr})'
        )
        return GeneratedStep(
            step_name=f"multi_row_formula_{tid}", code=code,
            imports={"from pyspark.sql import functions as F", "from pyspark.sql.window import Window"},
            input_dfs=[input_df], output_df=f"df_{tid}",
            confidence=confidence, notes=notes,
        )
register_type_handler("MultiRowFormula", MultiRowFormulaHandler)
```

```python
# handlers/multi_field_formula.py
class MultiFieldFormulaHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        expression = tool.config.get("mff_expression", "")
        fields = tool.config.get("mff_fields", [])

        lines = [
            f"# {tool.annotation or 'Multi-Field Formula'} (Tool {tid})",
            f"# Alteryx expression: {expression}",
            f"df_{tid} = {input_df}",
        ]

        from alteryx2dbx.transpiler.expression_emitter import transpile_expression
        all_notes = []
        min_conf = 1.0

        for field in fields:
            # Replace _CurrentField_ with actual field reference
            field_expr = expression.replace("_CurrentField_", f"[{field}]")
            field_expr = field_expr.replace("_CurrentFieldName_", f'"{field}"')
            try:
                pyspark_expr = transpile_expression(field_expr)
                lines.append(f'df_{tid} = df_{tid}.withColumn("{field}", {pyspark_expr})')
            except Exception as e:
                lines.append(f'# FAILED for {field}: {field_expr}')
                all_notes.append(f"Failed for {field}: {e}")
                min_conf = min(min_conf, 0.3)

        code = "\n".join(lines)
        return GeneratedStep(
            step_name=f"multi_field_formula_{tid}", code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df], output_df=f"df_{tid}",
            confidence=min_conf, notes=all_notes,
        )
register_type_handler("MultiFieldFormula", MultiFieldFormulaHandler)
```

**Step 3: Tests, run, commit**

```bash
git commit -m "feat: add MultiRowFormula, MultiFieldFormula handlers"
```

---

### Task 7: Expand Expression Grammar — IN, Switch, 40+ functions

**Files:**
- Modify: `src/alteryx2dbx/transpiler/grammar.lark`
- Modify: `src/alteryx2dbx/transpiler/expression_emitter.py`
- Create: `tests/test_transpiler_expanded.py`

**Step 1: Add IN expression to grammar**

Add between `not_expr` and `comparison`:

```lark
?comparison: add_expr
    | add_expr IN "(" func_args ")"   -> in_expr
    | add_expr EQ add_expr            -> eq
    ...
```

Add terminal:
```lark
IN: /[Ii][Nn]/
```

**Step 2: Add IN emitter**

```python
def in_expr(self, children):
    # children: value, IN_token, args_list
    value = children[0]
    args = children[2] if len(children) > 2 else children[1]
    if isinstance(args, list):
        values = ", ".join(args)
    else:
        values = str(args)
    return f"{value}.isin({values})"
```

**Step 3: Add Switch function to emitter**

```python
if func_lower == "switch":
    # Switch(field, default, val1, result1, val2, result2, ...)
    field = args[0]
    default = args[1]
    pairs = list(zip(args[2::2], args[3::2]))
    result = ""
    for val, res in pairs:
        if result:
            result += f".when({field} == {val}, {res})"
        else:
            result = f"F.when({field} == {val}, {res})"
    result += f".otherwise({default})"
    return result
```

**Step 4: Add 40+ new function mappings**

Add to `_DIRECT_MAP` and special cases in `func_call`:

**DateTime functions:**
```python
# Special cases
if func_lower == "datetimeadd":
    # DateTimeAdd(dt, interval, unit) — unit can be "day", "month", "year", "hour", etc.
    dt, interval = args[0], args[1]
    unit = self._extract_string_val(args[2]).lower() if len(args) > 2 else "day"
    if unit in ("day", "days"):
        return f"F.date_add({dt}, {interval})"
    elif unit in ("month", "months"):
        return f"F.add_months({dt}, {interval})"
    elif unit in ("year", "years"):
        return f"F.add_months({dt}, {interval} * 12)"
    elif unit in ("hour", "hours"):
        return f"({dt} + F.expr(f'INTERVAL ' + str({interval}) + ' HOURS'))"
    return f"F.date_add({dt}, {interval})  # TODO: verify unit={unit}"

if func_lower == "datetimediff":
    return f"F.datediff({args[0]}, {args[1]})"

if func_lower == "datetimeformat":
    # IMPORTANT: format string must NOT be wrapped in F.lit()
    fmt = self._extract_string_val(args[1]) if self._is_string_lit(args[1]) else args[1]
    spark_fmt = self._convert_dt_format(fmt)
    return f'F.date_format({args[0]}, "{spark_fmt}")'

if func_lower == "datetimeparse":
    fmt = self._extract_string_val(args[1]) if self._is_string_lit(args[1]) else args[1]
    spark_fmt = self._convert_dt_format(fmt)
    return f'F.to_timestamp({args[0]}, "{spark_fmt}")'

if func_lower == "datetimetrim":
    return f"F.date_trunc({args[1]}, {args[0]})"

if func_lower in ("datetimeyear", "year"):
    return f"F.year({args[0]})"
if func_lower in ("datetimemonth", "month"):
    return f"F.month({args[0]})"
if func_lower in ("datetimeday", "day"):
    return f"F.dayofmonth({args[0]})"
if func_lower in ("datetimehour", "hour"):
    return f"F.hour({args[0]})"
if func_lower in ("datetimeminutes", "minute"):
    return f"F.minute({args[0]})"
if func_lower in ("datetimeseconds", "second"):
    return f"F.second({args[0]})"
if func_lower in ("datetimedayofweek", "dayofweek"):
    return f"F.dayofweek({args[0]})"
if func_lower == "datetimetoday":
    return "F.current_date()"
if func_lower == "datetimefirstofmonth":
    return f"F.date_trunc('month', {args[0]})"
```

**Add to `_DIRECT_MAP`:**
```python
# String
"titlecase": "F.initcap",
"replace": "F.regexp_replace",  # Note: Replace(str, find, repl) maps cleanly
"reversestring": "F.reverse",
"concat": "F.concat",

# Math
"log": "F.log",
"log10": "F.log10",
"log2": "F.log2",
"exp": "F.exp",
"sin": "F.sin",
"cos": "F.cos",
"tan": "F.tan",
"asin": "F.asin",
"acos": "F.acos",
"atan": "F.atan",
"atan2": "F.atan2",
"rand": "F.rand",
```

**Add special cases:**
```python
if func_lower == "tointeger":
    return f'{args[0]}.cast("int")'
if func_lower == "todate":
    return f'{args[0]}.cast("date")'
if func_lower == "todatetime":
    return f'{args[0]}.cast("timestamp")'
if func_lower == "coalesce":
    args_str = ", ".join(args)
    return f"F.coalesce({args_str})"
if func_lower == "ifnull":
    return f"F.coalesce({args[0]}, {args[1]})"
if func_lower == "nullif":
    return f"F.when({args[0]} == {args[1]}, F.lit(None)).otherwise({args[0]})"
if func_lower == "isnumber":
    return f'{args[0]}.cast("double").isNotNull()'
if func_lower == "countwords":
    return f'F.size(F.split(F.trim({args[0]}), "\\\\s+"))'
if func_lower == "mid":
    # Mid(str, start, length) — Alteryx is 0-based
    start = args[1]
    m = re.match(r"F\.lit\((\d+)\)", start)
    if m:
        start_val = int(m.group(1)) + 1
        return f"F.substring({args[0]}, {start_val}, {args[2]})"
    return f"F.substring({args[0]}, ({start} + F.lit(1)), {args[2]})"
```

**Add `_convert_dt_format` helper to the emitter class:**
```python
_DT_FMT_MAP = {
    "%Y": "yyyy", "%y": "yy", "%m": "MM", "%d": "dd",
    "%H": "HH", "%M": "mm", "%S": "ss", "%p": "a",
    "%B": "MMMM", "%b": "MMM", "%A": "EEEE", "%a": "EEE",
}

def _convert_dt_format(self, fmt):
    result = fmt
    for ayx, spark in self._DT_FMT_MAP.items():
        result = result.replace(ayx, spark)
    return result
```

**Step 5: Write comprehensive tests**

Test IN expression parsing and emission, Switch function, all new DateTime functions, new string functions, new math functions, conversion functions. At minimum 30 new tests.

**Step 6: Run all tests, commit**

```bash
git commit -m "feat: expand expression grammar with IN, Switch, 40+ new functions"
```

---

### Task 8: Update DAG for New Anchor Types

**Files:**
- Modify: `src/alteryx2dbx/generator/notebook.py`
- Create: `tests/test_anchor_routing.py`

**Step 1: Extend `_build_input_map` for all dual-input/output tools**

```python
# In the dual-input ordering section:
if tool and tool.tool_type in ("Join", "FindReplace", "AppendFields") and len(inputs) >= 2:
    left_anchors = ("left", "find", "targets", "target", "t", "f", "#1")
    right_anchors = ("right", "replace", "source", "s", "r", "#2")
    left_dfs = [df for df, anchor in inputs if anchor.lower() in left_anchors]
    right_dfs = [df for df, anchor in inputs if anchor.lower() in right_anchors]
    other_dfs = [df for df, anchor in inputs
                 if anchor.lower() not in left_anchors and anchor.lower() not in right_anchors]
    input_map[tool_id] = left_dfs + right_dfs + other_dfs
```

**Step 2: Handle Unique dual-output (Unique/Duplicates anchors)**

In `_build_input_map`, extend source anchor handling:
```python
if source_anchor.lower() in ("unique", "u"):
    df_name = f"df_{conn.source_tool_id}_unique"
elif source_anchor.lower() in ("duplicates", "d"):
    df_name = f"df_{conn.source_tool_id}_duplicates"
elif source_anchor.lower() in ("join", "j"):
    df_name = f"df_{conn.source_tool_id}_joined"
```

**Step 3: Tests, commit**

```bash
git commit -m "feat: extend DAG routing for all anchor types"
```

---

## Phase 2: Production Quality Optimizations

### Task 9: Fan-out Cache + Syntax Validation

**Files:**
- Modify: `src/alteryx2dbx/generator/notebook.py`
- Create: `tests/test_optimizations.py`

**Step 1: Detect fan-out nodes and insert `.cache()`**

After building steps, before writing notebooks:

```python
def _insert_cache_hints(workflow, steps):
    """Insert .cache() for DataFrames used by 2+ downstream tools."""
    usage_count = {}
    for conn in workflow.connections:
        anchor = conn.source_anchor
        if anchor.lower() in ("true", "false"):
            df_name = f"df_{conn.source_tool_id}_{anchor.lower()}"
        else:
            df_name = f"df_{conn.source_tool_id}"
        usage_count[df_name] = usage_count.get(df_name, 0) + 1

    for df_name, count in usage_count.items():
        if count >= 2:
            # Find the step that produces this df and append .cache()
            tool_id = int(df_name.split("_")[1])
            if tool_id in steps:
                step = steps[tool_id]
                step.code += f"\n{df_name}.cache()  # Used by {count} downstream tools"
```

**Step 2: Add syntax validation on generated .py files**

```python
def _validate_syntax(path):
    """Validate generated Python file has no syntax errors."""
    code = path.read_text(encoding="utf-8")
    try:
        compile(code, str(path), "exec")
        return True
    except SyntaxError as e:
        return False
```

Call after writing each notebook in `generate_notebooks`.

**Step 3: Tests, commit**

```bash
git commit -m "feat: add fan-out cache hints and syntax validation"
```

---

### Task 10: Disabled Node Detection + Network Path Warnings

**Files:**
- Modify: `src/alteryx2dbx/parser/xml_parser.py`
- Modify: `src/alteryx2dbx/generator/notebook.py`
- Create: `tests/test_disabled_nodes.py`

**Step 1: Detect disabled nodes in parser**

```python
# In _parse_tools, check for Disabled attribute
disabled = node.find(".//Properties/Disabled")
if disabled is not None and disabled.get("value", "False") == "True":
    continue  # Skip disabled tools
```

Also check for ToolContainer with Disabled flag.

**Step 2: Add network path warnings in generator**

When writing code that contains `\\` paths:
```python
if "\\\\" in step.code or "\\\\server" in step.code.lower():
    step.notes.append("Network path detected — update to Databricks-accessible location")
```

**Step 3: Tests, commit**

```bash
git commit -m "feat: skip disabled nodes, warn on network paths"
```

---

### Task 11: TextInput + Browse + DynamicInput handlers

**Files:**
- Create: `src/alteryx2dbx/handlers/text_input.py`
- Create: `src/alteryx2dbx/handlers/browse.py`
- Create: `src/alteryx2dbx/handlers/dynamic_input.py`
- Create: `tests/test_handlers/test_io_handlers.py`

TextInput: inline data embedded in XML → generate `spark.createDataFrame()`.
Browse: passthrough (output viewer only).
DynamicInput: similar to InputData but with wildcard/glob paths.

**Step 1: Write handlers, tests, commit**

```bash
git commit -m "feat: add TextInput, Browse, DynamicInput handlers"
```

---

## Phase 3: Competitive Features

### Task 12: Batch Mode with Summary Report

**Files:**
- Modify: `src/alteryx2dbx/cli.py`
- Create: `src/alteryx2dbx/generator/batch_report.py`
- Create: `tests/test_batch.py`

Add `--report` flag to `convert` command that generates an aggregate HTML/markdown report:

```bash
alteryx2dbx convert ./workflows/ -o ./output/ --report
```

Generates `output/batch_report.md`:
```markdown
# Batch Conversion Report
- Workflows converted: 20
- Average coverage: 92%
- Total tools: 450
- Unsupported tools: 35

## Per-Workflow Summary
| Workflow | Tools | Coverage | Unsupported |
|----------|-------|----------|-------------|
```

**Step 1: Write batch_report.py, update CLI, tests, commit**

```bash
git commit -m "feat: add batch mode with aggregate conversion report"
```

---

### Task 13: README Update + Final Polish

**Files:**
- Modify: `README.md`
- Modify: `pyproject.toml` (version bump to 0.2.0)

Update README with:
- New supported tools table (30+ tools)
- New expression function count (80+)
- Updated coverage estimate (95%)
- Batch mode documentation

```bash
git commit -m "docs: update README with Phase 1-3 features, bump to v0.2.0"
```

---

## Execution Order Summary

| Task | Phase | What it builds | New handlers |
|------|-------|---------------|-------------|
| 1 | P1 | Easy handlers batch | +6 (Sample, Unique, RecordID, AutoField, CountRecords, AppendFields) |
| 2 | P1 | Data Cleansing | +1 |
| 3 | P1 | Find Replace | +1 |
| 4 | P1 | Transform handlers | +5 (CrossTab, Transpose, RunningTotal, GenerateRows, Tile) |
| 5 | P1 | Parse handlers | +3 (RegEx, TextToColumns, DateTime) |
| 6 | P1 | Advanced formulas | +2 (MultiRowFormula, MultiFieldFormula) |
| 7 | P1 | Expression expansion | IN, Switch, 40+ functions |
| 8 | P1 | DAG anchor routing | Unique/Duplicates, Join/J, FindReplace anchors |
| 9 | P2 | Cache + validation | Fan-out cache, syntax check |
| 10 | P2 | Disabled nodes + warnings | Skip disabled, network path warns |
| 11 | P2 | IO handlers | +3 (TextInput, Browse, DynamicInput) |
| 12 | P3 | Batch mode | Aggregate report |
| 13 | P3 | README + version bump | Documentation |

**After all tasks: 30+ tool types, 80+ expression functions, production optimizations, batch mode.**
