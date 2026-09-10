# alteryx2dbx — Alteryx to Databricks Migration Tool

**Date:** 2026-03-27
**Status:** Approved
**Type:** Open-source CLI tool (Python, pip-installable)
**Repo:** `alteryx2dbx`

## Purpose

A deterministic, rule-based tool that converts Alteryx Designer workflows (`.yxmd` files) into runnable PySpark Databricks notebooks. Built for real migration work (initially ~20 workflows, scaling to handle max complexity) and designed to be open-sourced.

The core problem: Cursor and LLMs hallucinate PySpark conversions. This tool produces predictable, reproducible, testable output — when it can't convert something, it says so honestly instead of guessing.

## Architecture

Five-stage pipeline, each stage independent and testable:

```
.yxmd file
    ↓
[1. XML Parser] → Internal Representation (dataclasses)
    ↓
[2. DAG Resolver] → Topological execution order
    ↓
[3. Expression Transpiler] → Alteryx formulas → PySpark expressions
    ↓
[4. Code Generator] → Jinja2 templates → .py notebook files
    ↓
[5. Validator Generator] → DataComPy validation script
```

## Project Structure

```
alteryx2dbx/
├── pyproject.toml
├── src/alteryx2dbx/
│   ├── __init__.py
│   ├── cli.py                    # Click CLI entry point
│   ├── parser/
│   │   ├── xml_parser.py         # .yxmd → IR dataclasses
│   │   └── models.py             # Tool, Connection, Workflow dataclasses
│   ├── dag/
│   │   └── resolver.py           # Topological sort, macro expansion
│   ├── transpiler/
│   │   ├── expression_parser.py  # Lark grammar → AST
│   │   ├── expression_emitter.py # AST → PySpark column expressions
│   │   └── grammar.lark          # Alteryx expression EBNF
│   ├── handlers/                  # One handler per Alteryx tool type
│   │   ├── base.py               # Abstract ToolHandler
│   │   ├── input_data.py
│   │   ├── output_data.py
│   │   ├── filter.py
│   │   ├── formula.py
│   │   ├── join.py
│   │   ├── select.py
│   │   ├── sort.py
│   │   ├── summarize.py
│   │   ├── union.py
│   │   ├── unique.py
│   │   ├── sample.py
│   │   ├── cross_tab.py
│   │   ├── transpose.py
│   │   ├── multi_row_formula.py
│   │   ├── multi_field_formula.py
│   │   ├── regex.py
│   │   ├── data_cleansing.py
│   │   ├── record_id.py
│   │   ├── generate_rows.py
│   │   ├── running_total.py
│   │   ├── text_to_columns.py
│   │   └── registry.py           # Maps Plugin strings → handler classes
│   ├── generator/
│   │   ├── notebook.py           # Assembles .py notebook files
│   │   ├── config.py             # Generates config.yml
│   │   ├── validator.py          # Generates 04_validate.py
│   │   └── templates/            # Jinja2 templates
│   └── utils/
│       ├── type_mapping.py       # Alteryx types → Spark types
│       └── null_semantics.py     # Null-safe wrappers
├── tests/
│   ├── fixtures/                  # Sample .yxmd files
│   ├── test_parser.py
│   ├── test_transpiler.py
│   ├── test_handlers/
│   └── test_generator.py
└── README.md
```

## Internal Representation (IR)

```python
@dataclass
class AlteryxField:
    name: str
    type: str          # V_WString, Int32, Double, etc.
    size: int | None
    scale: int | None

@dataclass
class AlteryxConnection:
    source_tool_id: int
    source_anchor: str   # "Output", "True", "False", "Left", "Right"
    target_tool_id: int
    target_anchor: str   # "Input", "Left", "Right"

@dataclass
class AlteryxTool:
    tool_id: int
    plugin: str          # e.g. "AlteryxBasePluginsGui.Filter.Filter"
    tool_type: str       # normalized: "Filter", "Formula", "Join", etc.
    config: dict         # tool-specific parsed config
    annotation: str      # user's label for this tool
    input_fields: list[AlteryxField]
    output_fields: list[AlteryxField]

@dataclass
class AlteryxWorkflow:
    name: str
    version: str
    tools: dict[int, AlteryxTool]
    connections: list[AlteryxConnection]
    properties: dict
```

Each tool handler returns a `GeneratedStep`:

```python
@dataclass
class GeneratedStep:
    step_name: str           # human-readable: "filter_active_customers"
    code: str                # PySpark code string
    imports: set[str]        # required imports
    input_dfs: list[str]     # variable names this step reads from
    output_df: str           # variable name this step produces
    notes: list[str]         # warnings, manual review flags
    confidence: float        # 0-1, how reliable the conversion is
```

## Tool Handler Pattern

Abstract base class per tool type. Registry maps Alteryx Plugin strings to handlers:

```python
class ToolHandler(ABC):
    @abstractmethod
    def can_handle(self, tool: AlteryxTool) -> bool: ...

    @abstractmethod
    def convert(self, tool: AlteryxTool, context: ConversionContext) -> GeneratedStep: ...

HANDLER_REGISTRY = {
    "AlteryxBasePluginsGui.Filter.Filter": FilterHandler,
    "AlteryxBasePluginsGui.Formula.Formula": FormulaHandler,
    "AlteryxBasePluginsGui.Join.Join": JoinHandler,
    # ...
}
```

Unsupported tools produce passthrough code with original XML preserved as comments and a `# TODO` marker.

### Tool Priority

| Tier | Tools |
|------|-------|
| Must-have (Phase 1) | Input, Output, Filter, Formula, Select, Join, Sort, Summarize, Union |
| Should-have (Phase 2) | Unique, Sample, RecordID, CrossTab, Transpose, DataCleansing, MultiFieldFormula |
| Nice-to-have (Phase 3) | MultiRowFormula, RegEx, GenerateRows, RunningTotal, TextToColumns, DateTime |

## Expression Transpiler

Lark EBNF grammar parses Alteryx expressions into an AST, which is walked by a PySpark emitter.

**Pipeline:** `expression string → Lark parser → parse tree → AST transformer → typed AST → PySpark emitter → PySpark expression string`

**AST node types:** IfExpr, FunctionCall, FieldRef, RowRef, BinaryOp, UnaryOp, Literal (String, Number, Bool, Null)

### Critical Semantic Fixes

| Alteryx behavior | PySpark fix |
|---|---|
| `[Field] = "abc"` is case-insensitive | Emit `F.lower(F.col("Field")) == "abc"` |
| `"abc" + NULL` = `"abc"` | Wrap in `F.coalesce(col, F.lit(""))` for string concat |
| `NULL == NULL` = `True` | Use `F.col().eqNullSafe()` |
| `Substring([F], 0, 3)` is 0-based | Emit `F.substring(col, 1, 3)` (offset +1) |
| `FindString([F], "x")` returns 0-based | Emit `F.locate("x", col) - 1` |
| Division always returns Double | Emit `.cast("double")` before division |

### Function Mapping (Top 30)

| Alteryx | PySpark |
|---|---|
| `Contains(s, t)` | `F.lower(col).contains(t.lower())` |
| `StartsWith(s, t)` | `F.col(s).startswith(t)` |
| `Trim(s)` / `TrimLeft` / `TrimRight` | `F.trim()` / `F.ltrim()` / `F.rtrim()` |
| `Length(s)` | `F.length()` |
| `ToString(x, fmt)` | `F.format_number()` or `.cast("string")` |
| `ToNumber(s)` | `.cast("double")` |
| `DateTimeParse(s, fmt)` | `F.to_timestamp(s, spark_fmt)` |
| `DateTimeFormat(d, fmt)` | `F.date_format(d, spark_fmt)` |
| `Round(x, n)` | `F.round(x, n)` |
| `Ceil(x)` / `Floor(x)` | `F.ceil()` / `F.floor()` |
| `IsNull(x)` | `F.col(x).isNull()` |
| `IsEmpty(x)` | `F.col(x) == F.lit("")` |
| `IIF(c, t, f)` | `F.when(c, t).otherwise(f)` |
| `Switch(f, d, v1,r1,...)` | Chained `F.when().when()...otherwise()` |
| `Min(a, b)` / `Max(a, b)` | `F.least()` / `F.greatest()` |
| `Abs(x)` | `F.abs()` |
| `Pow(x, n)` | `F.pow()` |
| `Regex_Replace(s, p, r)` | `F.regexp_replace()` |
| `Regex_Match(s, p)` | `F.col(s).rlike(p)` |
| `GetWord(s, n)` | `F.split(s, " ").getItem(n)` |
| `ReplaceChar(s, old, new)` | `F.translate()` |
| `Left(s, n)` / `Right(s, n)` | `F.substring(s, 1, n)` / `F.substring(s, -n, n)` |
| `Uppercase(s)` / `Lowercase(s)` | `F.upper()` / `F.lower()` |
| `PadLeft(s, n, c)` / `PadRight` | `F.lpad()` / `F.rpad()` |
| `a + b` (strings) | `F.concat(F.coalesce(a, F.lit("")), F.coalesce(b, F.lit("")))` |

## Output Structure

Per workflow:

```
output/workflow_name/
├── config.yml                # Data sources, paths, parameters
├── 01_load_sources.py        # Load all inputs (Databricks notebook format)
├── 02_transformations.py     # Business logic in DAG order, one cell per tool
├── 03_orchestrate.py         # Chain steps, write output
├── 04_validate.py            # DataComPy comparison vs Alteryx Excel output
├── conversion_report.md      # What converted, what needs review
└── alteryx_output/           # User drops original Alteryx .xlsx here
```

All `.py` files use Databricks notebook format (`# Databricks notebook source` header, `# COMMAND ----------` cell separators). Ready to copy-paste into Databricks and run.

### Validation (04_validate.py)

Uses DataComPy to compare Databricks output vs original Alteryx Excel output:
- Row count comparison
- Schema comparison (column names, types, order)
- Cell-by-cell comparison with tolerance (abs_tol=0.01, rel_tol=0.0001)
- Null normalization (empty strings, "NULL" strings, NaN → unified NA)
- Column name normalization (strip, lowercase)
- Human-readable diff report

## CLI Interface

```bash
# Single workflow
alteryx2dbx convert workflow.yxmd -o ./output/

# Batch — all .yxmd files in a directory
alteryx2dbx convert ./workflows/ -o ./output/

# Analyze only — no code generation, just conversion report
alteryx2dbx analyze workflow.yxmd

# List supported tools
alteryx2dbx tools
```

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Approach | Deterministic transpiler | LLMs hallucinate — the exact problem this solves |
| Language | Python | Same ecosystem as PySpark, natural for the audience |
| Expression parsing | Lark EBNF | Grammar is LL(1), Lark is fast and mature |
| Code generation | Jinja2 templates | Readable, maintainable, easy to customize |
| DAG resolution | networkx topological sort | Battle-tested, handles complex graphs |
| Validation | DataComPy | Capital One's tool, supports both pandas and PySpark |
| Output format | Databricks notebook .py | Copy-paste and run, no setup needed |
| Unsupported tools | Honest failure with context | Passthrough + TODO + original XML, never guesses |
| Confidence scoring | Per-tool 0-1 float | Conversion report shows reliability at a glance |
