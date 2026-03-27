# alteryx2dbx

Convert Alteryx `.yxmd` workflows to PySpark Databricks notebooks -- deterministically, without LLMs.

## The Problem

Migrating Alteryx workflows to Databricks is a common enterprise need, but it is almost always done manually. Teams walk through each workflow, read the XML, and rewrite the logic in PySpark by hand. Some attempt to use LLMs for the translation, but LLMs hallucinate PySpark APIs, invent nonexistent functions, and silently change semantics (especially around null handling, case sensitivity, and string indexing).

`alteryx2dbx` parses the Alteryx XML directly, resolves the DAG, and emits deterministic PySpark through a rule-based transpiler. Every expression mapping is explicit. When something cannot be converted, it is marked as unsupported with the original XML preserved in comments -- nothing is silently dropped.

## Quick Start

```bash
git clone https://github.com/aggarwalkartik/alteryx2dbx.git
cd alteryx2dbx
pip install -e ".[dev]"
```

Requires Python 3.11+.

## Usage

### Convert a single workflow

```bash
alteryx2dbx convert workflow.yxmd -o ./output
```

### Convert a directory of workflows (batch)

```bash
alteryx2dbx convert ./workflows/ -o ./output
```

This recursively finds all `.yxmd` files and converts each one into its own output folder.

### Analyze a workflow (dry run)

```bash
alteryx2dbx analyze workflow.yxmd
```

Prints every tool in execution order with its support status and coverage percentage. No files are written.

```
Workflow: simple_filter
Tools: 3
  [OK] [1] DbFileInput: Input Customers
  [OK] [2] Filter: Filter Active High Rev
  [OK] [3] DbFileOutput: Output Active Customers
Coverage: 3/3 (100%)
```

### List supported tools

```bash
alteryx2dbx tools
```

## Output Structure

Each converted workflow produces a self-contained folder:

```
output/<workflow_name>/
    01_load_sources.py        # Databricks notebook: reads source data
    02_transformations.py     # Databricks notebook: filters, joins, formulas, etc.
    03_orchestrate.py         # Databricks notebook: runs 01 + 02, writes outputs
    04_validate.py            # Databricks notebook: DataComPy comparison boilerplate
    config.yml                # Workflow metadata, source/output paths
    conversion_report.md      # Per-tool confidence scores and review notes
```

| File | Purpose |
|------|---------|
| `01_load_sources.py` | One cell per input tool. Reads CSV, Excel, or Parquet into DataFrames. |
| `02_transformations.py` | One cell per transformation tool in topological order. |
| `03_orchestrate.py` | Runs the load and transformation notebooks via `dbutils.notebook.run`, then writes outputs. |
| `04_validate.py` | DataComPy scaffold for comparing Alteryx output against Databricks output. |
| `config.yml` | YAML with workflow name, version, source paths, and output paths. |
| `conversion_report.md` | Confidence score per tool, average confidence, and count of tools needing manual review. |

## Supported Tools

| Alteryx Tool | PySpark Equivalent | Handler |
|--------------|--------------------|---------|
| Input Data / DbFileInput | `spark.read.format(...)` (CSV, Excel, Parquet) | `InputDataHandler` |
| Output Data / DbFileOutput | `df.write.format(...)` | `OutputDataHandler` |
| Filter | `df.filter(expr)` with True/False branch outputs | `FilterHandler` |
| Formula | `df.withColumn(name, expr)` per formula field | `FormulaHandler` |
| Select / AlteryxSelect | `df.select(...)`, `df.drop(...)`, `df.withColumnRenamed(...)` | `SelectHandler` |
| Join | `df.join(other, cond, type)` with joined/left-only/right-only outputs | `JoinHandler` |
| Sort | `df.orderBy(...)` | `SortHandler` |
| Summarize | `df.groupBy(...).agg(...)` with Sum, Count, Avg, Min, Max, First, Last, CountDistinct, Concat | `SummarizeHandler` |
| Union | `df.unionByName(other, allowMissingColumns=True)` | `UnionHandler` |

Unsupported tools are passed through with a `TODO` comment and the original XML config preserved for manual conversion.

## Expression Transpiler

Alteryx formula expressions are parsed with a Lark grammar and transpiled to PySpark column expressions. The transpiler handles:

- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **Comparisons**: `=`, `!=`, `>`, `>=`, `<`, `<=`
- **Logical operators**: `AND`, `OR`, `NOT`
- **Conditionals**: `IF/ELSEIF/ELSE/ENDIF` and `IIF()`
- **Field references**: `[FieldName]` to `F.col("FieldName")`
- **Row references**: `[Row-1:Revenue]` to `F.lag(F.col("Revenue"), 1)`
- **30+ function mappings**: `Trim`, `Left`, `Right`, `Substring`, `Contains`, `FindString`, `ToString`, `ToNumber`, `IsNull`, `IsEmpty`, `RegEx_Match`, `RegEx_Replace`, `GetWord`, `DateTimeNow`, `Round`, `Ceil`, `Floor`, `Abs`, `Pow`, `Sqrt`, `PadLeft`, `PadRight`, and more

**Semantic corrections** applied automatically:

| Alteryx Behavior | PySpark Translation | Why |
|------------------|---------------------|-----|
| `=` is case-insensitive for strings | `F.lower(col) == F.lit("value".lower())` | Alteryx `=` ignores case; Spark `==` does not |
| `Substring(field, 0, 3)` is 0-based | `F.substring(col, 1, 3)` | Alteryx uses 0-based indexing; Spark uses 1-based |
| `FindString` returns 0-based position | `F.locate(...) - 1` | Preserves original index semantics |
| `=` with non-string values | `.eqNullSafe(...)` | Prevents silent null propagation |

## How It Works

```
.yxmd file
    |
    v
[1. XML Parser]          Parse Alteryx XML into typed dataclasses
    |                     (AlteryxWorkflow, AlteryxTool, AlteryxConnection)
    v
[2. DAG Resolver]         Build dependency graph with NetworkX,
    |                     topological sort for execution order
    v
[3. Handler Dispatch]     Match each tool to its registered handler
    |                     by plugin name or tool type
    v
[4. Expression Transpiler]  Lark grammar parses Alteryx expressions,
    |                       PySparkEmitter transforms the parse tree
    v
[5. Notebook Generator]   Jinja2-free code assembly into Databricks
                          notebook format (.py with # COMMAND ----------
                          cell separators), plus config.yml and report
```

## Limitations

The following are not yet supported:

- **Spatial tools** (Buffer, Spatial Match, Trade Area, etc.)
- **Iterative macros** and **batch macros**
- **Analytic apps** (interface tools, app-level parameters)
- **In-database tools** (In-DB Connect, In-DB Filter, etc.)
- **Predictive/R tools** (Linear Regression, Decision Tree, R Tool)
- **Calgary loader** and **detour/block-until-done** tools
- **Nested macros** (`.yxmc` files referenced from within workflows)
- **Dynamic input/output** tools
- **Cross-tab** and **Transpose** tools (planned)

Expressions using unsupported functions are emitted as `# TODO` comments with the original expression preserved.

## Contributing

### Adding a new tool handler

1. Create a new file in `src/alteryx2dbx/handlers/` (e.g., `crosstab.py`).
2. Subclass `ToolHandler` and implement the `convert` method:

```python
from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep
from .base import ToolHandler
from .registry import register_type_handler

class CrossTabHandler(ToolHandler):
    def convert(self, tool: AlteryxTool, input_df_names=None) -> GeneratedStep:
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        # Build PySpark code string
        code = f"df_{tool.tool_id} = {input_df}.groupBy(...).pivot(...)"
        return GeneratedStep(
            step_name=f"crosstab_{tool.tool_id}",
            code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df],
            output_df=f"df_{tool.tool_id}",
        )

register_type_handler("CrossTab", CrossTabHandler)
```

3. Import the module in `src/alteryx2dbx/handlers/__init__.py`:

```python
from . import crosstab
```

4. Add a test fixture (`.yxmd` XML) in `tests/fixtures/` and a corresponding test.

### Running tests

```bash
pytest --cov=alteryx2dbx
```

## License

MIT
