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

### Batch mode with summary report

```bash
alteryx2dbx convert ./workflows/ -o ./output --report
```

When `--report` is passed, an aggregate `batch_report.md` is generated in the output directory with:
- Total workflow count, tool count, and overall coverage
- Per-workflow table sorted by confidence (lowest first)
- Unsupported tool list per workflow
- Error summary for any workflows that failed to convert

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
| `batch_report.md` | (With `--report`) Aggregate summary across all converted workflows. |

## Supported Tools (32 tool types)

| Alteryx Tool | PySpark Equivalent | Handler |
|--------------|--------------------|---------|
| InputData / DbFileInput | `spark.read.format(...)` (CSV, Excel, Parquet) | `InputDataHandler` |
| TextInput | `spark.createDataFrame(...)` from inline data | `TextInputHandler` |
| DynamicInput | `spark.read` with TODO for dynamic path resolution | `DynamicInputHandler` |
| OutputData / DbFileOutput | `df.write.format(...)` | `OutputDataHandler` |
| Browse / BrowseV2 | `display(df)` / `df.show()` | `BrowseHandler` |
| Filter | `df.filter(expr)` with True/False branch outputs | `FilterHandler` |
| Formula | `df.withColumn(name, expr)` per formula field | `FormulaHandler` |
| MultiFieldFormula | `df.withColumn()` applied across matching columns | `MultiFieldFormulaHandler` |
| MultiRowFormula | `F.lag()` / `F.lead()` window functions | `MultiRowFormulaHandler` |
| Select / AlteryxSelect | `df.select(...)`, `df.drop(...)`, `df.withColumnRenamed(...)` | `SelectHandler` |
| Join | `df.join(other, cond, type)` with joined/left-only/right-only outputs | `JoinHandler` |
| Union | `df.unionByName(other, allowMissingColumns=True)` | `UnionHandler` |
| AppendFields | `df.crossJoin(other)` | `AppendFieldsHandler` |
| FindReplace | `F.regexp_replace()` / `F.when().otherwise()` | `FindReplaceHandler` |
| Sort | `df.orderBy(...)` | `SortHandler` |
| Sample | `df.limit(n)` / `df.sample(fraction)` | `SampleHandler` |
| Unique | `df.dropDuplicates()` with unique/duplicate branch outputs | `UniqueHandler` |
| Summarize | `df.groupBy(...).agg(...)` (Sum, Count, Avg, Min, Max, First, Last, CountDistinct, Concat) | `SummarizeHandler` |
| CrossTab | `df.groupBy().pivot().agg()` | `CrossTabHandler` |
| Transpose | `F.stack()` unpivot | `TransposeHandler` |
| DataCleansing | `F.trim()`, `F.lower()`, null removal per column | `DataCleansingHandler` |
| RegEx | `F.regexp_extract()` / `F.regexp_replace()` (match, parse, replace modes) | `RegExHandler` |
| TextToColumns | `F.split()` with column expansion | `TextToColumnsHandler` |
| DateTime | `F.to_timestamp()` / `F.date_format()` with format conversion | `DateTimeHandler` |
| RecordID | `F.monotonically_increasing_id()` | `RecordIDHandler` |
| AutoField | Pass-through (Spark infers types) | `AutoFieldHandler` |
| CountRecords | `df.count()` into single-row DataFrame | `CountRecordsHandler` |
| RunningTotal | `F.sum().over(Window)` | `RunningTotalHandler` |
| GenerateRows | `spark.range()` with expression-based row generation | `GenerateRowsHandler` |
| Tile | `F.ntile()` / `F.percent_rank()` window functions | `TileHandler` |

Unsupported tools are passed through with a `TODO` comment and the original XML config preserved for manual conversion.

## Expression Transpiler (80+ functions)

Alteryx formula expressions are parsed with a Lark grammar and transpiled to PySpark column expressions. The transpiler handles:

- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **Comparisons**: `=`, `!=`, `>`, `>=`, `<`, `<=`
- **Logical operators**: `AND`, `OR`, `NOT`
- **Conditionals**: `IF/ELSEIF/ELSE/ENDIF` and `IIF()`
- **IN expressions**: `[Field] IN (1, 2, 3)` to `F.col("Field").isin(...)`
- **Switch**: `Switch([Field], default, val1, res1, ...)` to `F.when().when()...otherwise()`
- **Field references**: `[FieldName]` to `F.col("FieldName")`
- **Row references**: `[Row-1:Revenue]` to `F.lag(F.col("Revenue"), 1)`

### String functions
`Trim`, `TrimLeft`, `TrimRight`, `Length`, `Uppercase`, `Lowercase`, `TitleCase`, `ReverseString`, `Left`, `Right`, `Substring`, `Mid`, `Contains`, `StartsWith`, `EndsWith`, `FindString`, `GetWord`, `PadLeft`, `PadRight`, `Replace`, `ReplaceFirst`, `ReplaceChar`, `CountWords`

### Numeric functions
`Round`, `Ceil`, `Floor`, `Abs`, `Pow`, `Sqrt`, `Log`, `Log10`, `Log2`, `Exp`, `Min`, `Max`, `Sign`, `Rand`, `Sin`, `Cos`, `Tan`, `Asin`, `Acos`, `Atan`, `Atan2`

### Conversion functions
`ToString`, `ToNumber`, `ToInteger`, `ToDate`, `ToDateTime`

### Null handling
`IsNull`, `IsEmpty`, `Coalesce`, `IfNull`, `NullIf`

### Type testing
`IsNumber`, `IsInteger`

### RegEx functions
`RegEx_Match`, `RegEx_Replace`

### Date/Time functions
`DateTimeNow`, `DateTimeToday`, `DateTimeFormat`, `DateTimeParse`, `DateTimeAdd`, `DateTimeDiff`, `DateTimeYear`/`Year`, `DateTimeMonth`/`Month`, `DateTimeDay`/`Day`, `DateTimeHour`/`Hour`, `DateTimeMinutes`/`Minute`, `DateTimeSeconds`/`Second`, `DateTimeDayOfWeek`/`DayOfWeek`, `DateTimeFirstOfMonth`, `DateTimeTrim`

### Semantic corrections applied automatically

| Alteryx Behavior | PySpark Translation | Why |
|------------------|---------------------|-----|
| `=` is case-insensitive for strings | `F.lower(col) == F.lit("value".lower())` | Alteryx `=` ignores case; Spark `==` does not |
| `Substring(field, 0, 3)` is 0-based | `F.substring(col, 1, 3)` | Alteryx uses 0-based indexing; Spark uses 1-based |
| `FindString` returns 0-based position | `F.locate(...) - 1` | Preserves original index semantics |
| `=` with non-string values | `.eqNullSafe(...)` | Prevents silent null propagation |

## Features

- **Fan-out cache hints**: DataFrames used by 2+ downstream tools automatically get `.cache()` appended
- **Syntax validation**: All generated notebooks are compile-checked; syntax errors are logged as warnings
- **Disabled node detection**: Tools marked as disabled in Alteryx XML are flagged with network path warnings
- **Network path warnings**: UNC paths (`\\server\...`) in generated code are flagged for migration to DBFS/S3/ADLS
- **Batch mode with `--report`**: Aggregate conversion report across multiple workflows
- **Per-workflow conversion report**: Confidence scores per tool with review notes
- **Dual-output routing**: Filter (True/False), Unique (Unique/Duplicates), Join (Join/Left/Right) outputs routed correctly through the DAG

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
