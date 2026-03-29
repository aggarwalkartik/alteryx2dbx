# alteryx2dbx v2: `generate` Command Design

## Problem

The repo parses Alteryx `.yxmd` workflows into an internal representation and emits basic PySpark notebooks. But the real work — structuring notebooks, emitting SQL patterns, wiring config, writing validation, applying serverless fixes — is still done manually. The parser extracts everything needed (tool configs, connections, join keys, expressions). The generation logic is just applying known patterns deterministically.

## Design

### CLI Structure

```
alteryx2dbx parse   <source>       -o manifest.json     # .yxmd or .yxzp → JSON manifest
alteryx2dbx analyze <source>                             # existing dry-run (unchanged)
alteryx2dbx generate manifest.json -o ./output           # manifest → production notebooks
alteryx2dbx convert <source>       -o ./output --full    # shortcut: parse + generate
```

- `parse` handles both `.yxmd` and `.yxzp` (unzip, find primary workflow, parse)
- `generate` consumes the parser's IR serialized as JSON
- `convert` keeps backward compatibility; `--full` chains parse → generate
- Batch mode: directories with mixed `.yxmd`/`.yxzp` are supported

### Output Structure

```
output/{workflow_name}/
├── _config.py                  # Databricks notebook: catalog/schema/env via widgets, paths, params
├── _utils.py                   # Databricks notebook: logging, type casts, null-safe join, quality checks
├── 01_load_sources.py          # One cell per input tool, %run _config
├── 02_transformations.py       # Business logic in DAG order
├── 03_write_outputs.py         # Write results to target tables/paths
├── 04_validate.py              # Auto-generated: row counts, schema, aggregates, sample diff
├── 05_orchestrate.py           # %run chain: config → utils → load → transform → write
├── manifest.json               # Parsed IR for audit trail / re-generation
└── conversion_report.md        # Per-tool confidence, applied fixes, manual review flags
```

Key design decisions:
- `_config.py` replaces `config.yml` — runnable notebook, parameterized with `dbutils.widgets` for catalog/schema/env
- `_utils.py` emitted from a standard template (same every time)
- Outputs separated from orchestration (03 vs 05)
- Orchestrator is just `%run` calls in sequence, no logic
- Manifest included in output for reproducibility

### Serverless-Safe Code Generation

All generated code must run on both Databricks serverless and classic clusters.

**Removed patterns:**
- `.cache()` / `.persist()` — breaks on serverless
- RDD operations — not available on Spark Connect
- Global temp views — not supported
- DBFS path references — not available on serverless

**Replacement patterns:**
- Fan-out: `createOrReplaceTempView()` with unique session-scoped names (`_tmp_{workflow}_{tool_id}`)
- Cascading joins on small tables: Pandas round-trip (`toPandas()` → join → `createDataFrame()`)
- Self-joins: rewrite as window functions (`F.lag`, `F.lead`, `F.row_number`)
- Config: `dbutils.widgets.text("catalog", "dev")` — not hardcoded values
- Intermediate materialization: Delta temp tables where needed, not `.cache()`

### Semantic Fix Registry

SQLGlot-style dictionary of known Alteryx→PySpark semantic differences. Applied as transform passes after the expression transpiler emits PySpark code.

```python
FIXES = {
    "case_insensitive_join": {
        "pattern": "join on string equality",
        "fix": "wrap both sides in F.lower()",
        "severity": "silent_bug"
    },
    "null_safe_equality": {
        "pattern": "equality comparison with nullable column",
        "fix": "use .eqNullSafe() instead of ==",
        "severity": "silent_bug"
    },
    "numeric_cast": {
        "pattern": "Alteryx FixedDecimal field",
        "fix": "explicit .cast(DecimalType(p,s))",
        "severity": "data_loss"
    },
    "data_cleansing_upper": {
        "pattern": "DataCleansing with ModifyCase=Upper",
        "fix": "F.upper() on specified fields",
        "severity": "logic"
    },
    "dedup_direction": {
        "pattern": "Unique tool with sort dependency",
        "fix": "window + row_number with explicit ordering before dropDuplicates",
        "severity": "silent_bug"
    }
}
```

Each fix is reported in `conversion_report.md` per tool.

### .yxzp Support

Handled in the `parse` command:

1. Detect file extension — if `.yxzp`, unzip to temp directory
2. Find primary `.yxmd` file(s) at archive root
3. Resolve `.yxmc` macro paths against extracted root
4. Parse `.yxmd` through existing XML parser
5. Record bundled assets (data files) in manifest with packaged flag
6. Cleanup temp directory

Macro transpilation is out of scope — macros are flagged in the manifest as `"type": "macro_reference"` with path for manual handling.

### Expression Transpiler Wiring

The existing Lark parser + PySpark emitter (80+ functions) is currently disconnected from code generation. Every handler that encounters an Alteryx expression will route through the transpiler:

```
Alteryx expression → Lark parse → AST → PySpark emit → fix passes → final code string
```

Affected handlers:
- `FormulaHandler` — each formula_field.expression
- `FilterHandler` — filter expression
- `MultiRowFormulaHandler` — expression + automatic window function wrapping
- `JoinHandler` — computed join key expressions
- `SummarizeHandler` — aggregation expressions

### Validation Notebook

`04_validate.py` is auto-generated with real validation logic, not stubs.

**Key auto-detection (priority order):**
1. Join tool configs — join key fields from manifest
2. Unique tool configs — dedup fields
3. Heuristic — columns ending in `_id`, `_key`, `_pk`
4. Fallback — first column

**Generated sections:**
1. Row count assertion
2. Schema comparison (column names + types, with known type mappings)
3. Aggregate checks (sum/min/max on numeric columns, count distinct on keys)
4. Sample row diff via DataComPy on auto-detected keys

All generated as readable code — keys can be manually adjusted before running.

## References

- SQLGlot transpiler architecture — dialect-neutral AST with `TRANSFORMS` and `TYPE_MAPPING` dicts
- Databricks Labs Lakebridge/remorph — `transpile` command with pre-setup/transpilation/validation stages
- Databricks serverless limitations — no cache, no RDD, no global temp views, no DBFS
- dbt CLI — separates `parse` (offline) from `compile` (needs metadata) from `run` (materializes)
