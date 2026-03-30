# Review Guide — What to Check After Code Generation

The generated notebooks are a first draft. Alteryx and Spark have real behavioral differences that cause silent data mismatches. This guide covers what to watch for.

**The one rule**: always validate against the actual Alteryx output before going to production.

---

## Behavioral Differences

These cause most validation failures.

| Area | Alteryx Behavior | Spark Behavior | Fix |
|------|-----------------|----------------|-----|
| **NULL vs Empty String** | `CountDistinct` counts `""` as distinct; FindReplace defaults unmatched to `""` | `COUNT(DISTINCT)` ignores NULL; LEFT JOIN leaves unmatched as NULL | `COUNT(DISTINCT COALESCE(col, ''))` for Summarize; `COALESCE(col, '')` for FindReplace LEFT JOIN — but check per column (some FindReplace columns preserve NULL) |
| **COALESCE Type Resolution** | N/A | `COALESCE(BIGINT_expr, '')` resolves as BIGINT. View creation succeeds; crashes at collection time with `CAST_INVALID_INPUT` | Every `COALESCE(col, '')` where col might be non-STRING must be `COALESCE(CAST(col AS STRING), '')` |
| **Type Coercion in Joins** | Strict — `"123"` does NOT match `123` in FindReplace | Implicit — Spark silently casts `"123"` to `123` | Explicitly `CAST` both sides of join keys to the same type |
| **CAST Failures** | Returns NULL or 0 on invalid input | Throws `CAST_INVALID_INPUT` | Use `TRY_CAST` instead of `CAST` |
| **TO_DATE on empty strings** | Returns blank for unparseable dates | `TO_DATE('', fmt)` throws `CANNOT_PARSE_TIMESTAMP`. Wrapping in `TRY_CAST` does NOT help — the inner function throws first | Use `TRY_TO_DATE(col, fmt)` or `TRY_TO_TIMESTAMP(col, fmt)` |
| **Case Sensitivity** | `=` is case-insensitive for strings | `==` is case-sensitive | Wrap both sides in `LOWER()` or `F.lower()` for string comparisons |
| **String Indexing** | `Substring([field], 0, 3)` is 0-based | `SUBSTRING(col, 1, 3)` is 1-based | Add 1 to all Alteryx substring start positions |
| **NULL in WHERE** | `WHERE col != 'X'` includes NULLs | `WHERE col != 'X'` excludes NULLs | Use `WHERE col != 'X' OR col IS NULL` |
| **Window Functions** | `CountDistinct` works in Summarize by group | `COUNT(DISTINCT) OVER()` is NOT supported | Pre-aggregate with GROUP BY into temp view, then LEFT JOIN back |
| **Select tool Date truncation** | Select typed as "Date" truncates timestamps to midnight | Spark preserves full timestamp | Add `CAST(col AS DATE)` when Select tool types a column as "Date" |
| **FindReplace NULL vs empty** | Not all unmatched columns default to `""` — some preserve NULL | LEFT JOIN + COALESCE forces all to empty | Check actual Alteryx output per column before adding COALESCE |
| **Cleanse Macro UPPER()** | Config may show `upper`, but Cleanse often doesn't apply case conversion | Databricks applies `UPPER()` faithfully if coded | Verify against actual Alteryx output, not `.yxmd` config |
| **Date Formats** | C-style: `%d.%m.%Y` | Java-style: `dd.MM.yyyy` | Convert all Alteryx date format tokens |
| **Regex Syntax** | Perl-compatible | Java regex — double-escape `\\` in SQL strings | Convert `\d` to `\\d` |
| **Sort Stability** | Sort is a discrete tool with guaranteed order | Spark has no implicit sort; temp views are unordered | Always add explicit `ORDER BY` where order matters |
| **Empty String Default** | Many tools default empty/missing to `""` | Spark defaults to NULL | Use `COALESCE(col, '')` where Alteryx produces `""` |
| **Date Placeholders** | Converts `0001-01-01` to blank | Preserves `0001-01-01` from source databases; dates before 1900 crash Excel | Clamp dates: `IF(YEAR(col) < 1900, NULL, col)` |

### Serverless Compute

All materialization APIs are blocked on Databricks serverless:
- `.cache()` → `NOT_SUPPORTED_WITH_SERVERLESS`
- `.localCheckpoint()` → `NOT_IMPLEMENTED`
- `.write.parquet()` to workspace → `INSUFFICIENT_PERMISSIONS`
- `CACHE TABLE` → blocked

**Workaround for cascading patterns**: Pandas round-trip to break lineage:
```python
pdf = spark.table(view).toPandas()
spark.createDataFrame(pdf, schema).createOrReplaceTempView(view)
```
Only viable when data fits in driver memory. For single-output workflows, lazy evaluation is usually sufficient.

---

## Common Pitfalls

| Pitfall | What Happens | Prevention |
|---------|-------------|------------|
| `CAST` instead of `TRY_CAST` | Non-numeric values crash the pipeline | Always `TRY_CAST` for user-input fields |
| `COALESCE(BIGINT, '')` | View creates fine; crashes at data collection | Always `COALESCE(CAST(col AS STRING), '')` |
| `COUNT(DISTINCT) OVER()` | Spark doesn't support this | Pre-aggregate + LEFT JOIN |
| `TRY_CAST(TO_DATE(...))` | Inner `TO_DATE` throws before `TRY_CAST` catches it | Use `TRY_TO_DATE` directly |
| Excel float64 join keys | pandas reads numeric IDs as float64 → `CAST(AS STRING)` gives scientific notation (`"5.1E9"`) | Convert float64 → Int64 in pandas: check if all non-null values are whole numbers |
| `withColumn` loops | 80+ `withColumn` calls → deeply nested plan → 20+ min runtime | Single SQL `CREATE OR REPLACE TEMP VIEW` with all transformations |
| Mixed pandas dtypes | `ArrowInvalid` on DataFrame conversion | Cast object columns to str, replace nan/NaT/None with None |
| Implicit type coercion in joins | Spark joins string to int silently → extra matches | CAST both sides of every join key explicitly |
| Serverless — no cluster libraries | `ModuleNotFoundError` for openpyxl | `%pip install openpyxl xlsxwriter -q` at top of notebook |
| Output schema from config | Tool config has renames/placeholders not in actual output | Derive output columns from actual Alteryx output file |
| Cleanse macro misleading | `.yxmd` says `upper` but actual output retains mixed case | Verify against actual output, not config |
| MultiRowFormula direction | LEAD keeps last row's value; Alteryx keeps first | Use `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1` |
| COLUMN_ALREADY_EXISTS | `SELECT *` carries NULL columns from one pass into the next | Strip joined columns from unmatched views between passes |
| Summarize field mismatch | Assuming which field is counted when workflow counts a different one | Verify Summarize fields against workflow XML |
| Validation key uniqueness | Line-level keys used on detail-level data → many-to-many join inflates mismatches | Define validation keys per output granularity |
| Reference file version mismatch | Consistent diffs in lookup-derived columns — NOT a code bug | Freeze ALL input files at the same point in time for both systems |
| Database quoted column names | Column names with special characters have literal quotes as part of the name | Wrap with backticks including quotes: `` `"COL(SUFFIX)"` `` |

---

## Post-Generation Review Checklist

Run through this after every migration, before declaring it done:

- [ ] `TRY_CAST` used instead of `CAST` for user-input fields
- [ ] `TRY_TO_DATE` / `TRY_TO_TIMESTAMP` used instead of `TO_DATE` / `TO_TIMESTAMP`
- [ ] Every `COALESCE(col, '')` is `COALESCE(CAST(col AS STRING), '')` when col could be non-STRING
- [ ] Join keys explicitly CAST to same type on both sides
- [ ] `read_excel` converts float64 integer columns to Int64
- [ ] FindReplace unmatched columns checked individually — not all default to `""`
- [ ] Select tool "Date" typed fields wrapped in `CAST(col AS DATE)`
- [ ] Cleanse macro behavior verified against actual Alteryx output
- [ ] `COUNT(DISTINCT) OVER()` not used — pre-aggregate + LEFT JOIN instead
- [ ] Alteryx date formats converted (`%d.%m.%Y` → `dd.MM.yyyy`)
- [ ] No `withColumn` loops — use single SQL statements
- [ ] Output column list verified against actual Alteryx output file, not workflow config
- [ ] Database quoted column names preserved with backticks + quotes
- [ ] On serverless: `%pip install openpyxl xlsxwriter -q`
- [ ] Date placeholders clamped: `IF(YEAR(col) < 1900, NULL, col)`
- [ ] Summarize field references verified against workflow XML
- [ ] Cascading multi-pass joins strip joined columns from unmatched views
- [ ] Validation keys are unique per output granularity level

---

## Validation Strategy

### Minimum viable validation

1. **Row count** — must match exactly (or within tolerance if query timing differs)
2. **Column count and names** — must match exactly
3. **Row-level EXCEPT** — `alteryx_df.exceptAll(databricks_df)` both ways — shows exactly which rows differ
4. **Column-level comparison** — join on key columns, compare each column individually to pinpoint mismatches

### Freeze Input Files

The #1 cause of persistent validation failures that look like code bugs is **input file versioning**. If Alteryx reads files from a shared drive and you upload copies to Databricks at a different time, the data WILL differ.

**Symptoms** (NOT a code bug): row count matches exactly, all keys match, differences are ONLY in lookup-derived columns, numbers are deterministic across re-runs.

**Fix**: At a single point in time, copy ALL input files. Use those exact copies for both Alteryx and Databricks. Compare outputs. Remaining differences should now only be from code bugs.

---

## Quick Reference: Alteryx Expression → Spark SQL

| Alteryx | Spark SQL |
|---------|-----------|
| `if...then...elseif...else...endif` | `CASE WHEN...THEN...WHEN...ELSE...END` |
| `contains([col], "val")` | `col LIKE '%val%'` |
| `Left([col], n)` | `LEFT(col, n)` |
| `Right([col], n)` | `RIGHT(col, n)` |
| `IsEmpty([col])` | `col IS NULL OR col = ''` |
| `[col] in ("a", "b")` | `col IN ('a', 'b')` |
| `ToNumber([col])` | `TRY_CAST(col AS DOUBLE)` |
| `ToString([col])` | `CAST(col AS STRING)` |
| `DateTimeParse([col], fmt)` | `TRY_TO_DATE(col, spark_fmt)` |
| `DateTimeMonth([col])` | `MONTH(col)` |
| `DateTimeToday()` | `CURRENT_DATE()` |
| `DateTimeDiff(a, b, 'day')` | `DATEDIFF(a, b)` |
| `REGEX_Match([col], pattern)` | `col RLIKE 'pattern'` |
| `[f1] + [f2]` (string concat) | `CONCAT(f1, f2)` |
| `Substring([col], 0, 3)` | `SUBSTRING(col, 1, 3)` — note 0-based → 1-based |
| `FindString([col], "x")` | `LOCATE('x', col) - 1` — note: returns 0-based |

---

## Running on Databricks (without any IDE)

Install in a Databricks notebook:

```python
%pip install git+https://github.com/aggarwalkartik/alteryx2dbx.git -q
```

Then use the Python API:

```python
from pathlib import Path
from alteryx2dbx.parser.xml_parser import parse_yxmd
from alteryx2dbx.parser.unpacker import unpack_source
from alteryx2dbx.dag.resolver import resolve_dag
from alteryx2dbx.handlers.registry import get_handler
import alteryx2dbx.handlers  # triggers handler registration

# Parse a workflow
unpacked = unpack_source(Path("/Workspace/Users/your.email@company.com/workflow.yxmd"))
wf = parse_yxmd(unpacked.workflow_path)

# Analyze — see which tools are supported
order = resolve_dag(wf)
for tool_id in order:
    tool = wf.tools[tool_id]
    handler = get_handler(tool)
    supported = type(handler).__name__ != "UnsupportedHandler"
    print(f"[{'OK' if supported else 'UNSUPPORTED'}] {tool.tool_type}: {tool.annotation}")

# Generate notebooks
from alteryx2dbx.generator.notebook_v2 import generate_notebooks_v2
stats = generate_notebooks_v2(wf, Path("/Workspace/Users/your.email@company.com/output/"))
print(f"Done: {stats['tools_converted']}/{stats['tools_total']} tools converted")
```

Or from a terminal / shell cell:

```bash
alteryx2dbx analyze /Workspace/Users/your.email@company.com/workflow.yxmd
alteryx2dbx convert /Workspace/Users/your.email@company.com/workflow.yxmd -o ./output --full
```
