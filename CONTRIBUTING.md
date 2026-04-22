# Contributing

## Dev setup

```bash
git clone https://github.com/aggarwalkartik/alteryx2dbx.git
cd alteryx2dbx
uv sync                    # installs runtime + dev deps (pytest, pytest-cov)
uv run alteryx2dbx --help  # verify the CLI is on PATH inside the venv
uv run pytest              # smoke test
```

`uv sync` installs the `dev` dependency group by default. No `--all-extras` or `--group dev` needed. To include the optional Box or Confluence extras, use `uv sync --extra box --extra confluence`.

## Adding a new tool handler

1. Create a new file in `src/alteryx2dbx/handlers/` (e.g., `crosstab.py`).
2. Subclass `ToolHandler` and implement the `convert` method:

```python
from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep
from .base import ToolHandler
from .registry import register_type_handler

class CrossTabHandler(ToolHandler):
    def convert(self, tool: AlteryxTool, input_df_names=None) -> GeneratedStep:
        input_df = input_df_names[0] if input_df_names else "df_unknown"
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

## Adding a custom fix via plugin

Create a `plugins/` directory in your project root and add a `.py` file:

```python
# plugins/my_fixes.py
def register_fixes(register_fix):
    register_fix(
        fix_id="my_org_date_fix",
        description="Replace DATE_TRUNC with org-specific pattern",
        severity="warning",
        fn=_my_date_fix,
        phase="date-handling",
    )

def _my_date_fix(code: str, context: dict) -> tuple[str, bool]:
    if "DATE_TRUNC" in code:
        return code.replace("DATE_TRUNC", "MY_ORG_DATE_TRUNC"), True
    return code, False
```

The plugin is automatically discovered and applied during conversion. No config needed.

## Lessons learned

Track migration pitfalls and share them across your team:

```bash
uv run alteryx2dbx lessons add \
  --workflow customer_report \
  --symptom "Join produced duplicates" \
  --root-cause "Join keys were not unique" \
  --fix "Added dedup before join" \
  --category behavioral_difference

uv run alteryx2dbx lessons list                    # see all lessons
uv run alteryx2dbx lessons list --unpromoted       # see patterns not yet automated
uv run alteryx2dbx lessons promote <lesson-id>     # mark as encoded into tool rules
```

On Databricks, lessons are automatically stored in `/Workspace/Shared/alteryx2dbx/lessons.jsonl` so everyone in the workspace benefits. Locally, they live in the project directory as `lessons.jsonl`.

The tool also auto-captures lessons during every conversion: low-confidence tools, ambiguous patterns, and applied fixes are logged without any manual step.

## Running tests

```bash
uv run pytest --cov=alteryx2dbx
```

## Review checklist

After any change that affects generated notebooks, see [REVIEW_GUIDE.md](./REVIEW_GUIDE.md) for the behavioral-differences checklist — the most common sources of silent Alteryx-vs-Spark mismatches.
