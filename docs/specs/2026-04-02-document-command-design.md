# Document Command

## Context

The current tool converts workflows but produces minimal documentation (just `conversion_report.md` with confidence scores). Enterprise migrations need comprehensive documentation: what the workflow does, where data flows, what business logic is embedded, and who should review what.

The target user is an **analyst on Databricks**. They clone the repo, `%pip install` it, upload their .yxmd files, and run CLI commands in notebook cells. They should not need to touch a terminal or install anything locally.

## User Workflow

```python
# Databricks notebook — one-time setup
%pip install -e /Workspace/Repos/analyst/alteryx2dbx

# Document a single workflow
!alteryx2dbx document /Volumes/team/workflows/my_workflow.yxmd -o /Volumes/team/output/

# Document a directory of workflows
!alteryx2dbx document /Volumes/team/workflows/ -o /Volumes/team/output/
```

If `.alteryx2dbx.yml` exists with Confluence config and a valid PAT, the command also creates a **draft** page in Confluence. If no config or no PAT, it just writes local markdown and prints guidance on how to set up Confluence.

## CLI Interface

```
alteryx2dbx document <source> -o <output>
```

- `source`: path to .yxmd, .yxzp, directory, or manifest .json
- `-o / --output`: output directory (default `./output`)
- Accepts same source types as `convert` (single file, directory, manifest)

## Config File

`.alteryx2dbx.yml` — looked up by walking from the current working directory toward the root, stopping at the first match. On Databricks this means it lives in the cloned repo root (`/Workspace/Repos/analyst/alteryx2dbx/.alteryx2dbx.yml`). Can also be specified explicitly with `--config /path/to/.alteryx2dbx.yml`.

```yaml
confluence:
  url: https://company.atlassian.net
  space: DATA-MIGRATION
  parent_page: "Alteryx Migration Reports"
  pat: ""  # can also be set via CONFLUENCE_PAT env var or Databricks secret
```

**PAT resolution order:**
1. `confluence.pat` field in config file (for local dev — not recommended for shared repos)
2. `CONFLUENCE_PAT` environment variable
3. Databricks secret: `dbutils.secrets.get(scope, key)` — configured via optional fields in the yml:
   ```yaml
   confluence:
     secret_scope: "confluence"
     secret_key: "pat"
   ```

**Behavior matrix:**

| Config exists? | PAT available? | Behavior |
|---|---|---|
| No | - | Write local markdown only. Print: "Tip: create .alteryx2dbx.yml to enable Confluence publishing. See README." |
| Yes | No | Write local markdown. Print step-by-step guide to create a Confluence PAT. |
| Yes | Yes | Write local markdown + create Confluence **draft** (never publish directly). Print draft URL. |

## Per-Workflow Output: `migration_report.md`

### Sections

**1. Executive Summary**
- Workflow name, author, description (from MetaInfo XML)
- Conversion confidence (average across all tools)
- Readiness score: Ready (avg > 0.9) / Needs Review (0.7-0.9) / Significant Manual Work (< 0.7)
- Complexity: Simple (< 10 tools) / Medium (10-30) / Complex (30+)
- Tool count, supported count, unsupported count
- Date generated, alteryx2dbx version

**2. Data Flow Diagram**
- Mermaid.js flowchart generated from the DAG (networkx graph already exists)
- Node labels: `[tool_id] tool_type: annotation`
- Node colors: green = input tools, blue = output tools, yellow = transforms, red = unsupported
- Edge labels: anchor names for multi-output tools (True/False for Filter, Left/Right/Join for Join, Unique/Duplicates for Unique)
- Renders natively on GitHub, Obsidian, Confluence (with Mermaid plugin)

**3. Data Source Inventory**
Table with columns:
- Tool ID, Tool Type, Source Path/Box File, Format, Fields (count), Notes

Covers: DbFileInput, TextInput, DynamicInput, BoxInput

**4. Output Inventory**
Table with columns:
- Tool ID, Tool Type, Destination Path/Box Folder, Format, Notes

Covers: DbFileOutput, BoxOutput, Browse

**5. Business Logic Summary**
For each logic-carrying tool, a human-readable description:
- **Filters**: "Filters rows where {expression}" — with the raw Alteryx expression and the translated PySpark
- **Joins**: "Joins {left annotation} with {right annotation} on {fields}, type: {inner/left/right/full}"
- **Formulas**: "Creates/updates field {name} = {expression}"
- **Summarize**: "Groups by {fields}, aggregates: {SUM of X, COUNT of Y, ...}"
- **Select**: "Drops {N} fields, renames {N} fields"

Tools with confidence < 1.0 are flagged with a warning icon and notes.

**6. Conversion Details**
Same content as the current `conversion_report.md` — tool-by-tool table with confidence and notes. This section is the technical reference for the engineer doing the migration.

**7. Manual Review Checklist**
Auto-generated checklist of items that need human attention:
- Unsupported tools (confidence = 0.0)
- Low confidence tools (< 0.7)
- Network/UNC paths that need remapping
- Box tools (auth setup required)
- Encrypted credentials detected
- Expressions that fell back to TODO comments

### Markdown Format Rules (Confluence-Ready)
- Pure Markdown, no inline HTML
- Standard tables (pipe-delimited)
- Mermaid diagrams in fenced code blocks
- Structured headings (H1 = workflow name, H2 = sections)
- No frontmatter (Confluence doesn't understand it — metadata goes in the Executive Summary section)

## Batch Mode: `portfolio_report.md`

When `source` is a directory, also generates an aggregate report:
- Total workflows, average confidence, readiness distribution
- Table: workflow name, tool count, confidence, readiness, unsupported tools
- Sorted by lowest confidence first (worst = most attention needed)
- Links to individual `migration_report.md` files

## Confluence Integration

Uses the Confluence REST API v2 (not v1) via `atlassian-python-api` or direct `requests`.

**Draft creation flow:**
1. Check if a page with the workflow name already exists under `parent_page`
2. If exists: update the draft (idempotent)
3. If not: create a new page in **draft** status
4. Convert Markdown to Confluence storage format (Atlassian wiki markup)
5. Print the draft URL so the analyst can review and publish manually

**Mermaid handling for Confluence:**
- If the workspace has the Mermaid plugin: embed as `{mermaid}` macro
- Fallback: pre-render Mermaid to SVG using `mmdc` CLI (mermaid-cli) if available, otherwise embed as a code block with a note

**Confluence dependency:** `atlassian-python-api` added as optional:

```toml
[project.optional-dependencies]
confluence = ["atlassian-python-api>=3.0"]
```

The `document` command works without this package installed (local markdown only). If Confluence is configured but the package is missing, print: "Install confluence support: pip install alteryx2dbx[confluence]"

## Implementation: Where Code Lives

```
src/alteryx2dbx/
  document/
    __init__.py
    report.py          # Per-workflow migration_report.md generation
    portfolio.py       # Batch portfolio_report.md generation
    mermaid.py         # DAG → Mermaid.js flowchart string
    confluence.py      # Confluence draft creation + markdown conversion
    config.py          # .alteryx2dbx.yml loading + PAT resolution
  cli.py               # New `document` command added
```

The `document` command reuses the existing parser, DAG resolver, and handler registry (to get confidence scores). It does NOT run the full notebook generation pipeline — it parses, resolves, runs handlers for metadata only, then generates the report.

## What Does Not Change

- Existing commands (`parse`, `generate`, `convert`, `analyze`, `tools`) untouched
- `conversion_report.md` generated by `convert` stays as-is
- Notebook output structure unchanged

## Testing

- Unit tests for Mermaid generation from sample DAGs
- Unit tests for each report section with fixture workflows
- Unit tests for config loading (all PAT resolution paths)
- Unit tests for Confluence markdown → storage format conversion
- Integration test: `.yxmd` → `migration_report.md` with all sections populated
- Batch test: directory of workflows → `portfolio_report.md` + individual reports
- Config matrix test: no config, config without PAT, config with PAT (mocked Confluence API)
