# Box Tool Support

## Context

Most real-world Alteryx workflows use Box.com connectors to read/write data. These are marketplace add-on tools with versioned plugin strings like `box_input_v1.0.3`. The current parser and handler registry cannot match them (no prefix matching), so any workflow containing Box tools fails to convert meaningfully.

Without Box support, the tool is unusable for the majority of enterprise workflows.

## What Changes

### 1. Registry: Prefix Matching (Third Lookup Tier)

The `HandlerRegistry.get()` currently does:
1. Exact match on `tool.plugin`
2. Exact match on `tool.tool_type`
3. Fallback to `UnsupportedHandler`

Add a third tier between 2 and 3: **prefix matching**. Handlers can register a prefix (e.g. `box_input_v`) and any plugin string starting with that prefix matches.

```python
# registry.py additions
class HandlerRegistry:
    def __init__(self):
        self._handlers: dict[str, type[ToolHandler]] = {}
        self._type_handlers: dict[str, type[ToolHandler]] = {}
        self._prefix_handlers: dict[str, type[ToolHandler]] = {}

    def register_prefix(self, prefix: str, handler_cls: type[ToolHandler]):
        self._prefix_handlers[prefix] = handler_cls

    def get(self, tool: AlteryxTool) -> ToolHandler:
        # 1. Exact plugin match
        handler_cls = self._handlers.get(tool.plugin)
        if handler_cls:
            return handler_cls()
        # 2. Exact tool_type match
        handler_cls = self._type_handlers.get(tool.tool_type)
        if handler_cls:
            return handler_cls()
        # 3. Prefix match on plugin string
        for prefix, cls in self._prefix_handlers.items():
            if tool.plugin.startswith(prefix):
                return cls()
        return UnsupportedHandler()
```

Module-level helper:

```python
def register_prefix_handler(prefix: str, handler_cls: type[ToolHandler]):
    _registry.register_prefix(prefix, handler_cls)
```

### 2. Parser: Box Config Extraction

In `xml_parser.py._extract_config()`, add detection for Box tools. Since Box plugin strings are versioned (no dots), `tool_type` will be the full string like `box_input_v1.0.3`. Detect with `startswith`:

```python
if tool_type.startswith("box_input_v") or tool_type.startswith("box_output_v"):
    _extract_box_config(config_el, config)
```

New extractor `_extract_box_config()` pulls:
- `FilePath` (Box folder path)
- `boxFileId` (Box file ID — the key for API access)
- `boxParentId` (parent folder ID, for output)
- `fileName` (display name)
- `FileFormat` (Delimited, Excel, JSON, Avro)
- `authType` (EndUser or ServicePrincipal)
- `ExcelSheetNameValues` (sheet name for Excel files)
- `DelimitedHasHeader` (header flag for CSV)
- `Delimiter` (COMMA, TAB, PIPE, etc.)
- `ExistingFileBehavior` (Abort, Rename, Overwrite, Append — output only)

Encrypted fields (`clientSecret`, `accessToken`, `serviceCredsJSONStr`) are noted but not extracted — credentials come from Databricks Secrets at runtime.

### 3. BoxInputHandler

New file: `src/alteryx2dbx/handlers/box_input.py`

Registers with prefix `box_input_v`. Generates PySpark code that:
1. Gets a Box client from `_utils.py` helper
2. Downloads file by `boxFileId` to memory via `BytesIO`
3. Reads into pandas DataFrame based on `FileFormat`
4. Converts to Spark DataFrame

Generated code pattern:

```python
# Box Input: {annotation} (file: {fileName})
_box_file = box_client.file("{boxFileId}")
_box_bytes = BytesIO(_box_file.content())
df_{tool_id} = spark.createDataFrame(pd.read_csv(_box_bytes, header=True))
```

Format mapping:
- `Delimited` → `pd.read_csv(stream, sep=delimiter, header=has_header)`
- `Excel` → `pd.read_excel(stream, sheet_name=sheet)`
- `JSON` → `pd.read_json(stream)`
- `Avro` → TODO comment (rare, flag for manual review)

Confidence: **0.8** (auth setup is manual, file access depends on permissions).

Adds note: "Box auth requires Databricks Secret scope — see _config.py"

### 4. BoxOutputHandler

New file: `src/alteryx2dbx/handlers/box_output.py`

Registers with prefix `box_output_v`. Generates code that:
1. Converts DataFrame to pandas
2. Writes to `BytesIO` in target format
3. Uploads via Box SDK to `boxParentId` folder

Generated code pattern:

```python
# Box Output: {annotation} (file: {fileName})
_out_bytes = BytesIO()
df_{input}.toPandas().to_csv(_out_bytes, index=False)
_out_bytes.seek(0)
box_client.folder("{boxParentId}").upload_stream(_out_bytes, "{fileName}")
```

Handles `ExistingFileBehavior`:
- `Overwrite` → `file.update_contents_with_stream()`
- `Abort` → check existence first, raise if exists
- `Rename` / `Append` → TODO comment (complex, flag for review)

Confidence: **0.7** (write operations are higher risk).

### 5. Config and Utils Updates

**`_config.py`** gets a new widget:

```python
dbutils.widgets.text("box_secret_scope", "box", "Databricks Secret scope for Box credentials")
BOX_SECRET_SCOPE = dbutils.widgets.get("box_secret_scope")
```

**`_utils.py`** gets a `box_client` helper:

```python
def get_box_client():
    """Return authenticated Box client using Databricks Secrets."""
    from boxsdk import JWTAuth, Client
    import json
    jwt_config = json.loads(dbutils.secrets.get(BOX_SECRET_SCOPE, "jwt_config"))
    auth = JWTAuth.from_settings_dictionary(jwt_config)
    return Client(auth)

box_client = get_box_client()
```

These sections are only included when the workflow contains Box tools (detected during generation).

### 6. Dependencies

Add `boxsdk` to `pyproject.toml` as an optional dependency:

```toml
[project.optional-dependencies]
box = ["boxsdk[jwt]>=3.0"]
```

The generated notebooks include a `%pip install boxsdk[jwt]` cell when Box tools are present. The CLI itself does not require `boxsdk` — it only generates code that uses it.

## What Does Not Change

- Existing handlers, parser logic, notebook structure untouched
- The 5-notebook output structure stays the same (Box reads go into `01_load_sources.py`, Box writes into `03_write_outputs.py`)
- No new CLI commands

## Testing

- Unit tests for `_extract_box_config` with sample XML
- Unit tests for `BoxInputHandler` / `BoxOutputHandler` with all format variants
- Registry test: prefix matching resolves correctly, doesn't interfere with exact matches
- E2E test: workflow with Box Input + transforms + Box Output → valid notebook bundle
- Test fixtures: sample .yxmd snippets with Box tool XML (from the research doc)
