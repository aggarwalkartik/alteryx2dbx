"""Handler for tools that reference an Alteryx macro (.yxmc).

Macro inlining is not yet implemented. Rather than silently emit a passthrough
that hides the missing logic, this handler emits a loud banner plus a runtime
``NotImplementedError`` so notebooks fail visibly if run without a manual fix.
"""
from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep

from .base import ToolHandler


# Notes starting with this prefix are used by batch stats / CLI to count macros
# separately from other unsupported tools.
MACRO_NOTE_PREFIX = "MACRO: "


class MacroReferenceHandler(ToolHandler):
    def convert(
        self, tool: AlteryxTool, input_df_names: list[str] | None = None
    ) -> GeneratedStep:
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        macro_name = tool.macro_path or tool.plugin or "<unknown>"
        annotation = tool.annotation or tool.tool_type or "Macro"
        # Macro paths are usually Windows-style with backslashes; escape them so
        # the emitted Python string literal is valid.
        macro_literal = macro_name.replace("\\", "\\\\").replace('"', '\\"')

        code = (
            f"# ⚠️ MACRO REFERENCE — NOT CONVERTED\n"
            f"# Tool ID: {tool.tool_id}\n"
            f"# Annotation: {annotation}\n"
            f"# Macro: {macro_name}\n"
            f"# Upstream DataFrame: {input_df}\n"
            f"#\n"
            f"# Macro inlining is not yet supported by alteryx2dbx. The original\n"
            f"# workflow calls a macro whose body must be reimplemented by hand\n"
            f"# before this notebook can run end-to-end.\n"
            f"#\n"
            f"# Until then, executing this cell will raise to prevent silent data\n"
            f"# loss. Replace the raise with your translated logic and remove this\n"
            f"# banner when done.\n"
            f"raise NotImplementedError(\n"
            f"    \"Macro '{macro_literal}' (Tool {tool.tool_id}) not converted. \"\n"
            f"    \"See conversion_report.md.\"\n"
            f")\n"
            f"df_{tool.tool_id} = {input_df}  # unreachable; placeholder for downstream refs"
        )

        return GeneratedStep(
            step_name=f"macro_{tool.tool_id}",
            code=code,
            imports=set(),
            input_dfs=[input_df],
            output_df=f"df_{tool.tool_id}",
            notes=[f"{MACRO_NOTE_PREFIX}{macro_name} (Tool {tool.tool_id})"],
            confidence=0.0,
        )
