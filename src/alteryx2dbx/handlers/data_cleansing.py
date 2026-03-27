from __future__ import annotations

from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep

from .base import ToolHandler
from .registry import register_type_handler


class DataCleansingHandler(ToolHandler):
    def convert(self, tool: AlteryxTool, input_df_names: list[str] | None = None) -> GeneratedStep:
        input_df = input_df_names[0] if input_df_names else "df_unknown"
        tid = tool.tool_id
        config = tool.config

        remove_null = config.get("RemoveNull", False)
        remove_whitespace = config.get("RemoveWhitespace", False)
        trim_whitespace = config.get("TrimWhitespace", False)
        modify_case = config.get("ModifyCase", "")  # Upper, Lower, Title, or ""
        fields = config.get("cleansing_fields", [])  # list of field name strings

        lines: list[str] = [
            f"# {tool.annotation or 'Data Cleansing'} (Tool {tid})",
            f"df_{tid} = {input_df}",
        ]

        if fields:
            target_fields = fields
        else:
            # Apply to all string columns — generate a dynamic block
            target_fields = None

        if target_fields:
            for fld in target_fields:
                col_expr = f'F.col("{fld}")'
                expr = self._build_chain(col_expr, trim_whitespace, remove_whitespace, modify_case, remove_null)
                lines.append(f'df_{tid} = df_{tid}.withColumn("{fld}", {expr})')
        else:
            # Dynamic: apply to all string columns at runtime
            chain_desc = self._build_chain("c", trim_whitespace, remove_whitespace, modify_case, remove_null)
            lines.append(f"# Apply cleansing to all string columns")
            lines.append(f"for _col_name in [f.name for f in df_{tid}.schema.fields if str(f.dataType) == 'StringType()']:")
            chain_runtime = self._build_chain("F.col(_col_name)", trim_whitespace, remove_whitespace, modify_case, remove_null)
            lines.append(f"    df_{tid} = df_{tid}.withColumn(_col_name, {chain_runtime})")

        code = "\n".join(lines)
        return GeneratedStep(
            step_name=f"data_cleansing_{tid}",
            code=code,
            imports={"from pyspark.sql import functions as F"},
            input_dfs=[input_df],
            output_df=f"df_{tid}",
            notes=[],
            confidence=1.0,
        )

    @staticmethod
    def _build_chain(
        col_expr: str,
        trim_whitespace: bool,
        remove_whitespace: bool,
        modify_case: str,
        remove_null: bool,
    ) -> str:
        """Build a chained PySpark expression string."""
        expr = col_expr
        if trim_whitespace:
            expr = f"F.trim({expr})"
        if remove_whitespace:
            expr = f'F.regexp_replace({expr}, r"\\s+", "")'
        if modify_case:
            case_lower = modify_case.lower()
            if case_lower == "upper":
                expr = f"F.upper({expr})"
            elif case_lower == "lower":
                expr = f"F.lower({expr})"
            elif case_lower == "title":
                expr = f"F.initcap({expr})"
        if remove_null:
            expr = f'F.coalesce({expr}, F.lit(""))'
        return expr


register_type_handler("DataCleansing", DataCleansingHandler)
