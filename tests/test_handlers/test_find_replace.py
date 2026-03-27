from alteryx2dbx.parser.models import AlteryxTool
from alteryx2dbx.handlers.find_replace import FindReplaceHandler


def _make_tool(
    tool_id=25,
    find_field="Category",
    replace_field="NewCategory",
    find_mode="Normal",
    annotation="Find Replace",
):
    return AlteryxTool(
        tool_id=tool_id,
        plugin="AlteryxBasePluginsEngine.FindReplace",
        tool_type="FindReplace",
        config={
            "find_field": find_field,
            "replace_field": replace_field,
            "find_mode": find_mode,
        },
        annotation=annotation,
    )


class TestFindReplaceHandler:
    def test_generates_join_code(self):
        handler = FindReplaceHandler()
        tool = _make_tool()
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert "df_1.join(" in step.code
        assert "df_2.select(" in step.code

    def test_uses_find_and_replace_fields(self):
        handler = FindReplaceHandler()
        tool = _make_tool(find_field="Status", replace_field="NewStatus")
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert '"Status"' in step.code
        assert '"NewStatus"' in step.code

    def test_left_join_for_lookup(self):
        handler = FindReplaceHandler()
        tool = _make_tool()
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert '"left"' in step.code

    def test_coalesce_for_replacement(self):
        handler = FindReplaceHandler()
        tool = _make_tool()
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert "F.coalesce" in step.code

    def test_drops_temp_columns(self):
        handler = FindReplaceHandler()
        tool = _make_tool(tool_id=30)
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert "_fr_find_30" in step.code
        assert "_fr_replace_30" in step.code
        assert ".drop(" in step.code

    def test_two_input_dfs(self):
        handler = FindReplaceHandler()
        tool = _make_tool()
        step = handler.convert(tool, input_df_names=["df_data", "df_lookup"])

        assert step.input_dfs == ["df_data", "df_lookup"]

    def test_output_df_name(self):
        handler = FindReplaceHandler()
        tool = _make_tool(tool_id=42)
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert step.output_df == "df_42"

    def test_confidence_is_0_9(self):
        handler = FindReplaceHandler()
        tool = _make_tool()
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert step.confidence == 0.9

    def test_imports_include_pyspark_functions(self):
        handler = FindReplaceHandler()
        tool = _make_tool()
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert "from pyspark.sql import functions as F" in step.imports

    def test_case_insensitive_matching(self):
        handler = FindReplaceHandler()
        tool = _make_tool()
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert "F.lower(" in step.code

    def test_find_mode_in_notes(self):
        handler = FindReplaceHandler()
        tool = _make_tool(find_mode="RegEx")
        step = handler.convert(tool, input_df_names=["df_1", "df_2"])

        assert any("RegEx" in note for note in step.notes)

    def test_default_input_when_none(self):
        handler = FindReplaceHandler()
        tool = _make_tool()
        step = handler.convert(tool, input_df_names=None)

        assert "df_unknown" in step.code
