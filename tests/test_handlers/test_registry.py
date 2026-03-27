from alteryx2dbx.parser.models import AlteryxTool, GeneratedStep
from alteryx2dbx.handlers.base import ToolHandler, UnsupportedHandler
from alteryx2dbx.handlers.registry import HandlerRegistry, get_handler


def _make_tool(**overrides):
    defaults = dict(
        tool_id=7,
        plugin="AlteryxBasePluginsGui.Foobar.Foobar",
        tool_type="Foobar",
        config={"_raw_xml": "<Configuration>\n  <Option />\n</Configuration>"},
        annotation="Some annotation",
    )
    defaults.update(overrides)
    return AlteryxTool(**defaults)


# ── UnsupportedHandler ───────────────────────────────────────────────


def test_unsupported_handler_returns_passthrough():
    tool = _make_tool()
    handler = UnsupportedHandler()
    step = handler.convert(tool, input_df_names=["df_3"])

    assert step.output_df == "df_7"
    assert step.confidence == 0.0
    assert "PASSTHROUGH" in step.code
    assert "TODO" in step.code
    assert "UNSUPPORTED TOOL" in step.code
    assert "Foobar" in step.code


def test_unsupported_handler_no_input_dfs():
    tool = _make_tool()
    handler = UnsupportedHandler()
    step = handler.convert(tool, input_df_names=None)

    assert "df_unknown" in step.code
    assert step.input_dfs == ["df_unknown"]


def test_unsupported_handler_preserves_raw_xml():
    tool = _make_tool(config={"_raw_xml": "<Cfg>\n  <Val>123</Val>\n</Cfg>"})
    handler = UnsupportedHandler()
    step = handler.convert(tool, input_df_names=["df_1"])

    assert "# <Cfg>" in step.code
    assert "#   <Val>123</Val>" in step.code


def test_unsupported_handler_notes():
    tool = _make_tool()
    handler = UnsupportedHandler()
    step = handler.convert(tool, input_df_names=["df_1"])

    assert len(step.notes) == 1
    assert "Foobar" in step.notes[0]
    assert tool.plugin in step.notes[0]


# ── HandlerRegistry ─────────────────────────────────────────────────


class DummyHandler(ToolHandler):
    def convert(self, tool, input_df_names=None):
        return GeneratedStep(
            step_name="dummy",
            code="# dummy",
            output_df="df_dummy",
        )


def test_registry_returns_unsupported_for_unknown_tool():
    registry = HandlerRegistry()
    tool = _make_tool()
    handler = registry.get(tool)
    assert isinstance(handler, UnsupportedHandler)


def test_registry_plugin_lookup():
    registry = HandlerRegistry()
    registry.register("AlteryxBasePluginsGui.Foobar.Foobar", DummyHandler)
    tool = _make_tool()
    handler = registry.get(tool)
    assert isinstance(handler, DummyHandler)


def test_registry_type_lookup():
    registry = HandlerRegistry()
    registry.register_type("Foobar", DummyHandler)
    tool = _make_tool()
    handler = registry.get(tool)
    assert isinstance(handler, DummyHandler)


def test_registry_plugin_takes_priority_over_type():
    registry = HandlerRegistry()

    class PluginHandler(ToolHandler):
        def convert(self, tool, input_df_names=None):
            return GeneratedStep(step_name="plugin", code="", output_df="")

    registry.register("AlteryxBasePluginsGui.Foobar.Foobar", PluginHandler)
    registry.register_type("Foobar", DummyHandler)
    tool = _make_tool()
    handler = registry.get(tool)
    assert isinstance(handler, PluginHandler)


def test_get_handler_module_function():
    """Test the module-level get_handler convenience function."""
    tool = _make_tool()
    handler = get_handler(tool)
    # Unknown tool → UnsupportedHandler
    assert isinstance(handler, UnsupportedHandler)
