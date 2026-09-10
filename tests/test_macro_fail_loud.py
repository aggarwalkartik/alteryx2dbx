"""Macro references must fail loudly, not silently passthrough.

Covers the end-to-end contract:
  1. Parser tags macro-referencing nodes with is_macro + macro_path.
  2. Registry routes macro tools to MacroReferenceHandler.
  3. Generated code raises NotImplementedError at runtime.
  4. CLI surfaces macro warnings and exits non-zero.
"""
from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from alteryx2dbx.cli import main
from alteryx2dbx.handlers.macro_reference import MacroReferenceHandler
from alteryx2dbx.handlers.registry import get_handler
from alteryx2dbx.parser.xml_parser import parse_yxmd

# Kept in its own directory so batch tests over tests/fixtures/ don't pick it up.
FIXTURE = Path(__file__).parent / "fixtures_macros" / "macro_workflow.yxmd"


def test_parser_detects_macro_reference():
    wf = parse_yxmd(FIXTURE)
    macro_tool = wf.tools[2]
    assert macro_tool.is_macro is True
    assert macro_tool.macro_path.endswith("CleanseOrders.yxmc")


def test_non_macro_tools_are_not_flagged():
    wf = parse_yxmd(FIXTURE)
    assert wf.tools[1].is_macro is False
    assert wf.tools[3].is_macro is False


def test_registry_routes_macros_to_macro_handler():
    wf = parse_yxmd(FIXTURE)
    handler = get_handler(wf.tools[2])
    assert isinstance(handler, MacroReferenceHandler)


def test_macro_handler_emits_runtime_raise():
    wf = parse_yxmd(FIXTURE)
    handler = get_handler(wf.tools[2])
    step = handler.convert(wf.tools[2], input_df_names=["df_1"])
    assert "raise NotImplementedError" in step.code
    assert "MACRO REFERENCE" in step.code
    assert step.confidence == 0.0
    assert any(n.startswith("MACRO:") for n in step.notes)


def test_convert_cli_exits_nonzero_and_warns_on_macros(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        main, ["convert", str(FIXTURE), "-o", str(tmp_path), "--full"]
    )
    assert result.exit_code == 1
    # Banner lands on stderr, which click merges into output by default.
    assert "MACRO" in result.output
    assert "CleanseOrders.yxmc" in result.output


def test_analyze_cli_reports_macro_status():
    runner = CliRunner()
    result = runner.invoke(main, ["analyze", str(FIXTURE)])
    assert result.exit_code == 0
    assert "[MACRO]" in result.output
    assert "Macros: 1 tool(s)" in result.output
