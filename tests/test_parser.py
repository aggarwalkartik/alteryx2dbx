from pathlib import Path
from alteryx2dbx.parser.models import AlteryxWorkflow
from alteryx2dbx.parser.xml_parser import parse_yxmd

FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_simple_filter_returns_workflow():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert isinstance(wf, AlteryxWorkflow)
    assert wf.name == "simple_filter"


def test_parse_simple_filter_tools():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert len(wf.tools) == 3
    assert 1 in wf.tools and 2 in wf.tools and 3 in wf.tools


def test_parse_tool_types():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert wf.tools[1].tool_type == "DbFileInput"
    assert wf.tools[2].tool_type == "Filter"
    assert wf.tools[3].tool_type == "DbFileOutput"


def test_parse_tool_plugins():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert "DbFileInput" in wf.tools[1].plugin
    assert "Filter" in wf.tools[2].plugin


def test_parse_tool_annotations():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert wf.tools[1].annotation == "Input Customers"
    assert wf.tools[2].annotation == "Filter Active High Rev"


def test_parse_connections():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    assert len(wf.connections) == 2
    conn1 = wf.connections[0]
    assert conn1.source_tool_id == 1
    assert conn1.source_anchor == "Output"
    assert conn1.target_tool_id == 2
    assert conn1.target_anchor == "Input"


def test_parse_filter_config():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    filter_tool = wf.tools[2]
    assert "expression" in filter_tool.config
    assert "active" in filter_tool.config["expression"]


def test_parse_input_fields():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    input_tool = wf.tools[1]
    assert len(input_tool.output_fields) == 4
    field_names = [f.name for f in input_tool.output_fields]
    assert "customer_id" in field_names
    assert "revenue" in field_names


def test_parse_input_config_file_path():
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    input_tool = wf.tools[1]
    assert "file_path" in input_tool.config
    assert "customers.xlsx" in input_tool.config["file_path"]


def test_parse_nonexistent_file_raises():
    import pytest
    with pytest.raises(FileNotFoundError):
        parse_yxmd(Path("nonexistent.yxmd"))
