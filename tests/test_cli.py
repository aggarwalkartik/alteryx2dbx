import zipfile as zf_mod
from pathlib import Path
from click.testing import CliRunner
from alteryx2dbx.cli import main

FIXTURES = Path(__file__).parent / "fixtures"


def test_cli_convert_single_file(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, ["convert", str(FIXTURES / "simple_filter.yxmd"), "-o", str(tmp_path)])
    assert result.exit_code == 0
    assert (tmp_path / "simple_filter" / "01_load_sources.py").exists()


def test_cli_convert_directory(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, ["convert", str(FIXTURES), "-o", str(tmp_path)])
    assert result.exit_code == 0


def test_cli_analyze():
    runner = CliRunner()
    result = runner.invoke(main, ["analyze", str(FIXTURES / "simple_filter.yxmd")])
    assert result.exit_code == 0
    assert "simple_filter" in result.output


def test_cli_tools():
    runner = CliRunner()
    result = runner.invoke(main, ["tools"])
    assert result.exit_code == 0
    assert "Filter" in result.output


MINIMAL_YXMD = (
    '<?xml version="1.0"?>\n'
    '<AlteryxDocument yxmdVer="2024.1">\n'
    '  <Properties><MetaInfo><Name>ParseTest</Name></MetaInfo></Properties>\n'
    '  <Nodes>\n'
    '    <Node ToolID="1">\n'
    '      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput"/>\n'
    '      <Properties><Configuration><File>data/input.csv</File></Configuration></Properties>\n'
    '    </Node>\n'
    '  </Nodes>\n'
    '  <Connections></Connections>\n'
    '</AlteryxDocument>\n'
)


def test_analyze_yxzp(tmp_path):
    yxzp = tmp_path / "test.yxzp"
    with zf_mod.ZipFile(yxzp, "w") as zf:
        zf.writestr("inner.yxmd", MINIMAL_YXMD)
    runner = CliRunner()
    result = runner.invoke(main, ["analyze", str(yxzp)])
    assert result.exit_code == 0
    assert "Coverage:" in result.output
