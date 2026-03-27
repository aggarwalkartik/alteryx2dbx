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
