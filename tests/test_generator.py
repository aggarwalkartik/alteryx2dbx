from pathlib import Path
from alteryx2dbx.parser.xml_parser import parse_yxmd
from alteryx2dbx.generator.notebook import generate_notebooks

FIXTURES = Path(__file__).parent / "fixtures"


def test_generate_creates_output_dir(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    generate_notebooks(wf, tmp_path)
    assert (tmp_path / "simple_filter").exists()


def test_generate_creates_all_files(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    generate_notebooks(wf, tmp_path)
    wf_dir = tmp_path / "simple_filter"
    assert (wf_dir / "config.yml").exists()
    assert (wf_dir / "01_load_sources.py").exists()
    assert (wf_dir / "02_transformations.py").exists()
    assert (wf_dir / "03_orchestrate.py").exists()
    assert (wf_dir / "04_validate.py").exists()
    assert (wf_dir / "conversion_report.md").exists()


def test_notebooks_have_databricks_header(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    generate_notebooks(wf, tmp_path)
    content = (tmp_path / "simple_filter" / "01_load_sources.py").read_text()
    assert "# Databricks notebook source" in content


def test_notebooks_have_command_separators(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    generate_notebooks(wf, tmp_path)
    content = (tmp_path / "simple_filter" / "02_transformations.py").read_text()
    assert "# COMMAND ----------" in content


def test_load_sources_has_spark_read(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    generate_notebooks(wf, tmp_path)
    content = (tmp_path / "simple_filter" / "01_load_sources.py").read_text()
    assert "spark.read" in content


def test_transformations_has_filter(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    generate_notebooks(wf, tmp_path)
    content = (tmp_path / "simple_filter" / "02_transformations.py").read_text()
    assert "filter" in content.lower()


def test_validator_has_datacompy(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    generate_notebooks(wf, tmp_path)
    content = (tmp_path / "simple_filter" / "04_validate.py").read_text()
    assert "datacompy" in content


def test_report_has_tools(tmp_path):
    wf = parse_yxmd(FIXTURES / "simple_filter.yxmd")
    generate_notebooks(wf, tmp_path)
    content = (tmp_path / "simple_filter" / "conversion_report.md").read_text()
    assert "Filter" in content
