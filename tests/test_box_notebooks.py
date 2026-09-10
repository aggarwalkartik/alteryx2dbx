from pathlib import Path
from alteryx2dbx.parser.xml_parser import parse_yxmd
from alteryx2dbx.generator.notebook_v2 import generate_notebooks_v2

BOX_WORKFLOW_YXMD = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2024.1">
  <Properties><MetaInfo><Name>BoxWorkflow</Name></MetaInfo></Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="box_input_v1.0.3">
        <Position x="78" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <authType>ServicePrincipal</authType>
          <FilePath>/data/input.csv</FilePath>
          <boxFileId>123456789</boxFileId>
          <boxParentId>987654321</boxParentId>
          <fileName>input.csv</fileName>
          <FileFormat>Delimited</FileFormat>
          <DelimitedHasHeader>True</DelimitedHasHeader>
          <Delimiter>COMMA</Delimiter>
        </Configuration>
        <Annotation DisplayMode="0"><Name>Box Input</Name></Annotation>
      </Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="box_output_v1.0.3">
        <Position x="258" y="78"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <authType>ServicePrincipal</authType>
          <FilePath>/data/output.csv</FilePath>
          <boxFileId>111222333</boxFileId>
          <boxParentId>444555666</boxParentId>
          <fileName>output.csv</fileName>
          <FileFormat>Delimited</FileFormat>
          <ExistingFileBehavior>Overwrite</ExistingFileBehavior>
        </Configuration>
        <Annotation DisplayMode="0"><Name>Box Output</Name></Annotation>
      </Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection>
      <Origin ToolID="1" Connection="Output"/>
      <Destination ToolID="2" Connection="Output"/>
    </Connection>
  </Connections>
</AlteryxDocument>
'''


def test_box_workflow_generates_config_with_box_scope(tmp_path):
    wf_file = tmp_path / "box_wf.yxmd"
    wf_file.write_text(BOX_WORKFLOW_YXMD, encoding="utf-8")
    wf = parse_yxmd(wf_file)
    output_dir = tmp_path / "output"
    generate_notebooks_v2(wf, output_dir)
    config_content = (output_dir / "BoxWorkflow" / "_config.py").read_text()
    assert "box_secret_scope" in config_content
    assert "BOX_SECRET_SCOPE" in config_content


def test_box_workflow_generates_utils_with_box_client(tmp_path):
    wf_file = tmp_path / "box_wf.yxmd"
    wf_file.write_text(BOX_WORKFLOW_YXMD, encoding="utf-8")
    wf = parse_yxmd(wf_file)
    output_dir = tmp_path / "output"
    generate_notebooks_v2(wf, output_dir)
    utils_content = (output_dir / "BoxWorkflow" / "_utils.py").read_text()
    assert "get_box_client" in utils_content
    assert "box_client" in utils_content


def test_non_box_workflow_has_no_box_config(tmp_path):
    fixtures = Path(__file__).parent / "fixtures"
    wf = parse_yxmd(fixtures / "simple_filter.yxmd")
    output_dir = tmp_path / "output"
    generate_notebooks_v2(wf, output_dir)
    config_content = (output_dir / "simple_filter" / "_config.py").read_text()
    assert "box_secret_scope" not in config_content
