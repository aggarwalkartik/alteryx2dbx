"""Parser for Alteryx .yxmd workflow files."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from .models import AlteryxConnection, AlteryxField, AlteryxTool, AlteryxWorkflow


def parse_yxmd(path: Path) -> AlteryxWorkflow:
    """Parse a .yxmd file into an AlteryxWorkflow IR model.

    Args:
        path: Path to the .yxmd file.

    Returns:
        Parsed AlteryxWorkflow dataclass.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Workflow file not found: {path}")

    tree = ET.parse(path)
    root = tree.getroot()

    version = root.get("yxmdVer", "")

    # Parse workflow-level properties
    properties = _parse_properties(root)

    # Extract workflow name from Properties/MetaInfo/Name, fall back to filename
    name = _extract_workflow_name(root, path)

    # Parse tools (nodes)
    tools = _parse_tools(root)

    # Parse connections
    connections = _parse_connections(root)

    return AlteryxWorkflow(
        name=name,
        version=version,
        tools=tools,
        connections=connections,
        properties=properties,
    )


def _extract_workflow_name(root: ET.Element, path: Path) -> str:
    """Extract workflow name from MetaInfo or fall back to filename stem."""
    # Look in Properties/MetaInfo/Name
    meta_name = root.find("./Properties/MetaInfo/Name")
    if meta_name is not None and meta_name.text:
        return meta_name.text
    return path.stem


def _parse_properties(root: ET.Element) -> dict:
    """Parse workflow-level Properties into a dict."""
    props = {}
    props_el = root.find("Properties")
    if props_el is None:
        return props

    for child in props_el:
        if child.tag == "MetaInfo":
            meta = {}
            for meta_child in child:
                meta[meta_child.tag] = meta_child.text or meta_child.get("value", "")
            props["MetaInfo"] = meta
        else:
            # Store simple properties as key: value or key: attribs
            if child.attrib:
                props[child.tag] = dict(child.attrib)
            elif child.text and child.text.strip():
                props[child.tag] = child.text.strip()
            else:
                props[child.tag] = {k: v for k, v in child.attrib.items()} or ""
    return props


def _parse_tools(root: ET.Element) -> dict[int, AlteryxTool]:
    """Parse all Node elements into AlteryxTool dataclasses."""
    tools: dict[int, AlteryxTool] = {}

    for node in root.findall(".//Nodes/Node"):
        tool_id = int(node.get("ToolID", "0"))

        # Extract plugin from GuiSettings
        gui_settings = node.find("GuiSettings")
        plugin = gui_settings.get("Plugin", "") if gui_settings is not None else ""

        # Derive tool_type: last segment of the dotted plugin name
        tool_type = plugin.rsplit(".", 1)[-1] if plugin else ""

        # Extract annotation name
        annotation = _extract_annotation(node)

        # Extract config
        config = _extract_config(node, tool_type)

        # Extract output fields from MetaInfo/RecordInfo
        output_fields = _extract_fields(node)

        tools[tool_id] = AlteryxTool(
            tool_id=tool_id,
            plugin=plugin,
            tool_type=tool_type,
            config=config,
            annotation=annotation,
            output_fields=output_fields,
        )

    return tools


def _extract_annotation(node: ET.Element) -> str:
    """Extract the annotation name from a Node element."""
    ann_name = node.find(".//Annotation/Name")
    if ann_name is not None and ann_name.text:
        return ann_name.text
    return ""


def _extract_config(node: ET.Element, tool_type: str) -> dict:
    """Extract tool-specific configuration from a Node's Configuration block."""
    config_el = node.find(".//Configuration")
    if config_el is None:
        return {}

    config: dict = {}

    # Store raw XML for all tools
    config["_raw_xml"] = ET.tostring(config_el, encoding="unicode")

    # Tool-specific extraction
    if tool_type in ("DbFileInput", "DbFileOutput"):
        _extract_file_config(config_el, config)
    elif tool_type == "Filter":
        _extract_filter_config(config_el, config)
    elif tool_type == "Formula":
        _extract_formula_config(config_el, config)
    elif tool_type in ("Join", "FindReplace"):
        _extract_join_config(config_el, config)
    elif tool_type in ("AlteryxSelect", "Select"):
        _extract_select_config(config_el, config)
    elif tool_type == "Summarize":
        _extract_summarize_config(config_el, config)
    elif tool_type == "Sort":
        _extract_sort_config(config_el, config)

    return config


def _extract_file_config(config_el: ET.Element, config: dict) -> None:
    """Extract file path and format from DbFileInput/DbFileOutput config."""
    file_el = config_el.find("File")
    if file_el is not None:
        if file_el.text:
            config["file_path"] = file_el.text
        file_format = file_el.get("FileFormat")
        if file_format:
            config["FileFormat"] = file_format


def _extract_filter_config(config_el: ET.Element, config: dict) -> None:
    """Extract filter expression."""
    expr_el = config_el.find("Expression")
    if expr_el is not None and expr_el.text:
        config["expression"] = expr_el.text
    mode_el = config_el.find("Mode")
    if mode_el is not None and mode_el.text:
        config["mode"] = mode_el.text


def _extract_formula_config(config_el: ET.Element, config: dict) -> None:
    """Extract formula fields."""
    fields = []
    for formula_field in config_el.findall(".//FormulaField"):
        fields.append({
            "field": formula_field.get("field", ""),
            "expression": formula_field.get("expression", ""),
            "type": formula_field.get("type", ""),
            "size": formula_field.get("size", ""),
        })
    if fields:
        config["formula_fields"] = fields


def _extract_join_config(config_el: ET.Element, config: dict) -> None:
    """Extract join configuration."""
    join_fields = []
    for join_el in config_el.findall(".//JoinInfo"):
        connection = join_el.get("connection", "")
        for field_el in join_el.findall("Field"):
            left = field_el.get("left", "")
            right = field_el.get("right", "")
            if left and right:
                join_fields.append({"left": left, "right": right})
        # Store connection type (Left/Right indicates which side this JoinInfo describes)
        if connection:
            config["join_connection"] = connection
    if join_fields:
        config["join_fields"] = join_fields
    # Extract join-by-record-position flag
    by_pos = config_el.find("JoinByRecordPos")
    if by_pos is not None:
        config["join_by_position"] = by_pos.get("value", "False") == "True"


def _extract_select_config(config_el: ET.Element, config: dict) -> None:
    """Extract select field configuration."""
    fields = []
    for sf in config_el.findall(".//SelectField"):
        fields.append({
            "field": sf.get("field", ""),
            "selected": sf.get("selected", "True"),
            "rename": sf.get("rename", ""),
            "type": sf.get("type", ""),
            "size": sf.get("size", ""),
        })
    if fields:
        config["select_fields"] = fields


def _extract_summarize_config(config_el: ET.Element, config: dict) -> None:
    """Extract summarize field configuration."""
    fields = []
    for sf in config_el.findall(".//SummarizeField"):
        fields.append({
            "field": sf.get("field", ""),
            "action": sf.get("action", ""),
            "rename": sf.get("rename", ""),
        })
    if fields:
        config["summarize_fields"] = fields


def _extract_sort_config(config_el: ET.Element, config: dict) -> None:
    """Extract sort configuration."""
    fields = []
    # Look specifically inside SortInfo for Field elements
    sort_info = config_el.find("SortInfo")
    if sort_info is not None:
        for sf in sort_info.findall("Field"):
            fields.append({
                "field": sf.get("field", ""),
                "order": sf.get("order", "Ascending"),
            })
    if fields:
        config["sort_fields"] = fields


def _extract_fields(node: ET.Element) -> list[AlteryxField]:
    """Extract output fields from MetaInfo/RecordInfo."""
    fields: list[AlteryxField] = []
    for meta_info in node.findall(".//MetaInfo"):
        for field_el in meta_info.findall(".//RecordInfo/Field"):
            name = field_el.get("name", "")
            ftype = field_el.get("type", "")
            size_str = field_el.get("size")
            scale_str = field_el.get("scale")
            fields.append(AlteryxField(
                name=name,
                type=ftype,
                size=int(size_str) if size_str else None,
                scale=int(scale_str) if scale_str else None,
            ))
    return fields


def _parse_connections(root: ET.Element) -> list[AlteryxConnection]:
    """Parse all Connection elements into AlteryxConnection dataclasses."""
    connections: list[AlteryxConnection] = []

    for conn in root.findall(".//Connections/Connection"):
        origin = conn.find("Origin")
        dest = conn.find("Destination")
        if origin is not None and dest is not None:
            connections.append(AlteryxConnection(
                source_tool_id=int(origin.get("ToolID", "0")),
                source_anchor=origin.get("Connection", ""),
                target_tool_id=int(dest.get("ToolID", "0")),
                target_anchor=dest.get("Connection", ""),
            ))

    return connections
