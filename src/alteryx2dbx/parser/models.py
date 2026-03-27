from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class AlteryxField:
    name: str
    type: str
    size: int | None = None
    scale: int | None = None


@dataclass
class AlteryxConnection:
    source_tool_id: int
    source_anchor: str
    target_tool_id: int
    target_anchor: str


@dataclass
class AlteryxTool:
    tool_id: int
    plugin: str
    tool_type: str
    config: dict
    annotation: str = ""
    input_fields: list[AlteryxField] = field(default_factory=list)
    output_fields: list[AlteryxField] = field(default_factory=list)


@dataclass
class AlteryxWorkflow:
    name: str
    version: str
    tools: dict[int, AlteryxTool] = field(default_factory=dict)
    connections: list[AlteryxConnection] = field(default_factory=list)
    properties: dict = field(default_factory=dict)


@dataclass
class GeneratedStep:
    step_name: str
    code: str
    imports: set[str] = field(default_factory=set)
    input_dfs: list[str] = field(default_factory=list)
    output_df: str = ""
    notes: list[str] = field(default_factory=list)
    confidence: float = 1.0
