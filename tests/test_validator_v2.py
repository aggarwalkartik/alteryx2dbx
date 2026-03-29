"""Tests for validator_v2 — smart validation generator with auto-detected join keys."""
from __future__ import annotations

from pathlib import Path

import pytest

from alteryx2dbx.parser.models import (
    AlteryxConnection,
    AlteryxField,
    AlteryxTool,
    AlteryxWorkflow,
    GeneratedStep,
)
from alteryx2dbx.generator.validator_v2 import detect_join_keys, generate_validator_v2


def _make_workflow(
    tools: dict[int, AlteryxTool] | None = None,
    connections: list[AlteryxConnection] | None = None,
) -> AlteryxWorkflow:
    return AlteryxWorkflow(
        name="test_wf",
        version="2024.1",
        tools=tools or {},
        connections=connections or [],
    )


# ── detect_join_keys ─────────────────────────────────────────────────


class TestDetectJoinKeys:
    def test_detect_join_keys_from_join_tool(self):
        """Workflow with a Join tool → finds the join key from config."""
        join_tool = AlteryxTool(
            tool_id=1,
            plugin="AlteryxBasePluginsGui.Join.Join",
            tool_type="Join",
            config={
                "join_fields": [
                    {"left": "customer_id", "right": "cust_id"},
                    {"left": "order_date", "right": "date"},
                ],
            },
        )
        wf = _make_workflow(tools={1: join_tool})
        keys = detect_join_keys(wf)
        assert "customer_id" in keys
        assert "order_date" in keys

    def test_detect_keys_from_unique_tool(self):
        """Unique tool config provides key candidates."""
        unique_tool = AlteryxTool(
            tool_id=2,
            plugin="AlteryxBasePluginsGui.Unique.Unique",
            tool_type="Unique",
            config={"unique_fields": ["account_id", "region"]},
        )
        wf = _make_workflow(tools={2: unique_tool})
        keys = detect_join_keys(wf)
        assert "account_id" in keys
        assert "region" in keys

    def test_detect_keys_heuristic(self):
        """No join/unique tools; output field named order_id detected via heuristic."""
        tool = AlteryxTool(
            tool_id=3,
            plugin="AlteryxBasePluginsGui.Select.Select",
            tool_type="Select",
            config={},
            output_fields=[
                AlteryxField(name="order_id", type="Int64"),
                AlteryxField(name="amount", type="Double"),
                AlteryxField(name="status_code", type="String"),
                AlteryxField(name="primary_key", type="Int64"),
            ],
        )
        wf = _make_workflow(tools={3: tool})
        keys = detect_join_keys(wf)
        assert "order_id" in keys
        assert "status_code" in keys
        assert "primary_key" in keys
        # 'amount' should NOT be detected
        assert "amount" not in keys

    def test_detect_keys_deduplication(self):
        """Duplicate key names across tools are deduplicated."""
        join_tool = AlteryxTool(
            tool_id=1,
            plugin="AlteryxBasePluginsGui.Join.Join",
            tool_type="Join",
            config={"join_fields": [{"left": "id", "right": "id"}]},
        )
        unique_tool = AlteryxTool(
            tool_id=2,
            plugin="AlteryxBasePluginsGui.Unique.Unique",
            tool_type="Unique",
            config={"unique_fields": ["id"]},
        )
        wf = _make_workflow(tools={1: join_tool, 2: unique_tool})
        keys = detect_join_keys(wf)
        assert keys.count("id") == 1

    def test_detect_keys_empty_workflow(self):
        """Empty workflow returns empty list."""
        wf = _make_workflow()
        keys = detect_join_keys(wf)
        assert keys == []

    def test_join_tool_priority_over_heuristic(self):
        """Join tool keys come before heuristic keys, with no duplicates."""
        join_tool = AlteryxTool(
            tool_id=1,
            plugin="AlteryxBasePluginsGui.Join.Join",
            tool_type="Join",
            config={"join_fields": [{"left": "customer_id", "right": "cust_id"}]},
        )
        select_tool = AlteryxTool(
            tool_id=2,
            plugin="AlteryxBasePluginsGui.Select.Select",
            tool_type="Select",
            config={},
            output_fields=[
                AlteryxField(name="customer_id", type="Int64"),
                AlteryxField(name="order_id", type="Int64"),
            ],
        )
        wf = _make_workflow(tools={1: join_tool, 2: select_tool})
        keys = detect_join_keys(wf)
        # customer_id from join comes first, order_id from heuristic
        assert keys[0] == "customer_id"
        assert "order_id" in keys
        assert keys.count("customer_id") == 1


# ── generate_validator_v2 ────────────────────────────────────────────


class TestGenerateValidatorV2:
    def test_generates_validation_notebook(self, tmp_path: Path):
        """File exists and contains row count, schema, datacompy, and detected key."""
        join_tool = AlteryxTool(
            tool_id=1,
            plugin="AlteryxBasePluginsGui.Join.Join",
            tool_type="Join",
            config={"join_fields": [{"left": "account_id", "right": "acct_id"}]},
            output_fields=[
                AlteryxField(name="account_id", type="Int64"),
                AlteryxField(name="balance", type="Double"),
            ],
        )
        output_tool = AlteryxTool(
            tool_id=2,
            plugin="AlteryxBasePluginsGui.OutputData.OutputData",
            tool_type="OutputData",
            config={},
        )
        wf = _make_workflow(
            tools={1: join_tool, 2: output_tool},
            connections=[
                AlteryxConnection(
                    source_tool_id=1,
                    source_anchor="Join",
                    target_tool_id=2,
                    target_anchor="Input",
                )
            ],
        )
        steps = {
            1: GeneratedStep(step_name="join", code="df_1 = ...", output_df="df_1"),
            2: GeneratedStep(step_name="output", code="df_2 = df_1", output_df="df_2"),
        }

        generate_validator_v2(tmp_path, wf, steps, [1, 2])

        out_file = tmp_path / "04_validate.py"
        assert out_file.exists()
        content = out_file.read_text(encoding="utf-8")

        # Databricks notebook marker
        assert "# Databricks notebook source" in content
        # Section markers
        assert "Row Count" in content
        assert "Schema" in content
        assert "datacompy" in content.lower() or "DataComPy" in content
        # Detected key used (not TODO placeholder)
        assert "account_id" in content

    def test_fallback_to_todo_key(self, tmp_path: Path):
        """When no keys detected, falls back to TODO_primary_key."""
        tool = AlteryxTool(
            tool_id=1,
            plugin="AlteryxBasePluginsGui.Select.Select",
            tool_type="Select",
            config={},
            output_fields=[
                AlteryxField(name="name", type="String"),
                AlteryxField(name="amount", type="Double"),
            ],
        )
        wf = _make_workflow(tools={1: tool})
        steps = {
            1: GeneratedStep(step_name="select", code="df_1 = ...", output_df="df_1"),
        }

        generate_validator_v2(tmp_path, wf, steps, [1])

        content = (tmp_path / "04_validate.py").read_text(encoding="utf-8")
        assert "TODO_primary_key" in content

    def test_notebook_has_all_sections(self, tmp_path: Path):
        """Verify all 5 validation sections are present."""
        tool = AlteryxTool(
            tool_id=1,
            plugin="AlteryxBasePluginsGui.OutputData.OutputData",
            tool_type="OutputData",
            config={},
            output_fields=[
                AlteryxField(name="order_id", type="Int64"),
                AlteryxField(name="total", type="Double"),
            ],
        )
        wf = _make_workflow(tools={1: tool})
        steps = {
            1: GeneratedStep(step_name="output", code="df_1 = ...", output_df="df_1"),
        }

        generate_validator_v2(tmp_path, wf, steps, [1])

        content = (tmp_path / "04_validate.py").read_text(encoding="utf-8")

        # All sections present
        assert "Load Alteryx" in content or "baseline" in content.lower()
        assert "Row Count" in content
        assert "Schema" in content
        assert "Aggregate" in content
        assert "DataComPy" in content or "datacompy" in content

    def test_aggregate_checks_on_numeric_columns(self, tmp_path: Path):
        """Aggregate section references numeric columns for sum/min/max."""
        tool = AlteryxTool(
            tool_id=1,
            plugin="AlteryxBasePluginsGui.OutputData.OutputData",
            tool_type="OutputData",
            config={},
            output_fields=[
                AlteryxField(name="id", type="Int64"),
                AlteryxField(name="revenue", type="Double"),
                AlteryxField(name="name", type="String"),
            ],
        )
        wf = _make_workflow(tools={1: tool})
        steps = {
            1: GeneratedStep(step_name="output", code="df_1 = ...", output_df="df_1"),
        }

        generate_validator_v2(tmp_path, wf, steps, [1])

        content = (tmp_path / "04_validate.py").read_text(encoding="utf-8")
        # Numeric columns should appear in aggregate checks
        assert "revenue" in content
