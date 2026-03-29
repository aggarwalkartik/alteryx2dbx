"""Tests for the semantic fix registry."""
from __future__ import annotations

from alteryx2dbx.fixes import FIXES, FixResult, apply_fixes


class TestRegistryHasKnownFixes:
    def test_registry_contains_case_insensitive_join(self):
        assert "case_insensitive_join" in FIXES

    def test_registry_contains_null_safe_equality(self):
        assert "null_safe_equality" in FIXES

    def test_registry_contains_numeric_cast(self):
        assert "numeric_cast" in FIXES

    def test_each_fix_has_required_keys(self):
        for fix_id, entry in FIXES.items():
            assert "description" in entry, f"{fix_id} missing description"
            assert "severity" in entry, f"{fix_id} missing severity"
            assert "fn" in entry, f"{fix_id} missing fn"
            assert callable(entry["fn"]), f"{fix_id} fn not callable"


class TestCaseInsensitiveJoinFix:
    def test_lower_added_to_string_join_keys(self):
        code = (
            'df_1["CustomerID"] == df_2["CustID"]'
        )
        context = {
            "tool_type": "Join",
            "join_fields": [{"left": "CustomerID", "right": "CustID"}],
        }
        result = apply_fixes(code, context)
        assert "F.lower" in result.code
        assert "CustomerID" in result.code
        assert "CustID" in result.code

    def test_lower_wraps_col_references(self):
        code = (
            'df_1["Name"] == df_2["Name"]'
        )
        context = {
            "tool_type": "Join",
            "join_fields": [{"left": "Name", "right": "Name"}],
        }
        result = apply_fixes(code, context)
        assert 'F.lower(df_1["Name"])' in result.code
        assert 'F.lower(df_2["Name"])' in result.code


class TestNullSafeEqualityFix:
    def test_eq_replaced_with_eqnullsafe_in_filter(self):
        code = 'F.col("status") == F.lit("active")'
        context = {"tool_type": "Filter"}
        result = apply_fixes(code, context)
        assert "eqNullSafe" in result.code
        assert 'F.col("status").eqNullSafe(F.lit("active"))' in result.code

    def test_eq_replaced_in_formula(self):
        code = 'F.col("x") == F.lit("y")'
        context = {"tool_type": "Formula"}
        result = apply_fixes(code, context)
        assert "eqNullSafe" in result.code

    def test_multiple_equalities_replaced(self):
        code = 'F.col("a") == F.lit("1") & F.col("b") == F.lit("2")'
        context = {"tool_type": "Filter"}
        result = apply_fixes(code, context)
        assert result.code.count("eqNullSafe") == 2


class TestNumericCastFix:
    def test_decimal_cast_added(self):
        code = 'df = df.withColumn("amount", F.col("amount"))'
        context = {
            "tool_type": "Formula",
            "output_fields": [
                {"name": "amount", "type": "FixedDecimal", "size": 10, "scale": 2},
            ],
        }
        result = apply_fixes(code, context)
        assert ".cast(DecimalType(10, 2))" in result.code

    def test_no_cast_for_non_decimal(self):
        code = 'df = df.withColumn("name", F.col("name"))'
        context = {
            "tool_type": "Formula",
            "output_fields": [
                {"name": "name", "type": "V_String", "size": 50, "scale": None},
            ],
        }
        result = apply_fixes(code, context)
        assert "DecimalType" not in result.code


class TestFixReportTracksApplied:
    def test_applied_fixes_populated(self):
        code = 'F.col("x") == F.lit("y")'
        context = {"tool_type": "Filter"}
        result = apply_fixes(code, context)
        assert isinstance(result, FixResult)
        assert len(result.applied_fixes) > 0
        fix_ids = [f["fix_id"] for f in result.applied_fixes]
        assert "null_safe_equality" in fix_ids

    def test_applied_fixes_have_fix_id(self):
        code = 'F.col("x") == F.lit("y")'
        context = {"tool_type": "Filter"}
        result = apply_fixes(code, context)
        for fix in result.applied_fixes:
            assert "fix_id" in fix


class TestNoFixesWhenNotApplicable:
    def test_filter_code_gets_no_join_fixes(self):
        code = 'df = df.filter(F.col("x") > 5)'
        context = {"tool_type": "Filter"}
        result = apply_fixes(code, context)
        fix_ids = [f["fix_id"] for f in result.applied_fixes]
        assert "case_insensitive_join" not in fix_ids

    def test_join_code_gets_no_null_safe_fix(self):
        code = 'df_1["ID"] == df_2["ID"]'
        context = {
            "tool_type": "Join",
            "join_fields": [{"left": "ID", "right": "ID"}],
        }
        result = apply_fixes(code, context)
        fix_ids = [f["fix_id"] for f in result.applied_fixes]
        assert "null_safe_equality" not in fix_ids

    def test_no_fixes_returns_empty_list(self):
        code = "df = spark.table('my_table')"
        context = {"tool_type": "InputData"}
        result = apply_fixes(code, context)
        assert result.applied_fixes == []
        assert result.code == code
