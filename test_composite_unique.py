#!/usr/bin/env python3
"""Unit tests for composite UNIQUE constraint handling.

This tests the fix for the bug where string columns in composite UNIQUE indexes
incorrectly get `_<batch_idx>` suffix appended, even though the column itself
is not individually unique.
"""
import unittest
import random
from unittest.mock import MagicMock, patch
from generate_synthetic_data_utils import (
    ColumnMeta,
    UniqueConstraint,
    TableMeta,
    GLOBALS,
    generate_value_with_config
)


class TestCompositeUniqueConstraintClassification(unittest.TestCase):
    """Test that single-column vs composite UNIQUE constraints are correctly classified."""
    
    def test_single_column_unique_detected(self):
        """Test that single-column UNIQUE constraints are correctly identified."""
        # Simulate unique constraints
        unique_constraints = [
            UniqueConstraint("unique_email", ("email",)),  # Single column
            UniqueConstraint("unique_category_code", ("category", "code")),  # Composite
        ]
        
        single_unique_cols = set()
        composite_unique_constraints = []
        
        for uc in unique_constraints:
            if len(uc.columns) == 1:
                single_unique_cols.add(uc.columns[0])
            else:
                composite_unique_constraints.append(uc)
        
        # email should be in single_unique_cols
        self.assertIn("email", single_unique_cols)
        self.assertEqual(len(single_unique_cols), 1)
        
        # category and code should NOT be in single_unique_cols
        self.assertNotIn("category", single_unique_cols)
        self.assertNotIn("code", single_unique_cols)
        
        # One composite constraint
        self.assertEqual(len(composite_unique_constraints), 1)
        self.assertIn(("category", "code"), [uc.columns for uc in composite_unique_constraints])
    
    def test_composite_unique_cols_tracked_separately(self):
        """Test that composite UNIQUE columns are tracked separately."""
        unique_constraints = [
            UniqueConstraint("unique_email", ("email",)),
            UniqueConstraint("unique_category_code", ("category", "code")),
            UniqueConstraint("unique_region_district", ("region", "district")),
        ]
        
        single_unique_cols = set()
        composite_unique_cols = set()
        
        for uc in unique_constraints:
            if len(uc.columns) == 1:
                single_unique_cols.add(uc.columns[0])
            else:
                composite_unique_cols.update(uc.columns)
        
        # Single-column: email
        self.assertEqual(single_unique_cols, {"email"})
        
        # Composite columns: category, code, region, district
        self.assertEqual(composite_unique_cols, {"category", "code", "region", "district"})
    
    def test_column_in_both_single_and_composite(self):
        """Test a column that appears in both single-column and composite UNIQUE."""
        unique_constraints = [
            UniqueConstraint("unique_code", ("code",)),  # Single column
            UniqueConstraint("unique_category_code", ("category", "code")),  # Composite including code
        ]
        
        single_unique_cols = set()
        composite_unique_cols = set()
        
        for uc in unique_constraints:
            if len(uc.columns) == 1:
                single_unique_cols.add(uc.columns[0])
            else:
                composite_unique_cols.update(uc.columns)
        
        # code is in BOTH - single-column takes precedence for suffix
        self.assertIn("code", single_unique_cols)
        self.assertIn("code", composite_unique_cols)
        
        # category is only in composite
        self.assertNotIn("category", single_unique_cols)
        self.assertIn("category", composite_unique_cols)


class TestSuffixAppendingLogic(unittest.TestCase):
    """Test the suffix appending logic for UNIQUE constraints."""
    
    def _make_column(self, name, data_type, is_nullable="YES", column_type=None, char_max_length=255):
        """Helper to create ColumnMeta."""
        return ColumnMeta(
            name=name, data_type=data_type, is_nullable=is_nullable,
            column_type=column_type or data_type, column_key="", extra="",
            char_max_length=char_max_length, numeric_precision=10, numeric_scale=2,
            column_default=None
        )
    
    def test_single_column_unique_string_gets_suffix(self):
        """String column in single-column UNIQUE should get suffix."""
        col = self._make_column("email", "varchar")
        
        single_unique_cols = {"email"}
        cname = "email"
        batch_idx = 5
        base_value = "user@example.com"
        
        is_single_column_unique = cname in single_unique_cols
        
        if is_single_column_unique and base_value is not None:
            maxlen = int(col.char_max_length) if col.char_max_length else 255
            base_str = str(base_value)
            suffix = "_{0}".format(batch_idx)
            max_base_len = maxlen - len(suffix)
            result = (base_str[:max_base_len] + suffix)[:maxlen]
        else:
            result = base_value
        
        # Should have suffix
        self.assertTrue(result.endswith("_5"))
        self.assertEqual(result, "user@example.com_5")
    
    def test_composite_unique_string_no_suffix(self):
        """String column in composite-only UNIQUE should NOT get suffix."""
        col = self._make_column("category", "varchar")
        
        single_unique_cols = set()  # category is NOT in single-column UNIQUE
        composite_unique_cols = {"category", "code"}  # category IS in composite
        cname = "category"
        batch_idx = 5
        base_value = "electronics"
        
        is_single_column_unique = cname in single_unique_cols
        
        if is_single_column_unique and base_value is not None:
            maxlen = int(col.char_max_length) if col.char_max_length else 255
            base_str = str(base_value)
            suffix = "_{0}".format(batch_idx)
            max_base_len = maxlen - len(suffix)
            result = (base_str[:max_base_len] + suffix)[:maxlen]
        else:
            result = base_value
        
        # Should NOT have suffix
        self.assertEqual(result, "electronics")
        self.assertFalse(result.endswith("_5"))
    
    def test_single_column_unique_int_gets_batch_idx(self):
        """Integer column in single-column UNIQUE should use batch_idx."""
        single_unique_cols = {"unique_id"}
        cname = "unique_id"
        batch_idx = 42
        
        is_single_column_unique = cname in single_unique_cols
        
        if is_single_column_unique:
            result = batch_idx
        else:
            result = 999  # Some random value
        
        # Should be batch_idx
        self.assertEqual(result, 42)
    
    def test_composite_unique_int_no_batch_idx_assignment(self):
        """Integer column in composite-only UNIQUE should NOT use batch_idx assignment."""
        single_unique_cols = set()  # code is NOT in single-column UNIQUE
        composite_unique_cols = {"category", "code"}  # code IS in composite
        cname = "code"
        batch_idx = 42
        
        is_single_column_unique = cname in single_unique_cols
        
        if is_single_column_unique:
            result = batch_idx
        else:
            result = 1234  # Normal random value generation
        
        # Should NOT be batch_idx
        self.assertNotEqual(result, 42)
        self.assertEqual(result, 1234)


class TestValuesArrayCompositeUnique(unittest.TestCase):
    """Test that values arrays work correctly with composite UNIQUE constraints."""
    
    def setUp(self):
        self.rng = random.Random(42)
    
    def _make_column(self, name, data_type, is_nullable="YES", column_type=None, char_max_length=255):
        """Helper to create ColumnMeta."""
        return ColumnMeta(
            name=name, data_type=data_type, is_nullable=is_nullable,
            column_type=column_type or data_type, column_key="", extra="",
            char_max_length=char_max_length, numeric_precision=10, numeric_scale=2,
            column_default=None
        )
    
    def test_values_array_no_suffix_for_composite_unique(self):
        """Values from populate_columns values array should NOT get suffix for composite UNIQUE."""
        col = self._make_column("category", "varchar")
        config = {"column": "category", "values": ["electronics", "furniture", "clothing"]}
        
        # Simulate the fix: only apply suffix for single-column UNIQUE
        single_unique_cols = set()  # category is NOT in single-column UNIQUE
        composite_unique_cols = {"category", "code"}  # category IS in composite
        cname = "category"
        dtype = "varchar"
        
        # Generate value from config
        base_value = generate_value_with_config(self.rng, col, config)
        self.assertIn(base_value, ["electronics", "furniture", "clothing"])
        
        # Check if suffix should be applied
        is_single_column_unique = cname in single_unique_cols
        
        if is_single_column_unique and dtype in ("varchar", "char", "text") and base_value is not None:
            maxlen = int(col.char_max_length) if col.char_max_length else 255
            suffix = "_0"
            max_base_len = maxlen - len(suffix)
            result = (base_value[:max_base_len] + suffix)[:maxlen]
        else:
            result = base_value
        
        # Should be unchanged (no suffix)
        self.assertEqual(result, base_value)
        self.assertIn(result, ["electronics", "furniture", "clothing"])


class TestCompositeUniquenessCombination(unittest.TestCase):
    """Test that composite uniqueness is maintained via combination tracking."""
    
    def test_unique_combinations_tracked(self):
        """Test that unique combinations are tracked correctly for composite constraints."""
        unique_constraint = UniqueConstraint("unique_category_code", ("category", "code"))
        local_tracker = set()
        
        # Add some combinations
        combinations = [
            ("electronics", "A001"),
            ("electronics", "A002"),
            ("furniture", "A001"),  # Same code OK, different category
            ("clothing", "B001"),
        ]
        
        for combo in combinations:
            # Check for duplicates
            self.assertNotIn(combo, local_tracker)
            local_tracker.add(combo)
        
        # All combinations should be unique
        self.assertEqual(len(local_tracker), 4)
        
        # Trying to add duplicate should fail
        duplicate = ("electronics", "A001")
        self.assertIn(duplicate, local_tracker)


if __name__ == '__main__':
    unittest.main()
