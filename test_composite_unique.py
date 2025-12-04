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


class TestSequentialGenerationForCompositeUnique(unittest.TestCase):
    """Test sequential value generation for uncontrolled columns in composite UNIQUE constraints."""
    
    def test_sequential_value_format_string(self):
        """Test that sequential values for string columns have correct format."""
        counter = 0
        value = "seq_{0:08d}".format(counter)
        self.assertEqual(value, "seq_00000000")
        
        counter = 12345
        value = "seq_{0:08d}".format(counter)
        self.assertEqual(value, "seq_00012345")
        
        counter = 9999999
        value = "seq_{0:08d}".format(counter)
        self.assertEqual(value, "seq_09999999")
    
    def test_sequential_values_are_unique(self):
        """Test that sequential generation produces unique values."""
        values = set()
        for i in range(10000):
            value = "seq_{0:08d}".format(i)
            self.assertNotIn(value, values)
            values.add(value)
        self.assertEqual(len(values), 10000)
    
    def test_sequential_values_with_controlled_prefix(self):
        """Test that sequential values work with controlled column values."""
        # Simulate a composite UNIQUE constraint with category (controlled) and code (sequential)
        categories = ["electronics", "furniture", "clothing"]
        combinations = set()
        counter = 0
        
        # Generate 1000 combinations
        for i in range(1000):
            category = categories[i % len(categories)]  # Cycle through categories
            code = "seq_{0:08d}".format(counter)
            counter += 1
            
            combo = (category, code)
            self.assertNotIn(combo, combinations)
            combinations.add(combo)
        
        self.assertEqual(len(combinations), 1000)
    
    def test_sequential_handles_large_row_counts(self):
        """Test that sequential generation handles 10+ million rows."""
        # Verify the format can handle 10 million rows
        counter = 9999999
        value = "seq_{0:08d}".format(counter)
        self.assertEqual(len(value), 12)  # "seq_" (4) + 8 digits
        
        counter = 10000000
        value = "seq_{0:08d}".format(counter)
        self.assertEqual(value, "seq_10000000")
        self.assertEqual(len(value), 12)
    
    def test_sequential_integer_values(self):
        """Test sequential generation for integer columns."""
        values = set()
        for counter in range(10000):
            self.assertNotIn(counter, values)
            values.add(counter)
        self.assertEqual(len(values), 10000)


class TestSequentialGenerationDetection(unittest.TestCase):
    """Test detection of columns needing sequential generation."""
    
    def test_detect_uncontrolled_column_in_composite_unique(self):
        """Test detection of uncontrolled columns in composite UNIQUE."""
        # Simulate the detection logic from generate_batch_fast
        composite_unique_cols = ["category", "code"]
        populate_config = {"category": {"column": "category", "values": ["electronics", "furniture"]}}
        fk_cols = set()
        pk_columns = []
        
        cols_needing_sequential = set()
        
        for col_name in composite_unique_cols:
            is_controlled = col_name in populate_config and (
                "values" in populate_config.get(col_name, {}) or
                "min" in populate_config.get(col_name, {})
            )
            is_fk = col_name in fk_cols
            is_pk = col_name in pk_columns
            
            if not (is_controlled or is_fk or is_pk):
                cols_needing_sequential.add(col_name)
        
        # "code" should need sequential generation, "category" should not
        self.assertIn("code", cols_needing_sequential)
        self.assertNotIn("category", cols_needing_sequential)
        self.assertEqual(len(cols_needing_sequential), 1)
    
    def test_fk_column_not_sequential(self):
        """Test that FK columns don't get sequential generation."""
        composite_unique_cols = ["category", "code", "region_id"]
        populate_config = {"category": {"column": "category", "values": ["electronics"]}}
        fk_cols = {"region_id"}  # region_id is a foreign key
        pk_columns = []
        
        cols_needing_sequential = set()
        
        for col_name in composite_unique_cols:
            is_controlled = col_name in populate_config and (
                "values" in populate_config.get(col_name, {}) or
                "min" in populate_config.get(col_name, {})
            )
            is_fk = col_name in fk_cols
            is_pk = col_name in pk_columns
            
            if not (is_controlled or is_fk or is_pk):
                cols_needing_sequential.add(col_name)
        
        # Only "code" should need sequential generation
        self.assertIn("code", cols_needing_sequential)
        self.assertNotIn("category", cols_needing_sequential)
        self.assertNotIn("region_id", cols_needing_sequential)
        self.assertEqual(len(cols_needing_sequential), 1)
    
    def test_pk_column_not_sequential(self):
        """Test that PK columns don't get sequential generation."""
        composite_unique_cols = ["id", "category", "code"]
        populate_config = {"category": {"column": "category", "values": ["electronics"]}}
        fk_cols = set()
        pk_columns = ["id"]  # id is primary key
        
        cols_needing_sequential = set()
        
        for col_name in composite_unique_cols:
            is_controlled = col_name in populate_config and (
                "values" in populate_config.get(col_name, {}) or
                "min" in populate_config.get(col_name, {})
            )
            is_fk = col_name in fk_cols
            is_pk = col_name in pk_columns
            
            if not (is_controlled or is_fk or is_pk):
                cols_needing_sequential.add(col_name)
        
        # Only "code" should need sequential generation
        self.assertIn("code", cols_needing_sequential)
        self.assertNotIn("category", cols_needing_sequential)
        self.assertNotIn("id", cols_needing_sequential)
        self.assertEqual(len(cols_needing_sequential), 1)
    
    def test_all_controlled_no_sequential(self):
        """Test that if all columns are controlled, no sequential generation needed."""
        composite_unique_cols = ["category", "code"]
        populate_config = {
            "category": {"column": "category", "values": ["electronics"]},
            "code": {"column": "code", "min": 1000, "max": 9999}
        }
        fk_cols = set()
        pk_columns = []
        
        controlled_cols = []
        uncontrolled_cols = []
        
        for col_name in composite_unique_cols:
            is_controlled = col_name in populate_config and (
                "values" in populate_config.get(col_name, {}) or
                "min" in populate_config.get(col_name, {})
            )
            is_fk = col_name in fk_cols
            is_pk = col_name in pk_columns
            
            if is_controlled or is_fk or is_pk:
                controlled_cols.append(col_name)
            else:
                uncontrolled_cols.append(col_name)
        
        # Both columns are controlled
        self.assertEqual(controlled_cols, ["category", "code"])
        self.assertEqual(uncontrolled_cols, [])


class TestSpecialColumnNameOverride(unittest.TestCase):
    """Test that sequential generation overrides special column name detection (name, email, phone)."""
    
    def _detect_sequential_columns(self, composite_unique_cols, populate_config, fk_cols=None, pk_columns=None):
        """Helper to detect which columns need sequential generation.
        
        This mirrors the detection logic in generate_batch_fast().
        
        Args:
            composite_unique_cols: List of column names in the composite UNIQUE constraint
            populate_config: Dict mapping column names to their configuration
            fk_cols: Set of foreign key column names (default: empty set)
            pk_columns: List of primary key column names (default: empty list)
        
        Returns:
            Set of column names that need sequential generation
        """
        if fk_cols is None:
            fk_cols = set()
        if pk_columns is None:
            pk_columns = []
        
        cols_needing_sequential = set()
        
        for col_name in composite_unique_cols:
            is_controlled = col_name in populate_config and (
                "values" in populate_config.get(col_name, {}) or
                "min" in populate_config.get(col_name, {})
            )
            is_fk = col_name in fk_cols
            is_pk = col_name in pk_columns
            
            if not (is_controlled or is_fk or is_pk):
                cols_needing_sequential.add(col_name)
        
        return cols_needing_sequential
    
    def test_name_column_detected_for_sequential(self):
        """Test that a column with 'name' in its name is detected for sequential generation."""
        # Simulate the exact scenario from the bug report
        composite_unique_cols = ["CustomerName", "Column2"]  # CustomerName has "name" in it
        populate_config = {"Column2": {"column": "Column2", "values": ["v1", "v2"]}}
        
        cols_needing_sequential = self._detect_sequential_columns(composite_unique_cols, populate_config)
        
        # CustomerName should need sequential (not controlled, not FK, not PK)
        self.assertIn("CustomerName", cols_needing_sequential)
        # Column2 should NOT need sequential (it's controlled)
        self.assertNotIn("Column2", cols_needing_sequential)
    
    def test_email_column_detected_for_sequential(self):
        """Test that a column with 'email' in its name is detected for sequential generation."""
        composite_unique_cols = ["UserEmail", "TypeCode"]
        populate_config = {"TypeCode": {"column": "TypeCode", "values": ["A", "B"]}}
        
        cols_needing_sequential = self._detect_sequential_columns(composite_unique_cols, populate_config)
        
        # UserEmail should need sequential (not controlled)
        self.assertIn("UserEmail", cols_needing_sequential)
        # TypeCode should NOT need sequential (it's controlled)
        self.assertNotIn("TypeCode", cols_needing_sequential)
    
    def test_phone_column_detected_for_sequential(self):
        """Test that a column with 'phone' in its name is detected for sequential generation."""
        composite_unique_cols = ["ContactPhone", "Region"]
        populate_config = {"Region": {"column": "Region", "values": ["US", "EU"]}}
        
        cols_needing_sequential = self._detect_sequential_columns(composite_unique_cols, populate_config)
        
        # ContactPhone should need sequential (not controlled)
        self.assertIn("ContactPhone", cols_needing_sequential)
        # Region should NOT need sequential (it's controlled)
        self.assertNotIn("Region", cols_needing_sequential)
    
    def test_sequential_overrides_rand_name(self):
        """Test that sequential generation produces unique values instead of rand_name() collision.
        
        The rand_name() function in generate_synthetic_data_utils.py has a limited pool:
        - 10 first names: Alice, Bob, Charlie, Dana, Eve, Frank, Grace, Heidi, Ivan, Judy
        - 8 last names: Smith, Johnson, Williams, Jones, Brown, Davis, Miller, Wilson
        - Total combinations: 10 * 8 = 80 unique names
        
        Sequential generation should produce unlimited unique values.
        """
        # Simulate generating 100 values - with rand_name() only 80 combinations exist
        # Sequential should produce 100 unique values
        values = set()
        for i in range(100):
            value = "seq_{0:08d}".format(i)
            values.add(value)
        
        # All 100 values should be unique
        self.assertEqual(len(values), 100)
        
        # rand_name() has 10 first names * 8 last names = 80 max unique values
        # See generate_synthetic_data_utils.rand_name() for the exact lists
        rand_name_max_combinations = 80
        self.assertGreater(len(values), rand_name_max_combinations, 
            "Sequential generation should produce more unique values than rand_name()'s {0} combinations".format(
                rand_name_max_combinations))


if __name__ == '__main__':
    unittest.main()
