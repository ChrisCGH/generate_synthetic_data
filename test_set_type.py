#!/usr/bin/env python3
"""Unit tests for MySQL SET type column support"""
import unittest
import random
from generate_synthetic_data_utils import (
    generate_value_with_config,
    validate_set_value,
    ColumnMeta
)


class TestValidateSetValue(unittest.TestCase):
    """Test the validate_set_value function"""
    
    def test_validate_empty_set(self):
        """Test validation of empty set value"""
        set_definition = "set('read','write','execute','delete')"
        self.assertTrue(validate_set_value(set_definition, ""))
        self.assertTrue(validate_set_value(set_definition, None))
    
    def test_validate_single_value(self):
        """Test validation of single value"""
        set_definition = "set('read','write','execute','delete')"
        self.assertTrue(validate_set_value(set_definition, "read"))
        self.assertTrue(validate_set_value(set_definition, "write"))
        self.assertTrue(validate_set_value(set_definition, "execute"))
        self.assertTrue(validate_set_value(set_definition, "delete"))
    
    def test_validate_multiple_values(self):
        """Test validation of multiple values"""
        set_definition = "set('read','write','execute','delete')"
        self.assertTrue(validate_set_value(set_definition, "read,write"))
        self.assertTrue(validate_set_value(set_definition, "read,write,execute"))
        self.assertTrue(validate_set_value(set_definition, "read,write,execute,delete"))
        self.assertTrue(validate_set_value(set_definition, "write,delete"))
    
    def test_validate_invalid_single_value(self):
        """Test validation of invalid single value"""
        set_definition = "set('read','write','execute','delete')"
        self.assertFalse(validate_set_value(set_definition, "invalid"))
        self.assertFalse(validate_set_value(set_definition, "admin"))
    
    def test_validate_invalid_mixed_values(self):
        """Test validation of mixed valid and invalid values"""
        set_definition = "set('read','write','execute','delete')"
        self.assertFalse(validate_set_value(set_definition, "read,invalid"))
        self.assertFalse(validate_set_value(set_definition, "admin,write"))
    
    def test_validate_with_escaped_quotes(self):
        """Test validation with escaped quotes in values"""
        set_definition = "set('it''s','won''t','can''t')"
        self.assertTrue(validate_set_value(set_definition, "it's"))
        self.assertTrue(validate_set_value(set_definition, "won't"))
        self.assertTrue(validate_set_value(set_definition, "it's,won't"))
    
    def test_validate_none_definition(self):
        """Test validation with None definition"""
        self.assertTrue(validate_set_value(None, ""))
        self.assertFalse(validate_set_value(None, "value"))
    
    def test_validate_with_whitespace(self):
        """Test validation handles whitespace in values"""
        set_definition = "set('read','write','execute')"
        self.assertTrue(validate_set_value(set_definition, "read, write"))
        self.assertTrue(validate_set_value(set_definition, " read , write "))


class TestGenerateSetValueWithConfig(unittest.TestCase):
    """Test generating SET values with generate_value_with_config"""
    
    def setUp(self):
        self.rng = random.Random(42)  # Fixed seed for reproducibility
    
    def _make_set_column(self, name, set_values):
        """Helper to create SET ColumnMeta"""
        values_str = ",".join("'{0}'".format(v) for v in set_values)
        column_type = "set({0})".format(values_str)
        return ColumnMeta(
            name=name, data_type="set", is_nullable="YES",
            column_type=column_type, column_key="", extra="",
            char_max_length=None, numeric_precision=None, numeric_scale=None,
            column_default=None
        )
    
    def test_generate_set_without_config(self):
        """Test generating SET value without config (random subset)"""
        col = self._make_set_column("permissions", ["read", "write", "execute", "delete"])
        
        generated_values = set()
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, None)
            self.assertIsInstance(value, str)
            generated_values.add(value)
            
            # Validate that all parts are valid
            if value:
                parts = value.split(',')
                for part in parts:
                    self.assertIn(part, ["read", "write", "execute", "delete"])
        
        # Should generate multiple different combinations
        self.assertGreater(len(generated_values), 1)
        # Should include empty set at some point
        self.assertIn('', generated_values)
    
    def test_generate_set_maintains_definition_order(self):
        """Test that generated SET values maintain definition order"""
        col = self._make_set_column("permissions", ["read", "write", "execute", "delete"])
        
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, None)
            if value and ',' in value:
                parts = value.split(',')
                # Check that parts are in definition order
                definition_order = ["read", "write", "execute", "delete"]
                indices = [definition_order.index(p) for p in parts]
                # Indices should be in ascending order
                self.assertEqual(indices, sorted(indices))
    
    def test_generate_set_with_values_config(self):
        """Test generating SET value with specific values config"""
        col = self._make_set_column("permissions", ["read", "write", "execute", "delete"])
        config = {"column": "permissions", "values": ["read", "read,write", ""]}
        
        generated_values = set()
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIn(value, ["read", "read,write", ""])
            generated_values.add(value)
        
        # Should generate all specified values
        self.assertEqual(generated_values, {"read", "read,write", ""})
    
    def test_generate_set_all_values(self):
        """Test that generating SET can produce all values"""
        col = self._make_set_column("permissions", ["read", "write", "execute", "delete"])
        
        found_all_values = False
        for _ in range(500):
            value = generate_value_with_config(self.rng, col, None)
            if value == "read,write,execute,delete":
                found_all_values = True
                break
        
        self.assertTrue(found_all_values, "Should be able to generate all values")
    
    def test_generate_set_empty_definition(self):
        """Test generating SET with empty definition"""
        col = ColumnMeta(
            name="flags", data_type="set", is_nullable="YES",
            column_type="set()", column_key="", extra="",
            char_max_length=None, numeric_precision=None, numeric_scale=None,
            column_default=None
        )
        
        value = generate_value_with_config(self.rng, col, None)
        self.assertEqual(value, '')
    
    def test_generate_set_single_option(self):
        """Test generating SET with single option"""
        col = self._make_set_column("flag", ["enabled"])
        
        values_generated = set()
        for _ in range(50):
            value = generate_value_with_config(self.rng, col, None)
            values_generated.add(value)
        
        # Should generate either empty or the single value
        self.assertTrue(values_generated.issubset({'', 'enabled'}))
    
    def test_generate_set_with_escaped_quotes(self):
        """Test generating SET with escaped quotes in values"""
        col = ColumnMeta(
            name="options", data_type="set", is_nullable="YES",
            column_type="set('it''s','won''t')", column_key="", extra="",
            char_max_length=None, numeric_precision=None, numeric_scale=None,
            column_default=None
        )
        
        values_generated = set()
        for _ in range(50):
            value = generate_value_with_config(self.rng, col, None)
            values_generated.add(value)
        
        # Should handle escaped quotes correctly
        for v in values_generated:
            if v:
                parts = v.split(',')
                for part in parts:
                    self.assertIn(part, ["it's", "won't"])


class TestSetTypeDistribution(unittest.TestCase):
    """Test that SET type generates a good distribution of values"""
    
    def setUp(self):
        self.rng = random.Random(42)
    
    def _make_set_column(self, name, set_values):
        """Helper to create SET ColumnMeta"""
        values_str = ",".join("'{0}'".format(v) for v in set_values)
        column_type = "set({0})".format(values_str)
        return ColumnMeta(
            name=name, data_type="set", is_nullable="YES",
            column_type=column_type, column_key="", extra="",
            char_max_length=None, numeric_precision=None, numeric_scale=None,
            column_default=None
        )
    
    def test_distribution_includes_empty(self):
        """Test that distribution includes empty set"""
        col = self._make_set_column("permissions", ["read", "write", "execute", "delete"])
        
        empty_count = 0
        iterations = 1000
        
        for _ in range(iterations):
            value = generate_value_with_config(self.rng, col, None)
            if value == '':
                empty_count += 1
        
        # Empty set should appear with some frequency
        # With 5 possible sizes (0,1,2,3,4), roughly 20% should be empty
        self.assertGreater(empty_count, iterations * 0.05)
        self.assertLess(empty_count, iterations * 0.40)
    
    def test_distribution_includes_all_single_values(self):
        """Test that all single values appear in distribution"""
        col = self._make_set_column("permissions", ["read", "write", "execute", "delete"])
        
        single_values_seen = set()
        iterations = 1000
        
        for _ in range(iterations):
            value = generate_value_with_config(self.rng, col, None)
            if value and ',' not in value:
                single_values_seen.add(value)
        
        # All single values should eventually appear
        self.assertEqual(single_values_seen, {"read", "write", "execute", "delete"})
    
    def test_variety_of_combinations(self):
        """Test that various combinations are generated"""
        col = self._make_set_column("permissions", ["read", "write", "execute", "delete"])
        
        unique_values = set()
        iterations = 1000
        
        for _ in range(iterations):
            value = generate_value_with_config(self.rng, col, None)
            unique_values.add(value)
        
        # With 4 options, there are 2^4 = 16 possible subsets
        # We should generate at least several different ones
        self.assertGreater(len(unique_values), 10)


class TestSetTypeNullable(unittest.TestCase):
    """Test SET type with nullable columns"""
    
    def setUp(self):
        self.rng = random.Random(42)
    
    def test_not_null_set_column(self):
        """Test NOT NULL SET column generates valid values"""
        col = ColumnMeta(
            name="permissions", data_type="set", is_nullable="NO",
            column_type="set('read','write','execute')", column_key="", extra="",
            char_max_length=None, numeric_precision=None, numeric_scale=None,
            column_default=None
        )
        
        for _ in range(50):
            value = generate_value_with_config(self.rng, col, None)
            # Should return a string (can be empty for SET)
            self.assertIsInstance(value, str)
    
    def test_nullable_set_column(self):
        """Test nullable SET column generates valid values"""
        col = ColumnMeta(
            name="permissions", data_type="set", is_nullable="YES",
            column_type="set('read','write','execute')", column_key="", extra="",
            char_max_length=None, numeric_precision=None, numeric_scale=None,
            column_default=None
        )
        
        for _ in range(50):
            value = generate_value_with_config(self.rng, col, None)
            # Should return a string (can be empty for SET)
            self.assertIsInstance(value, str)


if __name__ == '__main__':
    unittest.main()
