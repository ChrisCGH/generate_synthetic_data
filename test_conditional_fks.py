#!/usr/bin/env python3
"""Unit tests for conditional logical FK feature"""
import unittest
from generate_synthetic_data_utils import (
    parse_fk_condition, 
    evaluate_fk_condition, 
    FKMeta,
    GLOBALS
)
from generate_synthetic_data import load_logical_fks_from_config


class TestParseCondition(unittest.TestCase):
    """Test the parse_fk_condition function"""
    
    def test_parse_simple_equality(self):
        """Test parsing simple equality condition"""
        result = parse_fk_condition("T = 'some_string'")
        self.assertEqual(result['column'], 'T')
        self.assertEqual(result['operator'], '=')
        self.assertEqual(result['value'], 'some_string')
    
    def test_parse_with_spaces(self):
        """Test parsing condition with extra spaces"""
        result = parse_fk_condition("  type  =  'Post'  ")
        self.assertEqual(result['column'], 'type')
        self.assertEqual(result['operator'], '=')
        self.assertEqual(result['value'], 'Post')
    
    def test_parse_underscore_column(self):
        """Test parsing condition with underscore in column name"""
        result = parse_fk_condition("account_type = 'personal'")
        self.assertEqual(result['column'], 'account_type')
        self.assertEqual(result['operator'], '=')
        self.assertEqual(result['value'], 'personal')
    
    def test_parse_empty_value(self):
        """Test parsing condition with empty string value"""
        result = parse_fk_condition("status = ''")
        self.assertEqual(result['column'], 'status')
        self.assertEqual(result['operator'], '=')
        self.assertEqual(result['value'], '')
    
    def test_parse_none(self):
        """Test parsing None condition"""
        result = parse_fk_condition(None)
        self.assertIsNone(result)
    
    def test_parse_empty_string(self):
        """Test parsing empty string condition"""
        result = parse_fk_condition("")
        self.assertIsNone(result)
    
    def test_parse_invalid_format(self):
        """Test parsing invalid format"""
        result = parse_fk_condition("T == 'value'")
        self.assertIsNone(result)
    
    def test_parse_no_quotes(self):
        """Test parsing condition without quotes (should fail)"""
        result = parse_fk_condition("T = value")
        self.assertIsNone(result)


class TestEvaluateCondition(unittest.TestCase):
    """Test the evaluate_fk_condition function"""
    
    def test_evaluate_matching_condition(self):
        """Test evaluating a condition that matches"""
        row = {'T': 'some_string', 'P_ID': None}
        result = evaluate_fk_condition("T = 'some_string'", row)
        self.assertTrue(result)
    
    def test_evaluate_non_matching_condition(self):
        """Test evaluating a condition that doesn't match"""
        row = {'T': 'other_string', 'P_ID': None}
        result = evaluate_fk_condition("T = 'some_string'", row)
        self.assertFalse(result)
    
    def test_evaluate_missing_column(self):
        """Test evaluating when discriminator column is missing"""
        row = {'P_ID': None}
        result = evaluate_fk_condition("T = 'some_string'", row)
        self.assertFalse(result)  # Missing column means condition not met
    
    def test_evaluate_none_condition(self):
        """Test evaluating None condition (unconditional FK)"""
        row = {'T': 'any_value', 'P_ID': None}
        result = evaluate_fk_condition(None, row)
        self.assertTrue(result)
    
    def test_evaluate_empty_condition(self):
        """Test evaluating empty condition (unconditional FK)"""
        row = {'T': 'any_value', 'P_ID': None}
        result = evaluate_fk_condition("", row)
        self.assertTrue(result)


class TestFKMetaWithCondition(unittest.TestCase):
    """Test the extended FKMeta namedtuple"""
    
    def test_fk_meta_with_condition(self):
        """Test creating FKMeta with condition"""
        fk = FKMeta(
            constraint_name="LOGICAL_X_P_ID",
            table_schema="db",
            table_name="X",
            column_name="P_ID",
            referenced_table_schema="db",
            referenced_table_name="W",
            referenced_column_name="ID",
            is_logical=True,
            condition="T = 'some_string'"
        )
        self.assertEqual(fk.condition, "T = 'some_string'")
        self.assertTrue(fk.is_logical)
    
    def test_fk_meta_without_condition(self):
        """Test creating FKMeta without condition (backward compatible)"""
        fk = FKMeta(
            constraint_name="FK_Y_X",
            table_schema="db",
            table_name="Y",
            column_name="X_ID",
            referenced_table_schema="db",
            referenced_table_name="X",
            referenced_column_name="ID",
            is_logical=False,
            condition=None
        )
        self.assertIsNone(fk.condition)
        self.assertFalse(fk.is_logical)


class TestLoadLogicalFKsWithCondition(unittest.TestCase):
    """Test load_logical_fks_from_config with conditional FKs"""
    
    def test_load_conditional_fks(self):
        """Test loading conditional logical FKs from config"""
        config = [
            {
                "schema": "db",
                "table": "X",
                "logical_fks": [
                    {
                        "column": "P_ID",
                        "referenced_schema": "db",
                        "referenced_table": "W",
                        "referenced_column": "ID",
                        "condition": "T = 'some_string'"
                    },
                    {
                        "column": "P_ID",
                        "referenced_schema": "db",
                        "referenced_table": "H",
                        "referenced_column": "ID",
                        "condition": "T = 'some_other_string'"
                    }
                ]
            }
        ]
        
        single_fks, composite_fks = load_logical_fks_from_config(config)
        
        self.assertEqual(len(single_fks), 2)
        self.assertEqual(single_fks[0].column_name, "P_ID")
        self.assertEqual(single_fks[0].referenced_table_name, "W")
        self.assertEqual(single_fks[0].condition, "T = 'some_string'")
        
        self.assertEqual(single_fks[1].column_name, "P_ID")
        self.assertEqual(single_fks[1].referenced_table_name, "H")
        self.assertEqual(single_fks[1].condition, "T = 'some_other_string'")
    
    def test_load_unconditional_fks(self):
        """Test loading unconditional logical FKs (backward compatible)"""
        config = [
            {
                "schema": "db",
                "table": "Child",
                "logical_fks": [
                    {
                        "column": "parent_id",
                        "referenced_schema": "db",
                        "referenced_table": "Parent",
                        "referenced_column": "id"
                    }
                ]
            }
        ]
        
        single_fks, composite_fks = load_logical_fks_from_config(config)
        
        self.assertEqual(len(single_fks), 1)
        self.assertEqual(single_fks[0].column_name, "parent_id")
        self.assertIsNone(single_fks[0].condition)
    
    def test_load_mixed_fks(self):
        """Test loading mix of conditional and unconditional FKs"""
        config = [
            {
                "schema": "db",
                "table": "Mixed",
                "logical_fks": [
                    {
                        "column": "parent_id",
                        "referenced_schema": "db",
                        "referenced_table": "Parent",
                        "referenced_column": "id"
                    },
                    {
                        "column": "poly_id",
                        "referenced_schema": "db",
                        "referenced_table": "TypeA",
                        "referenced_column": "id",
                        "condition": "type = 'A'"
                    },
                    {
                        "column": "poly_id",
                        "referenced_schema": "db",
                        "referenced_table": "TypeB",
                        "referenced_column": "id",
                        "condition": "type = 'B'"
                    }
                ]
            }
        ]
        
        single_fks, composite_fks = load_logical_fks_from_config(config)
        
        self.assertEqual(len(single_fks), 3)
        
        # First FK is unconditional
        self.assertEqual(single_fks[0].column_name, "parent_id")
        self.assertIsNone(single_fks[0].condition)
        
        # Second FK is conditional
        self.assertEqual(single_fks[1].column_name, "poly_id")
        self.assertEqual(single_fks[1].condition, "type = 'A'")
        
        # Third FK is conditional
        self.assertEqual(single_fks[2].column_name, "poly_id")
        self.assertEqual(single_fks[2].condition, "type = 'B'")


class TestCompositeConditionalFKs(unittest.TestCase):
    """Test composite FKs with conditions"""
    
    def test_load_composite_conditional_fks(self):
        """Test loading composite logical FKs with conditions"""
        config = [
            {
                "schema": "db",
                "table": "Child",
                "logical_fks": [
                    {
                        "child_columns": ["parent_schema", "parent_id"],
                        "referenced_schema": "db",
                        "referenced_table": "ParentA",
                        "referenced_columns": ["schema", "id"],
                        "condition": "type = 'A'"
                    }
                ]
            }
        ]
        
        single_fks, composite_fks = load_logical_fks_from_config(config)
        
        self.assertEqual(len(composite_fks), 1)
        self.assertEqual(composite_fks[0]["condition"], "type = 'A'")


if __name__ == '__main__':
    unittest.main()
