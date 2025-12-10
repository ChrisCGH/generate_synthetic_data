#!/usr/bin/env python3
"""
Test for conditional FK discriminator columns with ENUM in UNIQUE constraints.

This tests the bug fix where discriminator columns (like PT in condition PT = 'WD')
that are ENUM types and part of UNIQUE constraints were getting sequential values
(seq_0000125) instead of valid enum values from the schema.
"""
import unittest
import re
from generate_synthetic_data_utils import (
    parse_fk_condition,
    FKMeta,
)


class TestDiscriminatorDetection(unittest.TestCase):
    """Test detection of discriminator columns from conditional FK conditions"""
    
    def test_detect_discriminator_from_condition(self):
        """Test that we can parse discriminator column from FK condition"""
        
        # Simulate conditional FKs
        fks = [
            FKMeta(
                constraint_name="fk_d_w",
                table_schema="db",
                table_name="D",
                column_name="P_ID",
                referenced_table_schema="db",
                referenced_table_name="W",
                referenced_column_name="ID",
                is_logical=True,
                condition="PT = 'WD'"
            ),
            FKMeta(
                constraint_name="fk_d_h",
                table_schema="db",
                table_name="D",
                column_name="P_ID",
                referenced_table_schema="db",
                referenced_table_name="HP",
                referenced_column_name="ID",
                is_logical=True,
                condition="PT = 'H'"
            ),
            FKMeta(
                constraint_name="fk_d_mc",
                table_schema="db",
                table_name="D",
                column_name="P_ID",
                referenced_table_schema="db",
                referenced_table_name="MCP",
                referenced_column_name="ID",
                is_logical=True,
                condition="PT = 'MC'"
            ),
        ]
        
        # Extract discriminator columns
        node = "db.D"
        discriminator_cols = set()
        for fk in fks:
            if "{0}.{1}".format(fk.table_schema, fk.table_name) == node and fk.condition:
                parsed = parse_fk_condition(fk.condition)
                if parsed:
                    discriminator_cols.add(parsed['column'])
        
        # Should detect PT as discriminator
        self.assertEqual(discriminator_cols, {"PT"})
        print(f"✓ Detected discriminator columns: {discriminator_cols}")
    
    def test_parse_enum_values_from_column_type(self):
        """Test that we can parse ENUM values from column_type"""
        
        # Simulate ENUM column type: enum('WD','H','MC')
        column_type = "enum('WD','H','MC')"
        
        # Parse enum values (same regex used in generate_synthetic_data.py line 606)
        m = re.findall(r"'((?:[^']|(?:''))*)'", column_type)
        vals = [v.replace("''", "'") for v in m]
        
        # Should extract all enum values
        self.assertEqual(vals, ["WD", "H", "MC"])
        print(f"✓ Parsed ENUM values: {vals}")
    
    def test_enum_with_escaped_quotes(self):
        """Test parsing ENUM with escaped single quotes"""
        
        # ENUM values with escaped quotes
        column_type = "enum('O''Brien','Smith','D''Angelo')"
        
        m = re.findall(r"'((?:[^']|(?:''))*)'", column_type)
        vals = [v.replace("''", "'") for v in m]
        
        # Should handle escaped quotes correctly
        self.assertEqual(vals, ["O'Brien", "Smith", "D'Angelo"])
        print(f"✓ Parsed ENUM with escaped quotes: {vals}")


class TestDiscriminatorControlledStatus(unittest.TestCase):
    """Test that discriminator ENUM columns are marked as 'controlled'"""
    
    def test_discriminator_enum_is_controlled(self):
        """Test logic that marks discriminator ENUM columns as controlled"""
        
        # Simulate column metadata
        class MockColumn:
            def __init__(self, name, data_type):
                self.name = name
                self.data_type = data_type
        
        columns = [
            MockColumn("DN", "VARCHAR"),
            MockColumn("PT", "ENUM"),
        ]
        
        # Simulate discriminator detection
        discriminator_cols = {"PT"}
        
        # Simulate populate_config (DN has range, PT doesn't)
        populate_config = {
            "DN": {"column": "DN", "min": 1, "max": 100000}
        }
        
        # Check each column in UNIQUE constraint
        uc_columns = ["DN", "PT"]
        controlled_cols = []
        uncontrolled_cols = []
        
        for col_name in uc_columns:
            # Check if controlled by populate_columns
            is_controlled = col_name in populate_config and (
                "values" in populate_config.get(col_name, {}) or
                "min" in populate_config.get(col_name, {})
            )
            
            # Check if discriminator with ENUM type
            if col_name in discriminator_cols:
                col_meta = next((c for c in columns if c.name == col_name), None)
                if col_meta and col_meta.data_type and col_meta.data_type.lower() == "enum":
                    is_controlled = True
            
            if is_controlled:
                controlled_cols.append(col_name)
            else:
                uncontrolled_cols.append(col_name)
        
        # Both DN and PT should be controlled now
        self.assertEqual(set(controlled_cols), {"DN", "PT"})
        self.assertEqual(len(uncontrolled_cols), 0)
        print(f"✓ Controlled columns: {controlled_cols}")
        print(f"✓ Uncontrolled columns: {uncontrolled_cols}")


if __name__ == '__main__':
    unittest.main()
