#!/usr/bin/env python3
"""Unit tests for extended populate_columns configuration feature"""
import unittest
import random
from datetime import datetime
from generate_synthetic_data_utils import (
    parse_date,
    parse_populate_columns_config,
    validate_populate_column_config,
    generate_value_with_config,
    ColumnMeta,
    GLOBALS
)


class TestParseDate(unittest.TestCase):
    """Test the parse_date function"""
    
    def test_parse_date_yyyy_mm_dd(self):
        """Test parsing YYYY-MM-DD format"""
        result = parse_date("2020-01-15")
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2020)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)
    
    def test_parse_date_with_time(self):
        """Test parsing YYYY-MM-DD HH:MM:SS format"""
        result = parse_date("2020-06-20 14:30:45")
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2020)
        self.assertEqual(result.month, 6)
        self.assertEqual(result.day, 20)
        self.assertEqual(result.hour, 14)
        self.assertEqual(result.minute, 30)
        self.assertEqual(result.second, 45)
    
    def test_parse_date_iso_format(self):
        """Test parsing ISO format with T separator"""
        result = parse_date("2020-12-25T08:00:00")
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2020)
        self.assertEqual(result.month, 12)
        self.assertEqual(result.day, 25)
    
    def test_parse_date_none(self):
        """Test parsing None returns None"""
        result = parse_date(None)
        self.assertIsNone(result)
    
    def test_parse_date_empty(self):
        """Test parsing empty string returns None"""
        result = parse_date("")
        self.assertIsNone(result)
    
    def test_parse_date_invalid(self):
        """Test parsing invalid date format returns None"""
        result = parse_date("not-a-date")
        self.assertIsNone(result)
        
        result = parse_date("01/15/2020")  # Wrong format
        self.assertIsNone(result)


class TestParsePopulateColumnsConfig(unittest.TestCase):
    """Test the parse_populate_columns_config function"""
    
    def test_parse_simple_string_columns(self):
        """Test parsing simple string column names (backward compatible)"""
        table_cfg = {
            "schema": "db",
            "table": "users",
            "populate_columns": ["age", "status", "salary"]
        }
        result = parse_populate_columns_config(table_cfg)
        
        self.assertEqual(len(result), 3)
        self.assertIn("age", result)
        self.assertIn("status", result)
        self.assertIn("salary", result)
        self.assertEqual(result["age"], {"column": "age"})
    
    def test_parse_extended_int_range(self):
        """Test parsing extended format with integer range"""
        table_cfg = {
            "schema": "db",
            "table": "users",
            "populate_columns": [
                {"column": "age", "min": 18, "max": 65}
            ]
        }
        result = parse_populate_columns_config(table_cfg)
        
        self.assertEqual(len(result), 1)
        self.assertIn("age", result)
        self.assertEqual(result["age"]["min"], 18)
        self.assertEqual(result["age"]["max"], 65)
    
    def test_parse_extended_decimal_range(self):
        """Test parsing extended format with decimal range"""
        table_cfg = {
            "schema": "db",
            "table": "employees",
            "populate_columns": [
                {"column": "salary", "min": 30000.00, "max": 150000.00}
            ]
        }
        result = parse_populate_columns_config(table_cfg)
        
        self.assertEqual(len(result), 1)
        self.assertIn("salary", result)
        self.assertEqual(result["salary"]["min"], 30000.00)
        self.assertEqual(result["salary"]["max"], 150000.00)
    
    def test_parse_extended_values_list(self):
        """Test parsing extended format with values list"""
        table_cfg = {
            "schema": "db",
            "table": "orders",
            "populate_columns": [
                {"column": "status", "values": ["active", "pending", "inactive"]}
            ]
        }
        result = parse_populate_columns_config(table_cfg)
        
        self.assertEqual(len(result), 1)
        self.assertIn("status", result)
        self.assertEqual(result["status"]["values"], ["active", "pending", "inactive"])
    
    def test_parse_extended_int_values(self):
        """Test parsing extended format with integer values list"""
        table_cfg = {
            "schema": "db",
            "table": "tasks",
            "populate_columns": [
                {"column": "priority", "values": [1, 2, 3, 4, 5]}
            ]
        }
        result = parse_populate_columns_config(table_cfg)
        
        self.assertEqual(len(result), 1)
        self.assertIn("priority", result)
        self.assertEqual(result["priority"]["values"], [1, 2, 3, 4, 5])
    
    def test_parse_extended_date_range(self):
        """Test parsing extended format with date range"""
        table_cfg = {
            "schema": "db",
            "table": "events",
            "populate_columns": [
                {"column": "created_date", "min": "2020-01-01", "max": "2024-12-31"}
            ]
        }
        result = parse_populate_columns_config(table_cfg)
        
        self.assertEqual(len(result), 1)
        self.assertIn("created_date", result)
        self.assertEqual(result["created_date"]["min"], "2020-01-01")
        self.assertEqual(result["created_date"]["max"], "2024-12-31")
    
    def test_parse_mixed_format(self):
        """Test parsing mixed format (simple and extended)"""
        table_cfg = {
            "schema": "db",
            "table": "users",
            "populate_columns": [
                "simple_column",
                {"column": "age", "min": 18, "max": 65},
                {"column": "status", "values": ["active", "inactive"]}
            ]
        }
        result = parse_populate_columns_config(table_cfg)
        
        self.assertEqual(len(result), 3)
        self.assertIn("simple_column", result)
        self.assertIn("age", result)
        self.assertIn("status", result)
        self.assertEqual(result["simple_column"], {"column": "simple_column"})
        self.assertEqual(result["age"]["min"], 18)
        self.assertEqual(result["status"]["values"], ["active", "inactive"])
    
    def test_parse_empty_populate_columns(self):
        """Test parsing table with no populate_columns"""
        table_cfg = {
            "schema": "db",
            "table": "users"
        }
        result = parse_populate_columns_config(table_cfg)
        
        self.assertEqual(len(result), 0)
    
    def test_parse_missing_column_field(self):
        """Test parsing extended format missing 'column' field (should be skipped with warning)"""
        table_cfg = {
            "schema": "db",
            "table": "users",
            "populate_columns": [
                {"min": 18, "max": 65}  # Missing 'column' field
            ]
        }
        result = parse_populate_columns_config(table_cfg)
        
        self.assertEqual(len(result), 0)


class TestValidatePopulateColumnConfig(unittest.TestCase):
    """Test the validate_populate_column_config function"""
    
    def _make_column(self, name, data_type, is_nullable="YES"):
        """Helper to create ColumnMeta"""
        return ColumnMeta(
            name=name, data_type=data_type, is_nullable=is_nullable,
            column_type=data_type, column_key="", extra="",
            char_max_length=None, numeric_precision=10, numeric_scale=2,
            column_default=None
        )
    
    def test_validate_empty_config(self):
        """Test validation of empty config"""
        col = self._make_column("age", "int")
        result = validate_populate_column_config(col, {})
        self.assertTrue(result)
    
    def test_validate_none_config(self):
        """Test validation of None config"""
        col = self._make_column("age", "int")
        result = validate_populate_column_config(col, None)
        self.assertTrue(result)
    
    def test_validate_valid_int_range(self):
        """Test validation of valid integer range"""
        col = self._make_column("age", "int")
        config = {"column": "age", "min": 18, "max": 65}
        result = validate_populate_column_config(col, config)
        self.assertTrue(result)
    
    def test_validate_invalid_int_range_min_gte_max(self):
        """Test validation of invalid integer range (min >= max)"""
        col = self._make_column("age", "int")
        config = {"column": "age", "min": 65, "max": 18}
        result = validate_populate_column_config(col, config)
        self.assertFalse(result)
    
    def test_validate_valid_decimal_range(self):
        """Test validation of valid decimal range"""
        col = self._make_column("salary", "decimal")
        config = {"column": "salary", "min": 30000.00, "max": 150000.00}
        result = validate_populate_column_config(col, config)
        self.assertTrue(result)
    
    def test_validate_valid_date_range(self):
        """Test validation of valid date range"""
        col = self._make_column("created_date", "date")
        config = {"column": "created_date", "min": "2020-01-01", "max": "2024-12-31"}
        result = validate_populate_column_config(col, config)
        self.assertTrue(result)
    
    def test_validate_invalid_date_range(self):
        """Test validation of invalid date range (min >= max)"""
        col = self._make_column("created_date", "date")
        config = {"column": "created_date", "min": "2024-12-31", "max": "2020-01-01"}
        result = validate_populate_column_config(col, config)
        self.assertFalse(result)
    
    def test_validate_invalid_date_format(self):
        """Test validation of invalid date format"""
        col = self._make_column("created_date", "date")
        config = {"column": "created_date", "min": "invalid-date", "max": "2024-12-31"}
        result = validate_populate_column_config(col, config)
        self.assertFalse(result)


class TestGenerateValueWithConfig(unittest.TestCase):
    """Test the generate_value_with_config function"""
    
    def setUp(self):
        self.rng = random.Random(42)  # Fixed seed for reproducibility
    
    def _make_column(self, name, data_type, is_nullable="YES", column_type=None):
        """Helper to create ColumnMeta"""
        return ColumnMeta(
            name=name, data_type=data_type, is_nullable=is_nullable,
            column_type=column_type or data_type, column_key="", extra="",
            char_max_length=255, numeric_precision=10, numeric_scale=2,
            column_default=None
        )
    
    def test_generate_int_with_range(self):
        """Test generating integer value with range"""
        col = self._make_column("age", "int")
        config = {"column": "age", "min": 18, "max": 65}
        
        # Generate multiple values and ensure they're in range
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIsInstance(value, int)
            self.assertGreaterEqual(value, 18)
            self.assertLessEqual(value, 65)
    
    def test_generate_decimal_with_range(self):
        """Test generating decimal value with range"""
        col = self._make_column("salary", "decimal")
        config = {"column": "salary", "min": 30000.00, "max": 150000.00}
        
        # Generate multiple values and ensure they're in range
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIsInstance(value, float)
            self.assertGreaterEqual(value, 30000.00)
            self.assertLessEqual(value, 150000.00)
    
    def test_generate_float_with_range(self):
        """Test generating float value with range"""
        col = self._make_column("rate", "float")
        config = {"column": "rate", "min": 0.0, "max": 100.0}
        
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIsInstance(value, float)
            self.assertGreaterEqual(value, 0.0)
            self.assertLessEqual(value, 100.0)
    
    def test_generate_with_values_list_strings(self):
        """Test generating value from values list (strings)"""
        col = self._make_column("status", "varchar")
        config = {"column": "status", "values": ["active", "pending", "inactive"]}
        
        # Generate multiple values and ensure they're from the list
        values = set()
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIn(value, ["active", "pending", "inactive"])
            values.add(value)
        
        # Ensure all values are hit at least once (with enough iterations)
        self.assertEqual(values, {"active", "pending", "inactive"})
    
    def test_generate_with_values_list_integers(self):
        """Test generating value from values list (integers)"""
        col = self._make_column("priority", "int")
        config = {"column": "priority", "values": [1, 2, 3, 4, 5]}
        
        values = set()
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIn(value, [1, 2, 3, 4, 5])
            values.add(value)
        
        self.assertEqual(values, {1, 2, 3, 4, 5})
    
    def test_generate_date_with_range(self):
        """Test generating date value with range"""
        col = self._make_column("created_date", "date")
        config = {"column": "created_date", "min": "2020-01-01", "max": "2024-12-31"}
        
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIsInstance(value, str)
            # Parse the generated date to verify it's valid
            parsed = datetime.strptime(value, "%Y-%m-%d")
            self.assertGreaterEqual(parsed, datetime(2020, 1, 1))
            self.assertLessEqual(parsed, datetime(2024, 12, 31))
    
    def test_generate_datetime_with_range(self):
        """Test generating datetime value with range"""
        col = self._make_column("last_login", "datetime")
        config = {"column": "last_login", "min": "2024-01-01 00:00:00", "max": "2024-12-31 23:59:59"}
        
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIsInstance(value, str)
            # Parse the generated datetime to verify it's valid
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            self.assertGreaterEqual(parsed, datetime(2024, 1, 1, 0, 0, 0))
            self.assertLessEqual(parsed, datetime(2024, 12, 31, 23, 59, 59))
    
    def test_generate_timestamp_with_range(self):
        """Test generating timestamp value with range"""
        col = self._make_column("updated_at", "timestamp")
        config = {"column": "updated_at", "min": "2023-06-01", "max": "2023-12-31"}
        
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIsInstance(value, str)
            # Timestamp format includes time
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            self.assertGreaterEqual(parsed.date(), datetime(2023, 6, 1).date())
            self.assertLessEqual(parsed.date(), datetime(2023, 12, 31).date())
    
    def test_generate_int_without_config(self):
        """Test generating integer value without config (default behavior)"""
        col = self._make_column("count", "int")
        
        value = generate_value_with_config(self.rng, col, None)
        self.assertIsInstance(value, int)
    
    def test_generate_age_column_without_config(self):
        """Test generating age column without config (should use 18-80 range)"""
        col = self._make_column("user_age", "int")
        
        for _ in range(100):
            value = generate_value_with_config(self.rng, col, None)
            self.assertIsInstance(value, int)
            self.assertGreaterEqual(value, 18)
            self.assertLessEqual(value, 80)
    
    def test_generate_email_column_without_config(self):
        """Test generating email column without config"""
        col = self._make_column("user_email", "varchar")
        
        value = generate_value_with_config(self.rng, col, None)
        self.assertIsInstance(value, str)
        self.assertIn("@", value)
    
    def test_generate_name_column_without_config(self):
        """Test generating name column without config"""
        col = self._make_column("full_name", "varchar")
        
        value = generate_value_with_config(self.rng, col, None)
        self.assertIsInstance(value, str)
        self.assertIn(" ", value)  # Names should have space between first and last
    
    def test_generate_enum_column(self):
        """Test generating enum column without config"""
        col = self._make_column("status", "enum", column_type="enum('active','pending','deleted')")
        
        value = generate_value_with_config(self.rng, col, None)
        self.assertIn(value, ["active", "pending", "deleted"])
    
    def test_values_takes_precedence_over_range(self):
        """Test that values list takes precedence over min/max range"""
        col = self._make_column("value", "int")
        config = {"column": "value", "values": [100, 200, 300], "min": 1, "max": 10}
        
        # Values should take precedence - all generated values should be from values list
        for _ in range(50):
            value = generate_value_with_config(self.rng, col, config)
            self.assertIn(value, [100, 200, 300])


class TestBackwardCompatibility(unittest.TestCase):
    """Test backward compatibility with simple string column names"""
    
    def test_simple_strings_work(self):
        """Test that simple string column names still work"""
        table_cfg = {
            "schema": "db",
            "table": "users",
            "populate_columns": ["age", "status", "salary"]
        }
        result = parse_populate_columns_config(table_cfg)
        
        # All columns should be present
        self.assertIn("age", result)
        self.assertIn("status", result)
        self.assertIn("salary", result)
        
        # Each should have minimal config (just column name)
        for col_name in ["age", "status", "salary"]:
            self.assertEqual(result[col_name], {"column": col_name})
            # No min/max/values should be present
            self.assertNotIn("min", result[col_name])
            self.assertNotIn("max", result[col_name])
            self.assertNotIn("values", result[col_name])


if __name__ == '__main__':
    unittest.main()
