#!/usr/bin/env python3
"""Unit tests for UNIQUE constraint handling with populate_columns"""
import unittest
import random
from generate_synthetic_data_utils import (
    generate_unique_value_pool,
    ColumnMeta,
    GLOBALS
)


class TestGenerateUniqueValuePool(unittest.TestCase):
    """Test the generate_unique_value_pool function"""
    
    def setUp(self):
        self.rng = random.Random(42)  # Fixed seed for reproducibility
    
    def _make_column(self, name, data_type, is_nullable="YES", column_type=None, char_max_length=None):
        """Helper to create ColumnMeta"""
        return ColumnMeta(
            name=name, data_type=data_type, is_nullable=is_nullable,
            column_type=column_type or data_type, column_key="", extra="",
            char_max_length=char_max_length or 255, numeric_precision=10, numeric_scale=2,
            column_default=None
        )
    
    def test_integer_range_unique_values(self):
        """Test generating unique integer values with min/max range"""
        col = self._make_column("unique_code", "int")
        config = {"column": "unique_code", "min": 1000, "max": 9999}
        needed = 100
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should generate requested number of unique values
        self.assertEqual(len(pool), needed)
        # All values should be unique
        self.assertEqual(len(set(pool)), needed)
        # All values should be in range
        for val in pool:
            self.assertGreaterEqual(val, 1000)
            self.assertLessEqual(val, 9999)
    
    def test_integer_range_exact_size(self):
        """Test generating unique values when range equals needed count"""
        col = self._make_column("code", "int")
        config = {"column": "code", "min": 1, "max": 10}
        needed = 10
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should get exactly 10 unique values
        self.assertEqual(len(pool), 10)
        self.assertEqual(set(pool), set(range(1, 11)))
    
    def test_integer_range_insufficient(self):
        """Test warning when range has fewer values than needed"""
        col = self._make_column("code", "int")
        config = {"column": "code", "min": 1, "max": 5}
        needed = 10
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should only get 5 values (range size)
        self.assertEqual(len(pool), 5)
        self.assertEqual(set(pool), set(range(1, 6)))
    
    def test_values_list_unique(self):
        """Test generating unique values from a values list"""
        col = self._make_column("status", "varchar")
        config = {"column": "status", "values": ["A", "B", "C", "D", "E"]}
        needed = 5
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        self.assertEqual(len(pool), 5)
        self.assertEqual(set(pool), {"A", "B", "C", "D", "E"})
    
    def test_values_list_insufficient(self):
        """Test when values list has fewer items than needed"""
        col = self._make_column("status", "varchar")
        config = {"column": "status", "values": ["A", "B", "C"]}
        needed = 5
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should only get 3 values (list size)
        self.assertEqual(len(pool), 3)
        self.assertEqual(set(pool), {"A", "B", "C"})
    
    def test_decimal_range_unique_values(self):
        """Test generating unique decimal/float values"""
        col = self._make_column("price", "decimal")
        config = {"column": "price", "min": 10.00, "max": 1000.00}
        needed = 50
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should generate requested number of unique values
        self.assertEqual(len(pool), needed)
        # All values should be unique
        self.assertEqual(len(set(pool)), needed)
        # All values should be in range
        for val in pool:
            self.assertGreaterEqual(val, 10.00)
            self.assertLessEqual(val, 1000.00)
    
    def test_date_range_unique_values(self):
        """Test generating unique date values"""
        col = self._make_column("created_date", "date")
        config = {"column": "created_date", "min": "2024-01-01", "max": "2024-12-31"}
        needed = 100
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should generate requested number of unique values
        self.assertEqual(len(pool), needed)
        # All values should be unique
        self.assertEqual(len(set(pool)), needed)
        # All values should be valid date strings
        for val in pool:
            self.assertRegex(val, r"^\d{4}-\d{2}-\d{2}$")
    
    def test_datetime_range_unique_values(self):
        """Test generating unique datetime values"""
        col = self._make_column("created_at", "datetime")
        # Use a larger range to ensure enough unique days
        config = {"column": "created_at", "min": "2024-01-01", "max": "2024-03-31"}
        needed = 50
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should generate requested number of unique values
        self.assertEqual(len(pool), needed)
        # All values should be unique
        self.assertEqual(len(set(pool)), needed)
        # All values should be valid datetime strings
        for val in pool:
            self.assertRegex(val, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
    
    def test_string_unique_values(self):
        """Test generating unique string values"""
        col = self._make_column("code", "varchar", char_max_length=20)
        config = {"column": "code"}  # No values or min/max, just need unique strings
        needed = 50
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should generate unique values
        self.assertEqual(len(pool), needed)
        self.assertEqual(len(set(pool)), needed)
    
    def test_large_integer_range_sampling(self):
        """Test that large ranges use sampling instead of generating all values"""
        col = self._make_column("big_code", "bigint")
        config = {"column": "big_code", "min": 1, "max": 10000000}
        needed = 100
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should generate requested number of unique values
        self.assertEqual(len(pool), needed)
        self.assertEqual(len(set(pool)), needed)
        # All values should be in range
        for val in pool:
            self.assertGreaterEqual(val, 1)
            self.assertLessEqual(val, 10000000)
    
    def test_shuffled_output(self):
        """Test that output is shuffled (not in order)"""
        col = self._make_column("code", "int")
        config = {"column": "code", "min": 1, "max": 100}
        needed = 100
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        # Should be shuffled, not in sorted order
        self.assertNotEqual(pool, sorted(pool))


class TestUniqueConstraintIntegration(unittest.TestCase):
    """Test integration of unique constraint handling with generate_batch_fast"""
    
    def test_unique_values_no_duplicates(self):
        """Integration test: ensure no duplicates in generated values for UNIQUE columns"""
        # This is a conceptual test - actual integration would require mock database
        rng = random.Random(42)
        
        col = ColumnMeta(
            name="unique_code", data_type="int", is_nullable="NO",
            column_type="int", column_key="UNI", extra="",
            char_max_length=None, numeric_precision=10, numeric_scale=0,
            column_default=None
        )
        config = {"column": "unique_code", "min": 1000, "max": 9999}
        
        # Simulate generating for 500 rows
        needed = 500
        pool = generate_unique_value_pool(col, config, needed, rng)
        
        # All values must be unique
        self.assertEqual(len(pool), needed)
        self.assertEqual(len(set(pool)), needed)
        
        # Simulate assigning values to rows
        assigned_values = []
        for i in range(needed):
            assigned_values.append(pool[i])
        
        # All assigned values must be unique
        self.assertEqual(len(assigned_values), len(set(assigned_values)))


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions"""
    
    def setUp(self):
        self.rng = random.Random(42)
    
    def _make_column(self, name, data_type, **kwargs):
        """Helper to create ColumnMeta"""
        return ColumnMeta(
            name=name, data_type=data_type, is_nullable="YES",
            column_type=kwargs.get("column_type", data_type),
            column_key="", extra="",
            char_max_length=kwargs.get("char_max_length", 255),
            numeric_precision=kwargs.get("numeric_precision", 10),
            numeric_scale=kwargs.get("numeric_scale", 2),
            column_default=None
        )
    
    def test_zero_needed(self):
        """Test generating 0 values"""
        col = self._make_column("code", "int")
        config = {"column": "code", "min": 1, "max": 100}
        
        pool = generate_unique_value_pool(col, config, 0, self.rng)
        
        self.assertEqual(len(pool), 0)
    
    def test_single_value_range(self):
        """Test range with only one value (min == max)"""
        col = self._make_column("code", "int")
        config = {"column": "code", "min": 42, "max": 42}
        
        pool = generate_unique_value_pool(col, config, 1, self.rng)
        
        self.assertEqual(len(pool), 1)
        self.assertEqual(pool[0], 42)
    
    def test_float_type(self):
        """Test float column type"""
        col = self._make_column("rate", "float")
        config = {"column": "rate", "min": 0.0, "max": 100.0}
        needed = 50
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        self.assertEqual(len(pool), needed)
        self.assertEqual(len(set(pool)), needed)
    
    def test_double_type(self):
        """Test double column type"""
        col = self._make_column("amount", "double")
        config = {"column": "amount", "min": 0.0, "max": 1000.0}
        needed = 50
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        self.assertEqual(len(pool), needed)
        self.assertEqual(len(set(pool)), needed)
    
    def test_timestamp_type(self):
        """Test timestamp column type"""
        col = self._make_column("updated_at", "timestamp")
        config = {"column": "updated_at", "min": "2024-01-01", "max": "2024-12-31"}
        needed = 100
        
        pool = generate_unique_value_pool(col, config, needed, self.rng)
        
        self.assertEqual(len(pool), needed)
        self.assertEqual(len(set(pool)), needed)


class TestCrossBatchUniqueness(unittest.TestCase):
    """Test that unique values are maintained across multiple batches"""
    
    def setUp(self):
        self.rng = random.Random(42)
    
    def _make_column(self, name, data_type, **kwargs):
        """Helper to create ColumnMeta"""
        return ColumnMeta(
            name=name, data_type=data_type, is_nullable="YES",
            column_type=kwargs.get("column_type", data_type),
            column_key="", extra="",
            char_max_length=kwargs.get("char_max_length", 255),
            numeric_precision=kwargs.get("numeric_precision", 10),
            numeric_scale=kwargs.get("numeric_scale", 2),
            column_default=None
        )
    
    def test_global_pool_prevents_cross_batch_duplicates(self):
        """Test that a single global pool used across batches produces no duplicates"""
        col = self._make_column("WID", "int")
        config = {"column": "WID", "min": 10000, "max": 100000}
        
        # Simulate total rows needed across all batches
        total_rows = 5000
        
        # Generate one pool for the entire table (like the fix does)
        global_pool = generate_unique_value_pool(col, config, total_rows, self.rng)
        
        # All values should be unique
        self.assertEqual(len(global_pool), total_rows)
        self.assertEqual(len(set(global_pool)), total_rows)
        
        # Simulate extracting values for different batches
        batch_size = 1000
        all_values_used = []
        
        for batch_num in range(5):
            start_idx = batch_num * batch_size
            end_idx = start_idx + batch_size
            batch_values = global_pool[start_idx:end_idx]
            all_values_used.extend(batch_values)
        
        # All values across all batches should be unique
        self.assertEqual(len(all_values_used), total_rows)
        self.assertEqual(len(set(all_values_used)), total_rows)
    
    def test_per_batch_pools_cause_duplicates(self):
        """Demonstrate that per-batch pools would cause duplicates (the bug)"""
        col = self._make_column("WID", "int")
        config = {"column": "WID", "min": 10000, "max": 10100}  # Small range to force overlaps
        
        batch_size = 50
        num_batches = 3
        
        # Simulate per-batch pool generation (the buggy behavior)
        all_values = []
        for batch_num in range(num_batches):
            # Each batch creates its OWN rng and pool
            batch_rng = random.Random(42 + batch_num)  # Different seed per batch
            batch_pool = generate_unique_value_pool(col, config, batch_size, batch_rng)
            all_values.extend(batch_pool)
        
        # With only 101 possible values and 150 values needed across batches,
        # duplicates are likely when using per-batch pools
        total_values = len(all_values)
        unique_values = len(set(all_values))
        
        # This demonstrates the bug: per-batch pools can cause duplicates
        # (Note: This test shows the problem, not the solution)
        self.assertEqual(total_values, 150)
        # With per-batch pools, unique values < total values (duplicates exist)
        self.assertLess(unique_values, total_values)
    
    def test_global_pool_with_concurrent_access(self):
        """Test that global pool works correctly with simulated concurrent access"""
        import threading
        
        col = self._make_column("code", "int")
        config = {"column": "code", "min": 1, "max": 1000}
        total_rows = 500
        
        # Generate the global pool
        global_pool = generate_unique_value_pool(col, config, total_rows, self.rng)
        
        # Simulate thread-safe cursor (like the fix implements)
        cursor = [0]  # Use list to allow mutation in nested function
        lock = threading.Lock()
        extracted_values = []
        
        def extract_values(count):
            """Extract values from pool with thread-safe cursor"""
            batch_values = []
            for _ in range(count):
                with lock:
                    if cursor[0] < len(global_pool):
                        batch_values.append(global_pool[cursor[0]])
                        cursor[0] += 1
            return batch_values
        
        # Simulate 5 threads each extracting 100 values
        threads = []
        results = [None] * 5
        
        def worker(thread_id):
            results[thread_id] = extract_values(100)
        
        for i in range(5):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Combine all extracted values
        for result in results:
            extracted_values.extend(result)
        
        # All extracted values should be unique
        self.assertEqual(len(extracted_values), 500)
        self.assertEqual(len(set(extracted_values)), 500)


if __name__ == '__main__':
    unittest.main()
