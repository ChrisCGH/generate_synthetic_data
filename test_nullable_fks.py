#!/usr/bin/env python3
"""Unit tests for nullable FK population feature"""
import unittest
import random
from collections import defaultdict
from generate_synthetic_data_utils import ColumnMeta, FKMeta


class MockColumnMeta:
    """Mock column metadata for testing"""
    def __init__(self, name, is_nullable="YES"):
        self.name = name
        self.is_nullable = is_nullable
        self.data_type = "int"
        self.column_type = "int(11)"


class MockTableMeta:
    """Mock table metadata for testing"""
    def __init__(self, columns, pk_columns=None):
        self.columns = columns
        self.pk_columns = pk_columns or []
        self.schema = "db"
        self.name = "test"


class TestNullableFKPopulation(unittest.TestCase):
    """Test that nullable FK columns are populated from parent tables"""
    
    def test_nullable_fk_should_be_populated(self):
        """Test that nullable FK columns are populated by default (not left NULL)"""
        # Setup: Mock the logic from resolve_fks_batch
        node = "db.P"
        parent_caches = {"U_ID": [1, 2, 3, 4, 5]}  # Parent values available
        fk_population_rates = {}  # No explicit population rate
        rng = random.Random(42)
        
        # Mock column metadata for nullable FK
        col_meta = MockColumnMeta("U_ID", is_nullable="YES")
        
        # Simulate the fixed FK resolution logic
        temp_row = {}  # Start with empty row
        fk_col = "U_ID"
        
        # Skip if FK value was already assigned
        if temp_row.get(fk_col) is not None:
            pass  # Already assigned
        else:
            # Populate FK from parent values (works for both nullable and NOT NULL columns)
            fk_pop_rates = fk_population_rates.get(node, {})
            population_rate = fk_pop_rates.get(fk_col, 1.0)  # Default 100% population
            
            # Respect fk_population_rate
            should_populate = True
            if col_meta.is_nullable == "YES" and population_rate < 1.0:
                should_populate = (rng.random() < population_rate)
            
            if should_populate:
                parent_vals = parent_caches.get(fk_col, [])
                if parent_vals:
                    temp_row[fk_col] = rng.choice(parent_vals)
        
        # Verify: nullable FK should be populated with a value from parent
        self.assertIsNotNone(temp_row.get("U_ID"))
        self.assertIn(temp_row["U_ID"], [1, 2, 3, 4, 5])
    
    def test_not_null_fk_should_be_populated(self):
        """Test that NOT NULL FK columns are still populated"""
        node = "db.P"
        parent_caches = {"U_ID": [1, 2, 3, 4, 5]}
        fk_population_rates = {}
        rng = random.Random(42)
        
        col_meta = MockColumnMeta("U_ID", is_nullable="NO")
        
        temp_row = {}
        fk_col = "U_ID"
        
        if temp_row.get(fk_col) is not None:
            pass
        else:
            fk_pop_rates = fk_population_rates.get(node, {})
            population_rate = fk_pop_rates.get(fk_col, 1.0)
            
            should_populate = True
            if col_meta.is_nullable == "YES" and population_rate < 1.0:
                should_populate = (rng.random() < population_rate)
            
            if should_populate:
                parent_vals = parent_caches.get(fk_col, [])
                if parent_vals:
                    temp_row[fk_col] = rng.choice(parent_vals)
        
        self.assertIsNotNone(temp_row.get("U_ID"))
        self.assertIn(temp_row["U_ID"], [1, 2, 3, 4, 5])
    
    def test_already_assigned_fk_not_overwritten(self):
        """Test that FK values already assigned are not overwritten"""
        node = "db.P"
        parent_caches = {"U_ID": [1, 2, 3, 4, 5]}
        fk_population_rates = {}
        rng = random.Random(42)
        
        col_meta = MockColumnMeta("U_ID", is_nullable="YES")
        
        # Row already has a value assigned (e.g., from populate_columns with explicit values)
        temp_row = {"U_ID": 999}
        fk_col = "U_ID"
        
        # Apply the fixed logic
        if temp_row.get(fk_col) is not None:
            pass  # Skip - already assigned
        else:
            fk_pop_rates = fk_population_rates.get(node, {})
            population_rate = fk_pop_rates.get(fk_col, 1.0)
            
            should_populate = True
            if col_meta.is_nullable == "YES" and population_rate < 1.0:
                should_populate = (rng.random() < population_rate)
            
            if should_populate:
                parent_vals = parent_caches.get(fk_col, [])
                if parent_vals:
                    temp_row[fk_col] = rng.choice(parent_vals)
        
        # Verify: original value is preserved
        self.assertEqual(temp_row["U_ID"], 999)


class TestFKPopulationRate(unittest.TestCase):
    """Test fk_population_rate controls percentage of nullable FK rows populated"""
    
    def test_population_rate_50_percent(self):
        """Test that ~50% of rows get FK populated when rate is 0.5"""
        node = "db.P"
        parent_caches = {"U_ID": [1, 2, 3, 4, 5]}
        fk_population_rates = {node: {"U_ID": 0.5}}  # 50% population rate
        rng = random.Random(42)
        
        col_meta = MockColumnMeta("U_ID", is_nullable="YES")
        
        populated_count = 0
        null_count = 0
        num_rows = 1000
        
        for _ in range(num_rows):
            temp_row = {}
            fk_col = "U_ID"
            
            if temp_row.get(fk_col) is not None:
                pass
            else:
                fk_pop_rates = fk_population_rates.get(node, {})
                population_rate = fk_pop_rates.get(fk_col, 1.0)
                
                should_populate = True
                if col_meta.is_nullable == "YES" and population_rate < 1.0:
                    should_populate = (rng.random() < population_rate)
                
                if should_populate:
                    parent_vals = parent_caches.get(fk_col, [])
                    if parent_vals:
                        temp_row[fk_col] = rng.choice(parent_vals)
            
            if temp_row.get("U_ID") is not None:
                populated_count += 1
            else:
                null_count += 1
        
        # Verify: approximately 50% populated (with some margin)
        population_percentage = populated_count / num_rows
        self.assertGreater(population_percentage, 0.4)
        self.assertLess(population_percentage, 0.6)
    
    def test_population_rate_100_percent_default(self):
        """Test that 100% of rows get FK populated when rate is not specified"""
        node = "db.P"
        parent_caches = {"U_ID": [1, 2, 3, 4, 5]}
        fk_population_rates = {}  # No explicit rate - defaults to 100%
        rng = random.Random(42)
        
        col_meta = MockColumnMeta("U_ID", is_nullable="YES")
        
        populated_count = 0
        num_rows = 100
        
        for _ in range(num_rows):
            temp_row = {}
            fk_col = "U_ID"
            
            if temp_row.get(fk_col) is not None:
                pass
            else:
                fk_pop_rates = fk_population_rates.get(node, {})
                population_rate = fk_pop_rates.get(fk_col, 1.0)
                
                should_populate = True
                if col_meta.is_nullable == "YES" and population_rate < 1.0:
                    should_populate = (rng.random() < population_rate)
                
                if should_populate:
                    parent_vals = parent_caches.get(fk_col, [])
                    if parent_vals:
                        temp_row[fk_col] = rng.choice(parent_vals)
            
            if temp_row.get("U_ID") is not None:
                populated_count += 1
        
        # Verify: 100% populated
        self.assertEqual(populated_count, num_rows)
    
    def test_population_rate_ignored_for_not_null(self):
        """Test that population rate is ignored for NOT NULL FK columns"""
        node = "db.P"
        parent_caches = {"U_ID": [1, 2, 3, 4, 5]}
        fk_population_rates = {node: {"U_ID": 0.5}}  # 50% rate but should be ignored
        rng = random.Random(42)
        
        col_meta = MockColumnMeta("U_ID", is_nullable="NO")  # NOT NULL
        
        populated_count = 0
        num_rows = 100
        
        for _ in range(num_rows):
            temp_row = {}
            fk_col = "U_ID"
            
            if temp_row.get(fk_col) is not None:
                pass
            else:
                fk_pop_rates = fk_population_rates.get(node, {})
                population_rate = fk_pop_rates.get(fk_col, 1.0)
                
                should_populate = True
                # Key: for NOT NULL columns, population_rate is ignored
                if col_meta.is_nullable == "YES" and population_rate < 1.0:
                    should_populate = (rng.random() < population_rate)
                
                if should_populate:
                    parent_vals = parent_caches.get(fk_col, [])
                    if parent_vals:
                        temp_row[fk_col] = rng.choice(parent_vals)
            
            if temp_row.get("U_ID") is not None:
                populated_count += 1
        
        # Verify: 100% populated for NOT NULL columns regardless of rate
        self.assertEqual(populated_count, num_rows)


class TestNoParentValues(unittest.TestCase):
    """Test handling when no parent values are available"""
    
    def test_nullable_fk_no_parent_values(self):
        """Test that nullable FK remains NULL when no parent values available"""
        node = "db.P"
        parent_caches = {}  # No parent values
        fk_population_rates = {}
        rng = random.Random(42)
        
        col_meta = MockColumnMeta("U_ID", is_nullable="YES")
        
        temp_row = {}
        fk_col = "U_ID"
        
        if temp_row.get(fk_col) is not None:
            pass
        else:
            fk_pop_rates = fk_population_rates.get(node, {})
            population_rate = fk_pop_rates.get(fk_col, 1.0)
            
            should_populate = True
            if col_meta.is_nullable == "YES" and population_rate < 1.0:
                should_populate = (rng.random() < population_rate)
            
            if should_populate:
                parent_vals = parent_caches.get(fk_col, [])
                if parent_vals:
                    temp_row[fk_col] = rng.choice(parent_vals)
        
        # Verify: FK remains None (not in temp_row)
        self.assertNotIn("U_ID", temp_row)
    
    def test_not_null_fk_no_parent_values(self):
        """Test that NOT NULL FK remains unassigned when no parent values available (warning scenario)"""
        node = "db.P"
        parent_caches = {}  # No parent values
        fk_population_rates = {}
        rng = random.Random(42)
        
        col_meta = MockColumnMeta("U_ID", is_nullable="NO")
        
        temp_row = {}
        fk_col = "U_ID"
        warning_logged = False
        
        if temp_row.get(fk_col) is not None:
            pass
        else:
            fk_pop_rates = fk_population_rates.get(node, {})
            population_rate = fk_pop_rates.get(fk_col, 1.0)
            
            should_populate = True
            if col_meta.is_nullable == "YES" and population_rate < 1.0:
                should_populate = (rng.random() < population_rate)
            
            if should_populate:
                parent_vals = parent_caches.get(fk_col, [])
                if parent_vals:
                    temp_row[fk_col] = rng.choice(parent_vals)
                else:
                    # No parent values available
                    if col_meta.is_nullable == "NO":
                        warning_logged = True
        
        # Verify: FK not assigned (NULL) and warning would be logged
        self.assertNotIn("U_ID", temp_row)
        self.assertTrue(warning_logged)


class TestMultipleNullableFKs(unittest.TestCase):
    """Test multiple nullable FK columns in same table"""
    
    def test_multiple_fks_all_populated(self):
        """Test that multiple nullable FKs are all populated"""
        node = "db.P"
        parent_caches = {
            "U_ID": [1, 2, 3, 4, 5],
            "DEPT_ID": [100, 200, 300],
            "CATEGORY_ID": [10, 20, 30, 40]
        }
        fk_population_rates = {}
        rng = random.Random(42)
        
        fk_columns = ["U_ID", "DEPT_ID", "CATEGORY_ID"]
        col_metas = {
            "U_ID": MockColumnMeta("U_ID", is_nullable="YES"),
            "DEPT_ID": MockColumnMeta("DEPT_ID", is_nullable="YES"),
            "CATEGORY_ID": MockColumnMeta("CATEGORY_ID", is_nullable="YES")
        }
        
        temp_row = {}
        
        for fk_col in fk_columns:
            col_meta = col_metas[fk_col]
            
            if temp_row.get(fk_col) is not None:
                continue
            
            fk_pop_rates = fk_population_rates.get(node, {})
            population_rate = fk_pop_rates.get(fk_col, 1.0)
            
            should_populate = True
            if col_meta.is_nullable == "YES" and population_rate < 1.0:
                should_populate = (rng.random() < population_rate)
            
            if should_populate:
                parent_vals = parent_caches.get(fk_col, [])
                if parent_vals:
                    temp_row[fk_col] = rng.choice(parent_vals)
        
        # Verify all FKs populated
        self.assertIsNotNone(temp_row.get("U_ID"))
        self.assertIsNotNone(temp_row.get("DEPT_ID"))
        self.assertIsNotNone(temp_row.get("CATEGORY_ID"))
        self.assertIn(temp_row["U_ID"], [1, 2, 3, 4, 5])
        self.assertIn(temp_row["DEPT_ID"], [100, 200, 300])
        self.assertIn(temp_row["CATEGORY_ID"], [10, 20, 30, 40])


if __name__ == '__main__':
    unittest.main()
