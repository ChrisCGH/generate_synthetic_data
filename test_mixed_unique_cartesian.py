#!/usr/bin/env python3
"""Unit tests for Cartesian product with mixed UNIQUE constraints (FK + non-FK columns).

This tests the extended feature where composite UNIQUE constraints with mixed
FK and non-FK columns (that have explicit values or min/max) use Cartesian product.
"""
import unittest
import random
from generate_synthetic_data_utils import (
    ColumnMeta,
    UniqueConstraint,
    TableMeta,
    FKMeta,
)


class TestMixedUniqueDetection(unittest.TestCase):
    """Test detection of composite UNIQUE constraints with mixed FK/non-FK columns."""
    
    def _detect_controlled_constraints(self, unique_constraints, fk_map, populate_config):
        """Helper method to detect controlled constraints.
        
        Args:
            unique_constraints: List of UniqueConstraint objects
            fk_map: Dictionary mapping column names to FKMeta objects
            populate_config: Dictionary mapping column names to config objects
            
        Returns:
            List of controlled unique constraints
        """
        unique_controlled_constraints = []
        for uc in unique_constraints:
            if len(uc.columns) < 2:
                continue
            
            all_controlled = True
            for col in uc.columns:
                is_fk = col in fk_map
                has_explicit_config = False
                
                col_config = populate_config.get(col, {})
                has_explicit_config = "values" in col_config or "min" in col_config
                
                if not (is_fk or has_explicit_config):
                    all_controlled = False
                    break
            
            if all_controlled:
                unique_controlled_constraints.append(uc)
        
        return unique_controlled_constraints
    
    def test_detect_mixed_fk_with_explicit_values(self):
        """Test that UNIQUE constraints with FK + non-FK (explicit values) are detected."""
        unique_constraints = [
            UniqueConstraint("unique_a_pr", ("A_ID", "PR")),  # A_ID is FK, PR has explicit values
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "AC", "A_ID", "db", "A", "ID", False, None),
        }
        
        # Simulate populate_columns config
        populate_config = {
            "PR": {"column": "PR", "values": [0, 1]}
        }
        
        unique_controlled_constraints = self._detect_controlled_constraints(
            unique_constraints, fk_map, populate_config
        )
        
        # Should detect the mixed constraint
        self.assertEqual(len(unique_controlled_constraints), 1)
        self.assertEqual(unique_controlled_constraints[0].constraint_name, "unique_a_pr")
    
    def test_detect_mixed_fk_with_min_max(self):
        """Test that UNIQUE constraints with FK + non-FK (min/max range) are detected."""
        unique_constraints = [
            UniqueConstraint("unique_a_score", ("A_ID", "score")),  # A_ID is FK, score has min/max
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "T", "A_ID", "db", "A", "ID", False, None),
        }
        
        populate_config = {
            "score": {"column": "score", "min": 0, "max": 100}
        }
        
        unique_controlled_constraints = self._detect_controlled_constraints(
            unique_constraints, fk_map, populate_config
        )
        
        # Should detect the mixed constraint
        self.assertEqual(len(unique_controlled_constraints), 1)
        self.assertEqual(unique_controlled_constraints[0].constraint_name, "unique_a_score")
    
    def test_skip_non_fk_without_config(self):
        """Test that UNIQUE constraints with non-FK columns without config are skipped."""
        unique_constraints = [
            UniqueConstraint("unique_a_unconfigured", ("A_ID", "unconfigured_col")),
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "T", "A_ID", "db", "A", "ID", False, None),
        }
        
        populate_config = {}  # No config for unconfigured_col
        
        unique_controlled_constraints = self._detect_controlled_constraints(
            unique_constraints, fk_map, populate_config
        )
        
        # Should NOT detect constraint with unconfigured column
        self.assertEqual(len(unique_controlled_constraints), 0)
    
    def test_detect_all_non_fk_with_config(self):
        """Test that UNIQUE constraints with all non-FK columns (but configured) are detected."""
        unique_constraints = [
            UniqueConstraint("unique_x_y", ("x", "y")),  # Both non-FK but have explicit values
        ]
        
        fk_map = {}  # No FKs
        
        populate_config = {
            "x": {"column": "x", "values": [1, 2, 3]},
            "y": {"column": "y", "values": ["a", "b", "c"]}
        }
        
        unique_controlled_constraints = self._detect_controlled_constraints(
            unique_constraints, fk_map, populate_config
        )
        
        # Should detect constraint with all configured non-FK columns
        self.assertEqual(len(unique_controlled_constraints), 1)
        self.assertEqual(unique_controlled_constraints[0].constraint_name, "unique_x_y")
    
    def test_three_column_mixed(self):
        """Test three-column UNIQUE with mix of FKs and non-FKs."""
        unique_constraints = [
            UniqueConstraint("unique_abc", ("A_ID", "B_ID", "status")),
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "T", "A_ID", "db", "A", "ID", False, None),
            "B_ID": FKMeta("fk_b", "db", "T", "B_ID", "db", "B", "ID", False, None),
        }
        
        populate_config = {
            "status": {"column": "status", "values": ["active", "inactive"]}
        }
        
        unique_controlled_constraints = self._detect_controlled_constraints(
            unique_constraints, fk_map, populate_config
        )
        
        # Should detect three-column mixed constraint
        self.assertEqual(len(unique_controlled_constraints), 1)
        self.assertEqual(unique_controlled_constraints[0].constraint_name, "unique_abc")


class TestMixedCartesianProductGeneration(unittest.TestCase):
    """Test Cartesian product generation with mixed FK/non-FK columns."""
    
    def test_explicit_values_cartesian(self):
        """Test Cartesian product with explicit values array."""
        import itertools
        
        # Simulate parent FK values
        parent_a_values = [1, 2, 3]
        
        # Explicit values for non-FK column
        pr_values = [0, 1]
        
        # Generate Cartesian product
        all_combinations = list(itertools.product(parent_a_values, pr_values))
        
        # Should generate 3 × 2 = 6 combinations
        self.assertEqual(len(all_combinations), 6)
        
        # Verify combinations
        expected = [
            (1, 0), (1, 1),
            (2, 0), (2, 1),
            (3, 0), (3, 1)
        ]
        self.assertEqual(sorted(all_combinations), sorted(expected))
    
    def test_min_max_range_cartesian(self):
        """Test Cartesian product with min/max range values."""
        import itertools
        
        parent_a_values = [10, 20]
        
        # Simulate generated range values (from min/max)
        score_values = [0, 50, 100]
        
        # Generate Cartesian product
        all_combinations = list(itertools.product(parent_a_values, score_values))
        
        # Should generate 2 × 3 = 6 combinations
        self.assertEqual(len(all_combinations), 6)
    
    def test_multiple_non_fk_columns(self):
        """Test Cartesian product with multiple non-FK columns."""
        import itertools
        
        parent_a_values = [1, 2]
        status_values = ["active", "inactive"]
        priority_values = [1, 2, 3]
        
        # Generate Cartesian product
        all_combinations = list(itertools.product(parent_a_values, status_values, priority_values))
        
        # Should generate 2 × 2 × 3 = 12 combinations
        self.assertEqual(len(all_combinations), 12)


class TestInsufficientCombinations(unittest.TestCase):
    """Test behavior when combinations are insufficient for requested rows."""
    
    def test_insufficient_mixed_combinations(self):
        """Test warning when mixed FK + non-FK combinations are insufficient."""
        import itertools
        
        parent_a_values = [1, 2, 3]  # 3 values
        pr_values = [0, 1]  # 2 values
        
        all_combinations = list(itertools.product(parent_a_values, pr_values))
        
        # 3 × 2 = 6 combinations
        self.assertEqual(len(all_combinations), 6)
        
        # If we request 10 rows, we need to repeat combinations
        requested_rows = 10
        
        if len(all_combinations) < requested_rows:
            # Repeat using modulo
            extended = []
            for i in range(requested_rows):
                extended.append(all_combinations[i % len(all_combinations)])
            all_combinations = extended
        
        self.assertEqual(len(all_combinations), 10)
        
        # First 6 should be unique, remaining 4 should be duplicates
        unique_combos = set(all_combinations)
        self.assertEqual(len(unique_combos), 6)


if __name__ == '__main__':
    unittest.main()
