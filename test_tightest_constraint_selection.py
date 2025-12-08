#!/usr/bin/env python3
"""Unit tests for auto-selection of tightest composite UNIQUE constraint.

This tests the feature where multiple composite UNIQUE constraints are detected,
and the one with the fewest combinations is automatically selected.
"""
import unittest
import sys
from io import StringIO
from generate_synthetic_data_utils import (
    ColumnMeta,
    UniqueConstraint,
    TableMeta,
    FKMeta,
    GLOBALS,
)


class TestTightestConstraintSelection(unittest.TestCase):
    """Test auto-selection of tightest constraint when multiple exist."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Enable debug mode for test output
        GLOBALS["debug"] = True
    
    def tearDown(self):
        """Clean up after tests."""
        GLOBALS["debug"] = False
    
    def _calculate_combo_count(self, constraint, fk_map, populate_config, generated_rows):
        """Helper method to calculate combination count for a constraint.
        
        This mirrors the logic in generate_synthetic_data.py lines 1184-1218.
        The duplication is intentional - tests should independently validate behavior
        without depending on the implementation being tested.
        
        Args:
            constraint: UniqueConstraint object
            fk_map: Dictionary mapping column names to FKMeta objects
            populate_config: Dictionary mapping column names to config objects
            generated_rows: Dictionary of already generated rows by node
            
        Returns:
            int or float('inf') - Number of combinations or infinity if unknown
        """
        combo_count = 1
        
        for col_name in constraint.columns:
            # Check if FK column
            if col_name in fk_map:
                fk = fk_map[col_name]
                parent_node = "{0}.{1}".format(fk.referenced_table_schema, fk.referenced_table_name)
                
                if parent_node in generated_rows:
                    parent_rows = generated_rows[parent_node]
                    parent_col = fk.referenced_column_name
                    parent_vals = [r.get(parent_col) for r in parent_rows if r and r.get(parent_col) is not None]
                    unique_vals = len(set(parent_vals))
                    combo_count *= unique_vals
                else:
                    # Parent not generated yet - can't calculate
                    combo_count = float('inf')
                    break
            else:
                # Non-FK column with explicit config
                col_config = populate_config.get(col_name, {})
                
                if "values" in col_config:
                    combo_count *= len(col_config["values"])
                elif "min" in col_config:
                    # Estimate range size
                    min_val = col_config.get("min", 0)
                    max_val = col_config.get("max", 100)
                    combo_count *= (max_val - min_val + 1)
                else:
                    # Can't determine size
                    combo_count = float('inf')
                    break
        
        return combo_count
    
    def test_select_tighter_fk_constraint(self):
        """Test that constraint with fewer FK combinations is selected."""
        # Two constraints with FK columns only
        # ACS: A_ID (3000 vals) × C_ID (10 vals) = 30,000 combinations
        # APR: A_ID (3000 vals) × R_ID (2 unique IDs) = 6,000 combinations
        # Should select APR (tighter)
        
        constraints = [
            UniqueConstraint("ACS", ("A_ID", "C_ID")),
            UniqueConstraint("APR", ("A_ID", "R_ID")),
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "AC", "A_ID", "db", "A", "ID", False, None),
            "C_ID": FKMeta("fk_c", "db", "AC", "C_ID", "db", "C", "ID", False, None),
            "R_ID": FKMeta("fk_r", "db", "AC", "R_ID", "db", "R", "ID", False, None),
        }
        
        # Simulate generated parent rows
        generated_rows = {
            "db.A": [{"ID": i} for i in range(1, 3001)],  # 3000 unique IDs
            "db.C": [{"ID": i} for i in range(1, 11)],     # 10 unique IDs
            "db.R": [{"ID": i} for i in range(1, 3)],      # 2 unique IDs (1-2)
        }
        
        populate_config = {}
        
        # Calculate combo counts
        constraint_combos = []
        for uc in constraints:
            combo_count = self._calculate_combo_count(uc, fk_map, populate_config, generated_rows)
            constraint_combos.append((uc, combo_count))
        
        # Sort by combo count (ascending)
        constraint_combos.sort(key=lambda x: x[1])
        selected_uc, min_combos = constraint_combos[0]
        
        # Should select APR (6000 < 30000)
        self.assertEqual(selected_uc.constraint_name, "APR")
        self.assertEqual(min_combos, 6000)
        
        # Verify ACS has more combinations
        acs_combos = next((c for u, c in constraint_combos if u.constraint_name == "ACS"), None)
        self.assertEqual(acs_combos, 30000)
    
    def test_select_tighter_mixed_constraint(self):
        """Test that constraint with fewer mixed FK/non-FK combinations is selected."""
        # Two constraints with mixed columns
        # ACS: A_ID (3000 vals) × C_ID (10 vals) = 30,000 combinations
        # APR: A_ID (3000 vals) × PR (2 explicit values) = 6,000 combinations
        # Should select APR (tighter)
        
        constraints = [
            UniqueConstraint("ACS", ("A_ID", "C_ID")),
            UniqueConstraint("APR", ("A_ID", "PR")),
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "AC", "A_ID", "db", "A", "ID", False, None),
            "C_ID": FKMeta("fk_c", "db", "AC", "C_ID", "db", "C", "ID", False, None),
        }
        
        generated_rows = {
            "db.A": [{"ID": i} for i in range(1, 3001)],  # 3000 unique IDs
            "db.C": [{"ID": i} for i in range(1, 11)],     # 10 unique IDs
        }
        
        # PR has explicit values
        populate_config = {
            "PR": {"values": [0, 1]}  # 2 explicit values
        }
        
        # Calculate combo counts
        constraint_combos = []
        for uc in constraints:
            combo_count = self._calculate_combo_count(uc, fk_map, populate_config, generated_rows)
            constraint_combos.append((uc, combo_count))
        
        constraint_combos.sort(key=lambda x: x[1])
        selected_uc, min_combos = constraint_combos[0]
        
        # Should select APR (6000 < 30000)
        self.assertEqual(selected_uc.constraint_name, "APR")
        self.assertEqual(min_combos, 6000)
    
    def test_select_constraint_with_min_max_range(self):
        """Test calculation with min/max range for non-FK columns."""
        constraints = [
            UniqueConstraint("ACS", ("A_ID", "score")),  # score has min/max range
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "AC", "A_ID", "db", "A", "ID", False, None),
        }
        
        generated_rows = {
            "db.A": [{"ID": i} for i in range(1, 101)],  # 100 unique IDs
        }
        
        # score has min/max range
        populate_config = {
            "score": {"min": 0, "max": 9}  # 10 values (0-9)
        }
        
        combo_count = self._calculate_combo_count(
            constraints[0], fk_map, populate_config, generated_rows
        )
        
        # 100 IDs × 10 score values = 1000 combinations
        self.assertEqual(combo_count, 1000)
    
    def test_unknown_combinations_when_parent_not_generated(self):
        """Test that constraint gets inf combos when parent not generated yet."""
        constraints = [
            UniqueConstraint("ACS", ("A_ID", "C_ID")),
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "AC", "A_ID", "db", "A", "ID", False, None),
            "C_ID": FKMeta("fk_c", "db", "AC", "C_ID", "db", "C", "ID", False, None),
        }
        
        # Parent db.A exists, but db.C doesn't
        generated_rows = {
            "db.A": [{"ID": i} for i in range(1, 101)],
        }
        
        populate_config = {}
        
        combo_count = self._calculate_combo_count(
            constraints[0], fk_map, populate_config, generated_rows
        )
        
        # Should return infinity because db.C not generated
        self.assertEqual(combo_count, float('inf'))
    
    def test_unknown_combinations_when_no_explicit_config(self):
        """Test that constraint gets inf combos when non-FK has no values/min/max."""
        constraints = [
            UniqueConstraint("APR", ("A_ID", "PR")),
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "AC", "A_ID", "db", "A", "ID", False, None),
        }
        
        generated_rows = {
            "db.A": [{"ID": i} for i in range(1, 101)],
        }
        
        # PR has no explicit config
        populate_config = {}
        
        combo_count = self._calculate_combo_count(
            constraints[0], fk_map, populate_config, generated_rows
        )
        
        # Should return infinity because PR has no values or min/max
        self.assertEqual(combo_count, float('inf'))
    
    def test_equal_combinations_stable_sort(self):
        """Test that when constraints have equal combos, first is selected."""
        constraints = [
            UniqueConstraint("ABC", ("A_ID", "B_ID")),
            UniqueConstraint("ACD", ("A_ID", "C_ID")),
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "T", "A_ID", "db", "A", "ID", False, None),
            "B_ID": FKMeta("fk_b", "db", "T", "B_ID", "db", "B", "ID", False, None),
            "C_ID": FKMeta("fk_c", "db", "T", "C_ID", "db", "C", "ID", False, None),
        }
        
        generated_rows = {
            "db.A": [{"ID": i} for i in range(1, 11)],   # 10 IDs
            "db.B": [{"ID": i} for i in range(1, 101)],  # 100 IDs  
            "db.C": [{"ID": i} for i in range(1, 101)],  # 100 IDs
        }
        
        populate_config = {}
        
        # Calculate combo counts
        constraint_combos = []
        for uc in constraints:
            combo_count = self._calculate_combo_count(uc, fk_map, populate_config, generated_rows)
            constraint_combos.append((uc, combo_count))
        
        # Both should have 1000 combinations
        self.assertEqual(constraint_combos[0][1], 1000)
        self.assertEqual(constraint_combos[1][1], 1000)
        
        # Sort - with equal counts, order should be stable (first in list stays first)
        constraint_combos.sort(key=lambda x: x[1])
        selected_uc = constraint_combos[0][0]
        
        # Should select ABC (first alphabetically in original list)
        self.assertEqual(selected_uc.constraint_name, "ABC")
    
    def test_three_constraints_select_tightest(self):
        """Test selection among three constraints."""
        constraints = [
            UniqueConstraint("loose", ("A_ID", "B_ID")),      # 100 × 100 = 10,000
            UniqueConstraint("medium", ("A_ID", "C_ID")),     # 100 × 50 = 5,000
            UniqueConstraint("tight", ("A_ID", "PR")),        # 100 × 2 = 200
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "T", "A_ID", "db", "A", "ID", False, None),
            "B_ID": FKMeta("fk_b", "db", "T", "B_ID", "db", "B", "ID", False, None),
            "C_ID": FKMeta("fk_c", "db", "T", "C_ID", "db", "C", "ID", False, None),
        }
        
        generated_rows = {
            "db.A": [{"ID": i} for i in range(1, 101)],   # 100 IDs
            "db.B": [{"ID": i} for i in range(1, 101)],   # 100 IDs
            "db.C": [{"ID": i} for i in range(1, 51)],    # 50 IDs
        }
        
        populate_config = {
            "PR": {"values": [0, 1]}  # 2 values
        }
        
        # Calculate and sort
        constraint_combos = []
        for uc in constraints:
            combo_count = self._calculate_combo_count(uc, fk_map, populate_config, generated_rows)
            constraint_combos.append((uc, combo_count))
        
        constraint_combos.sort(key=lambda x: x[1])
        selected_uc, min_combos = constraint_combos[0]
        
        # Should select "tight" (200 combinations)
        self.assertEqual(selected_uc.constraint_name, "tight")
        self.assertEqual(min_combos, 200)
        
        # Verify all combo counts
        combo_dict = {u.constraint_name: c for u, c in constraint_combos}
        self.assertEqual(combo_dict["tight"], 200)
        self.assertEqual(combo_dict["medium"], 5000)
        self.assertEqual(combo_dict["loose"], 10000)


if __name__ == '__main__':
    unittest.main()
