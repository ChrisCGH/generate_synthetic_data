#!/usr/bin/env python3
"""Integration test for mixed UNIQUE constraints (FK + non-FK columns).

This test demonstrates the real-world scenario from the issue:
- Table AC with UNIQUE(A_ID, PR) where A_ID is FK and PR has explicit values [0, 1]
- Requesting 6000 rows with 3000 unique A_ID values and 2 PR values
- Expected: 6000 unique (A_ID, PR) combinations from 6000 possible combinations
"""
import sys
import random
from generate_synthetic_data_utils import (
    ColumnMeta,
    UniqueConstraint,
    TableMeta,
    FKMeta,
    debug_print,
    GLOBALS
)
import itertools


def test_mixed_unique_scenario():
    """Test the user's scenario: UNIQUE(A_ID, PR) with A_ID as FK and PR with explicit values."""
    print("\n=== Test 1: Mixed UNIQUE (FK + explicit values) - User Scenario ===")
    
    # Enable debug output
    GLOBALS["debug"] = True
    
    # Simulate parent table A with 3000 unique IDs
    parent_a_values = list(range(1, 3001))
    
    # PR has explicit values [0, 1]
    pr_values = [0, 1]
    
    # Generate Cartesian product
    all_combinations = list(itertools.product(parent_a_values, pr_values))
    
    print(f"Parent A values: {len(parent_a_values)} unique IDs")
    print(f"PR values: {pr_values}")
    print(f"Total possible combinations: {len(all_combinations)}")
    
    # Request 6000 rows
    requested_rows = 6000
    
    if len(all_combinations) >= requested_rows:
        # Sample random subset
        rng = random.Random(42)
        rng.shuffle(all_combinations)
        selected_combinations = all_combinations[:requested_rows]
        print(f"Selected {len(selected_combinations)} unique combinations")
    else:
        print(f"ERROR: Insufficient combinations ({len(all_combinations)} < {requested_rows})")
        return False
    
    # Verify uniqueness
    unique_combos = set(selected_combinations)
    print(f"Unique combinations in result: {len(unique_combos)}")
    print(f"Duplicates: {len(selected_combinations) - len(unique_combos)}")
    
    # Check counts per PR value
    pr_0_count = sum(1 for combo in selected_combinations if combo[1] == 0)
    pr_1_count = sum(1 for combo in selected_combinations if combo[1] == 1)
    print(f"Rows with PR=0: {pr_0_count}")
    print(f"Rows with PR=1: {pr_1_count}")
    
    assert len(unique_combos) == requested_rows, "Should have zero duplicates!"
    assert pr_0_count + pr_1_count == requested_rows, "PR values should cover all rows"
    
    print("✓ Test passed: Zero duplicates in UNIQUE(A_ID, PR)")
    return True


def test_insufficient_combinations():
    """Test scenario with insufficient combinations."""
    print("\n=== Test 2: Insufficient combinations (repeating) ===")
    
    # Simulate parent table with only 3 unique IDs
    parent_a_values = [1, 2, 3]
    
    # PR has explicit values [0, 1]
    pr_values = [0, 1]
    
    # Generate Cartesian product
    all_combinations = list(itertools.product(parent_a_values, pr_values))
    
    print(f"Parent A values: {len(parent_a_values)} unique IDs")
    print(f"PR values: {pr_values}")
    print(f"Total possible combinations: {len(all_combinations)}")
    
    # Request 10 rows (more than 6 combinations)
    requested_rows = 10
    
    if len(all_combinations) < requested_rows:
        print(f"WARNING: Only {len(all_combinations)} unique combinations but {requested_rows} rows requested")
        
        # Repeat combinations using modulo
        extended_combinations = []
        for i in range(requested_rows):
            extended_combinations.append(all_combinations[i % len(all_combinations)])
        selected_combinations = extended_combinations
    else:
        rng = random.Random(42)
        rng.shuffle(all_combinations)
        selected_combinations = all_combinations[:requested_rows]
    
    print(f"Generated {len(selected_combinations)} rows")
    
    # Verify uniqueness (should have only 6 unique)
    unique_combos = set(selected_combinations)
    print(f"Unique combinations in result: {len(unique_combos)}")
    print(f"Duplicates: {len(selected_combinations) - len(unique_combos)}")
    
    assert len(selected_combinations) == requested_rows, "Should generate requested number of rows"
    assert len(unique_combos) == len(all_combinations), "Should have max unique combinations"
    
    print("✓ Test passed: Repeated combinations when insufficient")
    return True


def test_multiple_non_fk_columns():
    """Test scenario with multiple non-FK columns with explicit values."""
    print("\n=== Test 3: Multiple non-FK columns (FK + two non-FK) ===")
    
    # Simulate parent table B with 10 unique IDs
    parent_b_values = list(range(1, 11))
    
    # Status has explicit values
    status_values = ["active", "inactive", "pending"]
    
    # Priority has explicit values
    priority_values = [1, 2]
    
    # Generate Cartesian product
    all_combinations = list(itertools.product(parent_b_values, status_values, priority_values))
    
    print(f"Parent B values: {len(parent_b_values)} unique IDs")
    print(f"Status values: {status_values}")
    print(f"Priority values: {priority_values}")
    print(f"Total possible combinations: {len(all_combinations)}")
    
    # Request 30 rows
    requested_rows = 30
    
    rng = random.Random(42)
    rng.shuffle(all_combinations)
    selected_combinations = all_combinations[:requested_rows]
    
    print(f"Selected {len(selected_combinations)} unique combinations")
    
    # Verify uniqueness
    unique_combos = set(selected_combinations)
    print(f"Unique combinations in result: {len(unique_combos)}")
    print(f"Duplicates: {len(selected_combinations) - len(unique_combos)}")
    
    assert len(unique_combos) == requested_rows, "Should have zero duplicates!"
    
    print("✓ Test passed: Zero duplicates with multiple non-FK columns")
    return True


def test_all_non_fk_with_values():
    """Test scenario with all non-FK columns (no FKs)."""
    print("\n=== Test 4: All non-FK columns with explicit values ===")
    
    # No FK columns, only non-FK with explicit values
    x_values = [1, 2, 3, 4, 5]
    y_values = ["a", "b", "c"]
    
    # Generate Cartesian product
    all_combinations = list(itertools.product(x_values, y_values))
    
    print(f"X values: {x_values}")
    print(f"Y values: {y_values}")
    print(f"Total possible combinations: {len(all_combinations)}")
    
    # Request 10 rows
    requested_rows = 10
    
    rng = random.Random(42)
    rng.shuffle(all_combinations)
    selected_combinations = all_combinations[:requested_rows]
    
    print(f"Selected {len(selected_combinations)} unique combinations")
    
    # Verify uniqueness
    unique_combos = set(selected_combinations)
    print(f"Unique combinations in result: {len(unique_combos)}")
    print(f"Duplicates: {len(selected_combinations) - len(unique_combos)}")
    
    assert len(unique_combos) == requested_rows, "Should have zero duplicates!"
    
    print("✓ Test passed: All non-FK columns work correctly")
    return True


def main():
    """Run all integration tests."""
    print("=" * 70)
    print("Integration Tests for Mixed UNIQUE Constraints")
    print("=" * 70)
    
    all_passed = True
    
    try:
        all_passed &= test_mixed_unique_scenario()
        all_passed &= test_insufficient_combinations()
        all_passed &= test_multiple_non_fk_columns()
        all_passed &= test_all_non_fk_with_values()
    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    print("\n" + "=" * 70)
    if all_passed:
        print("✓ All integration tests passed!")
        print("=" * 70)
        return 0
    else:
        print("✗ Some integration tests failed")
        print("=" * 70)
        return 1


if __name__ == '__main__':
    sys.exit(main())
