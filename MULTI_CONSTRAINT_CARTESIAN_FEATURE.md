# Multi-Constraint Cartesian Product Feature

## Overview

This feature enables the generation of synthetic data that satisfies multiple overlapping UNIQUE constraints simultaneously. When a table has multiple composite UNIQUE constraints that share columns, the system automatically generates combinations that satisfy all constraints with zero duplicates.

## Problem Statement

Previously, when a table had multiple composite UNIQUE constraints with overlapping columns, the system would:
1. Select the "tightest" constraint (fewest combinations)
2. Use Cartesian product for that constraint only
3. Fill other constraint columns with random values
4. **Result**: One constraint satisfied ✓, other constraints have duplicates ✗

### Example Problem

Table `db.AC` with:
- `UNIQUE KEY ACS (A_ID, C_ID)` - 3000 × 10 = 30,000 combinations
- `UNIQUE KEY APR (A_ID, PR)` - 3000 × 2 = 6,000 combinations
- Both share column `A_ID`
- Requesting 6000 rows

**Old Behavior:**
- APR selected (tighter: 6000 vs 30000 combos)
- 6000 unique (A_ID, PR) pairs ✓
- C_ID random → ACS has duplicates ✗

## Solution

### Multi-Constraint Cartesian Product

The system now detects overlapping constraints and generates combinations that satisfy all of them.

**Key Insight:** Both constraints share `A_ID`. For each A_ID value:
- APR requires 2 different PR values (0, 1)
- So each A_ID needs exactly 2 rows

**Strategy:**
1. Detect constraints that share columns
2. Identify the shared column(s)
3. Calculate rows needed per shared value = max(unique values in non-shared columns)
4. Generate combinations that cycle through all possible values
5. Result: All constraints satisfied ✓

**New Behavior:**
- Both constraints detected as overlapping
- Shared column: A_ID
- Rows per A_ID: 2 (max of PR's 2 values and C_ID's 10 values)
- 6000 unique (A_ID, PR) pairs ✓
- 6000 unique (A_ID, C_ID) pairs ✓

## Implementation Details

### Detection Logic (lines 1177-1196)

```python
# Check for overlapping UNIQUE constraints (share columns)
overlapping_constraint_groups = []
if len(unique_fk_constraints) > 1:
    # Find constraints that share columns
    for i, uc1 in enumerate(unique_fk_constraints):
        group = set([uc1])
        for j, uc2 in enumerate(unique_fk_constraints):
            if i != j and set(uc1.columns) & set(uc2.columns):
                group.add(uc2)
        
        if len(group) > 1:
            overlapping_constraint_groups.append(list(group))
    
    # Deduplicate groups
    unique_groups = []
    for group in overlapping_constraint_groups:
        group_set = frozenset(uc.constraint_name for uc in group)
        if not any(group_set == frozenset(uc.constraint_name for uc in g) for g in unique_groups):
            unique_groups.append(group)
    overlapping_constraint_groups = unique_groups
```

### Shared Column Analysis (lines 1207-1211)

```python
# Find shared columns
shared_cols = set(constraint_group[0].columns)
for uc in constraint_group[1:]:
    shared_cols &= set(uc.columns)
```

### Rows Per Shared Value Calculation (lines 1213-1236)

For each constraint, find non-shared columns and count their unique values.
Take the maximum across all constraints.

```python
rows_per_shared_combo = 1

for uc in constraint_group:
    non_shared_cols = [col for col in uc.columns if col not in shared_cols]
    
    for col_name in non_shared_cols:
        # Count unique values (from FK parent or explicit config)
        rows_per_shared_combo = max(rows_per_shared_combo, unique_count)
```

### Combination Generation (lines 1252-1281)

```python
all_combinations = []
for shared_val in shared_values:
    for local_idx in range(rows_per_shared_combo):
        row_assignment = {primary_shared_col: shared_val}
        
        # For each constraint's non-shared columns
        for uc in constraint_group:
            non_shared_cols = [col for col in uc.columns if col not in shared_cols]
            
            for col_name in non_shared_cols:
                # Cycle through available values using modulo
                row_assignment[col_name] = available_vals[local_idx % len(available_vals)]
        
        all_combinations.append(row_assignment)
```

## Precedence Hierarchy

1. **Overlapping constraints** (Multi-constraint Cartesian product) - **NEW**
2. Tightest single constraint (if no overlapping)
3. Random FK values (if no controlled constraints)

The overlapping constraint detection takes precedence over tightest constraint selection because satisfying multiple constraints is more important than optimizing for a single constraint.

## Configuration Requirements

For a column to be "controlled" (eligible for Cartesian product):
- **FK column**: References a parent table with generated rows
- **Non-FK column**: Has explicit `populate_columns` config with:
  - `values`: Array of explicit values
  - `min`/`max`: Numeric range

## Debug Output

When overlapping constraints are detected:

```
[DEBUG] db.AC: Found overlapping UNIQUE constraints: ['ACS', 'APR']
[DEBUG] db.AC: Shared columns: ['A_ID']
[DEBUG] db.AC: Rows per shared value combination: 2
[DEBUG] db.AC: Pre-allocated 6000 rows satisfying 2 constraints
```

## Edge Cases

### Insufficient Combinations

When there aren't enough unique combinations:

```
WARNING: db.AC only has 4000 unique combinations for overlapping constraints but 6000 rows requested. Will generate duplicates.
```

The system repeats combinations cyclically to reach the requested row count.

### Empty Value Lists

The system validates that value lists are not empty and prints errors:

```
ERROR: db.AC: No values available for column C_ID
ERROR: db.AC: No values found for shared column A_ID
```

## Testing

### Unit Tests (test_overlapping_constraints.py)

- `TestOverlappingConstraintDetection`: Detection logic tests
- `TestMultiConstraintCombinationGeneration`: Generation logic tests

### Integration Test (test_integration_overlapping.py)

Simulates the exact scenario from the issue:
- 3000 A_ID values
- 10 C_ID values  
- 2 PR values
- 6000 rows requested
- Verifies zero duplicates in both APR and ACS

## Verification Queries

After generation, verify with SQL:

```sql
-- Check APR constraint
SELECT A_ID, PR, COUNT(*) 
FROM db.AC 
GROUP BY A_ID, PR 
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Check ACS constraint
SELECT A_ID, C_ID, COUNT(*) 
FROM db.AC 
GROUP BY A_ID, C_ID 
HAVING COUNT(*) > 1;
-- Expected: 0 rows
```

## Example Use Case

### Database Schema

```sql
CREATE TABLE db.A (ID INT PRIMARY KEY);
CREATE TABLE db.C (ID INT PRIMARY KEY);

CREATE TABLE db.AC (
    A_ID INT,
    C_ID INT,
    PR INT,
    CONSTRAINT fk_a FOREIGN KEY (A_ID) REFERENCES A(ID),
    CONSTRAINT fk_c FOREIGN KEY (C_ID) REFERENCES C(ID),
    CONSTRAINT ACS UNIQUE KEY (A_ID, C_ID),
    CONSTRAINT APR UNIQUE KEY (A_ID, PR)
);
```

### Configuration

```json
{
  "tables": [
    {"schema": "db", "table": "A", "total_rows": 3000},
    {"schema": "db", "table": "C", "total_rows": 10},
    {
      "schema": "db",
      "table": "AC",
      "total_rows": 6000,
      "populate_columns": {
        "PR": {"column": "PR", "values": [0, 1]}
      }
    }
  ]
}
```

### Result

- 6000 rows generated
- Each (A_ID, PR) pair is unique - APR satisfied ✓
- Each (A_ID, C_ID) pair is unique - ACS satisfied ✓
- Each A_ID appears exactly 2 times (once with PR=0, once with PR=1)

## Limitations

1. Only handles one overlapping group (uses the first group found)
2. Shared columns must be "controlled" (FK or explicit config)
3. Non-shared columns must be "controlled" for proper generation
4. May generate duplicates if insufficient unique combinations exist

## Future Enhancements

1. Handle multiple overlapping groups simultaneously
2. Support more complex overlapping patterns (multiple shared columns)
3. Optimize for minimum rows needed across all constraints
4. Better handling of insufficient combinations (e.g., relax least important constraint)
