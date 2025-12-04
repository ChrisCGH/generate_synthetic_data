#!/usr/bin/env python3
"""Highly optimized standalone version"""
import argparse, json, sys, random, threading, re
import itertools
from collections import defaultdict, deque
from getpass import getpass
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

try:
    import pymysql
except ImportError:
    print("Error: PyMySQL required.  Install: pip install PyMySQL", file=sys.stderr)
    sys.exit(1)

from generate_synthetic_data_utils import *

def build_dependency_graph(config_tables, fk_list, composite_logical_fks=None):
    nodes = set("{0}.{1}".format(t['schema'], t['table']) for t in config_tables)
    edges = defaultdict(set)
    for fk in fk_list:
        parent = "{0}.{1}".format(fk. referenced_table_schema, fk.referenced_table_name)
        child = "{0}.{1}". format(fk.table_schema, fk.table_name)
        if parent in nodes and child in nodes:
            edges[parent].add(child)
    if composite_logical_fks:
        for comp in composite_logical_fks:
            parent = "{0}.{1}".format(comp['referenced_table_schema'], comp['referenced_table_name'])
            child = "{0}.{1}".format(comp['table_schema'], comp['table_name'])
            if parent in nodes and child in nodes:
                edges[parent].add(child)
    return nodes, edges

def topo_sort(nodes, edges):
    indeg = {n: 0 for n in nodes}
    for u in edges:
        for v in edges[u]:
            indeg[v] = indeg. get(v, 0) + 1
    q = deque([n for n, d in indeg.items() if d == 0])
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v in edges. get(u, []):
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)
    if len(order) != len(nodes):
        order.extend(list(set(nodes) - set(order)))
    return order

def connect_mysql(args):
    pwd = args.src_password
    if args.ask_pass and not pwd:
        pwd = getpass("Password for {0}@{1}: ".format(args. src_user, args.src_host))
    try:
        return pymysql.connect(host=args.src_host, port=args.src_port, user=args.src_user, password=pwd, charset="utf8mb4", autocommit=True)
    except Exception as e:
        print("Error: Failed to connect to MySQL: {0}".format(e), file=sys.stderr)
        sys.exit(1)

def load_table_columns(conn, schema, table):
    cur = conn.cursor()
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_TYPE, COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_DEFAULT FROM information_schema. COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION", (schema, table))
    return [ColumnMeta(*r) for r in cur.fetchall()]

def load_table_pk(conn, schema, table):
    cur = conn.cursor()
    cur.execute("SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION", (schema, table))
    return [r[0] for r in cur.fetchall()]

def load_table_engine_and_ai(conn, schema, table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT ENGINE, AUTO_INCREMENT FROM information_schema. TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (schema, table))
        r = cur.fetchone()
        return (r[0], r[1]) if r else (None, None)
    except:
        return None, None

def load_unique_constraints(conn, schema, table):
    cur = conn.cursor()
    cur.execute("SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND NON_UNIQUE=0 ORDER BY INDEX_NAME, SEQ_IN_INDEX", (schema, table))
    constraints = {}
    for idx_name, col_name, seq in cur.fetchall():
        if idx_name != "PRIMARY":
            constraints. setdefault(idx_name, []).append(col_name)
    return [UniqueConstraint(n, tuple(cols)) for n, cols in constraints.items()]

def load_fk_constraints_for_schema(conn, schema):
    cur = conn.cursor()
    cur.execute("SELECT CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=%s AND REFERENCED_TABLE_NAME IS NOT NULL", (schema,))
    return [FKMeta(*r, is_logical=False, condition=None) for r in cur.fetchall()]

def sample_static_fk_values(conn, static_schema, static_table, static_column, sample_size, rng):
    cur = conn.cursor()
    try:
        query = "SELECT DISTINCT `{0}` FROM `{1}`.`{2}` WHERE `{3}` IS NOT NULL".format(static_column, static_schema, static_table, static_column)
        if sample_size <= 500:
            query += " ORDER BY RAND()"
        query += " LIMIT %s"
        cur.execute(query, (sample_size,))
        return [r[0] for r in cur.fetchall()]
    except Exception as e:
        print("Error: Failed to sample from {0}.{1}.{2}: {3}".format(static_schema, static_table, static_column, e), file=sys.stderr)
        sys.exit(1)

def load_logical_fks_from_config(config):
    single_fks, composite_fks = [], []
    for table_cfg in config:
        tschema, tname = table_cfg["schema"], table_cfg["table"]
        ignore_self_refs = table_cfg.get("ignore_self_referential_fks", False)
        for lfk in table_cfg.get("logical_fks", []):
            if "column" in lfk:
                cname = lfk["column"]
                ref_schema, ref_table, ref_column = lfk["referenced_schema"], lfk["referenced_table"], lfk["referenced_column"]
                if ignore_self_refs and ref_schema == tschema and ref_table == tname:
                    continue
                condition = lfk.get("condition")  # Support conditional FK
                single_fks.append(FKMeta(lfk.get("constraint_name", "LOGICAL_{0}_{1}".format(tname, cname)), tschema, tname, cname, ref_schema, ref_table, ref_column, True, condition))
            elif "child_columns" in lfk and "referenced_columns" in lfk:
                child_cols, parent_cols = tuple(lfk["child_columns"]), tuple(lfk["referenced_columns"])
                ref_schema, ref_table = lfk["referenced_schema"], lfk["referenced_table"]
                if ignore_self_refs and ref_schema == tschema and ref_table == tname:
                    continue
                composite_fks.append({"constraint_name": lfk.get("constraint_name", "LOGICAL_{0}_{1}".format(tname, '_'.join(child_cols))), "table_schema": tschema, "table_name": tname, "child_columns": child_cols, "referenced_table_schema": ref_schema, "referenced_table_name": ref_table, "referenced_columns": parent_cols, "population_rate": lfk.get("population_rate"), "condition": lfk.get("condition")})
    return single_fks, composite_fks

def load_config(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        if not isinstance(cfg, list):
            raise ValueError("Config must be an array")
        for entry in cfg:
            if "schema" not in entry or "table" not in entry:
                raise ValueError("Each entry must have 'schema' and 'table'")
        return cfg
    except IOError:
        print("Error: Config file not found: {0}".format(path), file=sys.stderr)
        sys.exit(1)
    except (json.JSONDecodeError, ValueError) as e:
        print("Error: Invalid config: {0}".format(e), file=sys.stderr)
        sys.exit(1)

class FastSyntheticGenerator:
    """Optimized generator with batched operations and reduced object copies"""
    
    def __init__(self, conn, args, config):
        self.conn, self.args, self.config = conn, args, config
        self.rng = random.Random(args.seed if args.seed is not None else 42)
        self.hmac_key_bytes = args.hmac_key.encode("utf-8") if args.hmac_key else None
        self.sample_size = args.sample_size
        self.table_map = {"{0}.{1}".format(t['schema'], t['table']): t for t in config}
        self.metadata, self.fks, self.logical_composite_fks = {}, [], []
        self.fk_columns, self.unique_constraints, self.static_samples = {}, {}, {}
        self.generated_rows = {}
        self.unique_value_trackers = {}
        self.insert_sql_lines = []
        self.delete_sql_lines = []
        self.forced_explicit_parents, self.interleave_last_var = set(), {}
        self.pk_next_vals = {}
        self.parent_child_assignments, self.fk_population_rates = {}, {}
        self.used_pk_values = {}
        self.pk_counter_lock = threading.Lock()
        self.default_rows_per_table = args.rows if args.rows is not None else 100
        if args.scale is not None:
            self.default_rows_per_table = max(1, int(self.default_rows_per_table * args.scale))
        
        # Parse populate_columns configuration for extended format support
        self.populate_columns_config = {}
        for table_cfg in self.config:
            node = "{0}.{1}".format(table_cfg["schema"], table_cfg["table"])
            self.populate_columns_config[node] = parse_populate_columns_config(table_cfg)
        
        # Global unique value pools: maintained across all batches for UNIQUE columns
        # Format: {"schema.table.column": {"pool": [...], "cursor": 0, "size": N}}
        self.global_unique_value_pools = {}
        self.global_unique_pool_lock = threading.Lock()
        
        # Counters for sequential value generation in composite UNIQUE constraints
        # Format: {"schema.table.column": counter}
        self.composite_unique_counters = {}
        self.composite_unique_counter_lock = threading.Lock()
    
    def introspect(self):
        """Load schema metadata"""
        schemas = set(t["schema"] for t in self.config)
        fk_list = []
        for s in schemas:
            fk_list.extend(load_fk_constraints_for_schema(self.conn, s))
        single_logical, composite_logical = load_logical_fks_from_config(self.config)
        fk_list.extend(single_logical)
        self.logical_composite_fks = composite_logical
        
        config_table_names = set(self.table_map.keys())
        filtered_fks = []
        for fk in fk_list:
            child = "{0}.{1}".format(fk.table_schema, fk.table_name)
            parent = "{0}.{1}". format(fk.referenced_table_schema, fk.referenced_table_name)
            if child in config_table_names:
                table_cfg = self.table_map. get(child, {})
                if not (table_cfg.get("ignore_self_referential_fks", False) and child == parent):
                    filtered_fks.append(fk)
        self.fks = filtered_fks
        
        for fk in self.fks:
            self.fk_columns.setdefault("{0}.{1}".format(fk.table_schema, fk.table_name), set()).add(fk.column_name)
        for comp in self.logical_composite_fks:
            child = "{0}.{1}". format(comp['table_schema'], comp['table_name'])
            for col in comp['child_columns']:
                self.fk_columns. setdefault(child, set()).add(col)
        
        for table_cfg in self.config:
            table_key = "{0}.{1}". format(table_cfg['schema'], table_cfg['table'])
            fk_pop_rates = table_cfg.get("fk_population_rate", {})
            if fk_pop_rates:
                self.fk_population_rates[table_key] = fk_pop_rates
        
        for key in config_table_names:
            schema, table = key.split(".", 1)
            try:
                cols = load_table_columns(self.conn, schema, table)
                pkcols = load_table_pk(self.conn, schema, table)
                engine, auto_inc = load_table_engine_and_ai(self.conn, schema, table)
                unique_cons = load_unique_constraints(self.conn, schema, table)
                if not cols:
                    print("Error: Table {0}.{1} not found". format(schema, table), file=sys.stderr)
                    sys.exit(1)
                self.metadata[key] = TableMeta(schema, table, cols, pkcols, auto_inc is not None, engine)
                self.unique_constraints[key] = unique_cons
                self.unique_value_trackers[key] = {uc.constraint_name: set() for uc in unique_cons}
                if pkcols:
                    self. used_pk_values[key] = set()
                
                # Debug log UNIQUE constraints - distinguish single vs composite
                if unique_cons:
                    single_unique_cols = set()
                    composite_unique_cols = set()
                    for uc in unique_cons:
                        if len(uc.columns) == 1:
                            single_unique_cols.add(uc.columns[0])
                        else:
                            composite_unique_cols.update(uc.columns)
                    debug_print("{0}: Single-column UNIQUE: {1}".format(key, single_unique_cols))
                    if composite_unique_cols:
                        debug_print("{0}: Composite UNIQUE columns: {1}".format(key, composite_unique_cols))
                
                cfg = self.table_map.get(key, {})
                num_rows = cfg.get("rows") or self.default_rows_per_table
                self.generated_rows[key] = [None] * int(num_rows)
            except Exception as e:
                print("Error: Failed to load {0}.{1}: {2}".format(schema, table, e), file=sys.stderr)
                sys.exit(1)
    
    def apply_static_fk_sampling(self):
        for t in self.config:
            for sf in t. get("static_fks", []):
                key = "{0}.{1}.{2}".format(sf['static_schema'], sf['static_table'], sf['static_column'])
                if key not in self.static_samples:
                    self. static_samples[key] = sample_static_fk_values(self.conn, sf["static_schema"], sf["static_table"], sf["static_column"], self.sample_size, self.rng)
    
    def detect_forced_explicit_parents(self):
        child_to_parents = defaultdict(list)
        for fk in self.fks:
            child = "{0}.{1}".format(fk.table_schema, fk.table_name)
            parent = "{0}.{1}". format(fk.referenced_table_schema, fk.referenced_table_name)
            tmeta = self.metadata. get(child)
            if tmeta:
                for c in tmeta.columns:
                    if c.name == fk.column_name and c.is_nullable == "NO":
                        child_to_parents[child].append(parent)
                        break
        for child, parents in child_to_parents.items():
            unique_parents = {p for p in set(parents) if p in self. table_map}
            if len(unique_parents) > 1:
                for p in unique_parents:
                    self.forced_explicit_parents.add(p)
        for comp in self.logical_composite_fks:
            parent_table = "{0}.{1}". format(comp['referenced_table_schema'], comp['referenced_table_name'])
            if parent_table in self.table_map:
                parent_meta = self.metadata.get(parent_table)
                if parent_meta and parent_meta.pk_columns:
                    for ref_col in comp['referenced_columns']:
                        if ref_col in parent_meta.pk_columns:
                            self.forced_explicit_parents. add(parent_table)
                            break
        for table_cfg in self.config:
            if table_cfg. get("explicit_pk", False):
                self.forced_explicit_parents.add("{0}.{1}".format(table_cfg['schema'], table_cfg['table']))
    
    def prepare_pk_sequences(self):
        for tname, tmeta in self.metadata.items():
            if not tmeta.pk_columns or len(tmeta.pk_columns) != 1:
                continue
            pk = tmeta.pk_columns[0]
            if (not tmeta.auto_increment) or (tname in self.forced_explicit_parents):
                cur = self.conn.cursor()
                cur.execute("SELECT AUTO_INCREMENT FROM information_schema. TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (tmeta.schema, tmeta.name))
                r = cur.fetchone()
                auto_inc_next = r[0] if r and r[0] else None
                try:
                    cur.execute("SELECT MAX(`{0}`) FROM `{1}`.`{2}`".format(pk, tmeta.schema, tmeta. name))
                    r2 = cur.fetchone()
                    current_max = r2[0] if r2 and r2[0] else 0
                except:
                    current_max = 0
                start = max(1, auto_inc_next if auto_inc_next and isinstance(auto_inc_next, int) else 1)
                if isinstance(current_max, int):
                    start = max(start, current_max + 1)
                self.pk_next_vals[tname] = start
    
    def validate_not_null_fks(self):
        errors = []
        for fk in self.fks:
            child = "{0}.{1}". format(fk.table_schema, fk.table_name)
            parent = "{0}.{1}".format(fk. referenced_table_schema, fk.referenced_table_name)
            tmeta = self.metadata.get(child)
            if tmeta:
                colmeta = next((c for c in tmeta.columns if c.name == fk.column_name), None)
                if colmeta and colmeta. is_nullable == "NO":
                    cfg = self.table_map.get(child, {})
                    has_static = any(sf["column"] == fk.column_name for sf in cfg.get("static_fks", []))
                    if parent not in self.table_map and not has_static:
                        errors.append((child, fk. column_name, parent, "FK"))
        if errors:
            print("Error: NOT NULL FK columns reference parents not in config:", file=sys.stderr)
            for child, col, parent, fk_type in errors:
                print("  - {0}.{1} -> {2} ({3})".format(child, col, parent, fk_type), file=sys.stderr)
            sys.exit(1)
    
    def validate_conditional_fks(self):
        """Validate that discriminator columns exist in tables with conditional FKs"""
        errors = []
        for fk in self.fks:
            if not fk.condition:
                continue
            
            parsed = parse_fk_condition(fk.condition)
            if not parsed:
                errors.append("{0}.{1}: Invalid condition syntax: {2}".format(
                    fk.table_schema, fk.table_name, fk.condition))
                continue
            
            discriminator_col = parsed['column']
            table_key = "{0}.{1}".format(fk.table_schema, fk.table_name)
            tmeta = self.metadata.get(table_key)
            
            if tmeta:
                col_names = [c.name for c in tmeta.columns]
                if discriminator_col not in col_names:
                    errors.append("{0}: Discriminator column '{1}' in condition '{2}' not found in table. Available columns: {3}".format(
                        table_key, discriminator_col, fk.condition, ', '.join(col_names)))
        
        # Also validate composite FK conditions
        for comp in self.logical_composite_fks:
            condition = comp.get('condition')
            if not condition:
                continue
            
            parsed = parse_fk_condition(condition)
            if not parsed:
                errors.append("{0}.{1}: Invalid condition syntax in composite FK: {2}".format(
                    comp['table_schema'], comp['table_name'], condition))
                continue
            
            discriminator_col = parsed['column']
            table_key = "{0}.{1}".format(comp['table_schema'], comp['table_name'])
            tmeta = self.metadata.get(table_key)
            
            if tmeta:
                col_names = [c.name for c in tmeta.columns]
                if discriminator_col not in col_names:
                    errors.append("{0}: Discriminator column '{1}' in composite FK condition '{2}' not found in table. Available columns: {3}".format(
                        table_key, discriminator_col, condition, ', '.join(col_names)))
        
        if errors:
            print("Error: Conditional FK validation failed:", file=sys.stderr)
            for err in errors:
                print("  - {0}".format(err), file=sys.stderr)
            sys.exit(1)
    
    def generate_batch_fast(self, node, start_idx, end_idx, thread_rng, tmeta, cfg):
        """Generate a batch of rows with guaranteed unique values"""
        rows = []
        table_key = node
        populate_columns = cfg.get("populate_columns") if cfg else None
        fk_cols = self.fk_columns.get(table_key, set())
        
        # Get extended populate_columns configuration
        populate_config = self.populate_columns_config.get(table_key, {})
        
        unique_constraints = self.unique_constraints.get(table_key, [])
        
        single_unique_cols = set()
        composite_unique_constraints = []
        composite_unique_cols = set()  # Track columns in composite UNIQUE constraints separately
        
        for uc in unique_constraints:
            if len(uc.columns) == 1:
                single_unique_cols.add(uc.columns[0])
            else:
                composite_unique_constraints.append(uc)
                composite_unique_cols.update(uc.columns)
        
        all_unique_cols = set(single_unique_cols)
        all_unique_cols.update(composite_unique_cols)
        
        local_trackers = {uc.constraint_name: set() for uc in unique_constraints}
        
        # Identify UNIQUE columns that use global pools (have min/max or values config)
        unique_cols_with_global_pools = set()
        for col_name in single_unique_cols:
            if col_name in populate_config and col_name not in tmeta.pk_columns:
                col_config = populate_config[col_name]
                if "min" in col_config or "values" in col_config:
                    pool_key = "{0}.{1}".format(node, col_name)
                    if pool_key in self.global_unique_value_pools:
                        unique_cols_with_global_pools.add(col_name)
        
        # Identify columns in composite UNIQUE constraints that need sequential generation
        # These are columns that are:
        # 1. Part of a composite UNIQUE constraint
        # 2. NOT controlled by populate_columns (no explicit values or range)
        # 3. NOT a foreign key column
        # 4. NOT a primary key column
        cols_needing_sequential = set()
        
        for uc in composite_unique_constraints:
            controlled_cols_in_constraint = []
            uncontrolled_cols_in_constraint = []
            
            for col_name in uc.columns:
                # Check if column is controlled by populate_columns
                is_controlled = col_name in populate_config and (
                    "values" in populate_config.get(col_name, {}) or
                    "min" in populate_config.get(col_name, {})
                )
                # Also treat FK columns and PK columns as "controlled"
                is_fk = col_name in fk_cols
                is_pk = col_name in tmeta.pk_columns
                
                if is_controlled or is_fk or is_pk:
                    controlled_cols_in_constraint.append(col_name)
                else:
                    uncontrolled_cols_in_constraint.append(col_name)
            
            if uncontrolled_cols_in_constraint and controlled_cols_in_constraint:
                # There's at least one controlled column and at least one uncontrolled column
                # The uncontrolled column(s) need sequential generation
                for col_name in uncontrolled_cols_in_constraint:
                    cols_needing_sequential.add(col_name)
                
                # Debug output: Log which columns will use sequential generation (only once per table)
                if start_idx == 0:
                    debug_print("{0}: Composite UNIQUE {1} will use sequential generation for uncontrolled columns: {2}".format(
                        node, uc.constraint_name, uncontrolled_cols_in_constraint))
        
        for batch_idx in range(start_idx, end_idx):
            row = {}
            
            for col in tmeta.columns:
                cname = col.name
                
                if cname in tmeta.pk_columns and tmeta.auto_increment and table_key not in self.forced_explicit_parents:
                    row[cname] = None
                    continue
                
                if cfg:
                    found_static = False
                    for sf in cfg.get("static_fks", []):
                        if sf["column"] == cname:
                            key = "{0}.{1}.{2}".format(sf['static_schema'], sf['static_table'], sf['static_column'])
                            pool = self.static_samples.get(key, [])
                            row[cname] = rand_choice(thread_rng, pool)
                            found_static = True
                            break
                    if found_static:
                        continue
                
                if cname in fk_cols:
                    row[cname] = None
                    continue
                
                dtype = (col.data_type or "").lower()
                
                # PRIORITY 0: Sequential generation for uncontrolled columns in composite UNIQUE constraints
                # This prevents collisions when generating large datasets
                if cname in cols_needing_sequential:
                    counter_key = "{0}.{1}".format(node, cname)
                    with self.composite_unique_counter_lock:
                        if counter_key not in self.composite_unique_counters:
                            self.composite_unique_counters[counter_key] = 0
                        counter_val = self.composite_unique_counters[counter_key]
                        self.composite_unique_counters[counter_key] += 1
                    
                    # Get max length for string types with safe conversion
                    try:
                        maxlen = int(col.char_max_length) if col.char_max_length else 255
                    except (ValueError, TypeError):
                        maxlen = 255
                    
                    # Format: seq_{counter} with zero-padding for better sorting
                    # Use enough digits to handle large row counts (10 million = 8 digits)
                    if dtype in ("varchar", "char", "text", "mediumtext", "longtext"):
                        seq_value = "seq_{0:08d}".format(counter_val)
                        row[cname] = seq_value[:maxlen]
                    elif "int" in dtype or dtype in ("bigint", "smallint", "mediumint", "tinyint"):
                        row[cname] = counter_val
                    else:
                        # For other types, use string representation
                        seq_value = "seq_{0:08d}".format(counter_val)
                        row[cname] = seq_value[:maxlen] if maxlen else seq_value
                    continue
                
                is_in_unique = cname in all_unique_cols
                
                # Check if column is in populate_columns (either simple or extended format)
                if col.is_nullable == "YES" and not is_in_unique:
                    if populate_columns is None or cname not in populate_config:
                        continue
                
                base_value = None
                
                # PRIORITY 1: Use global unique value pool (thread-safe)
                if cname in unique_cols_with_global_pools:
                    pool_key = "{0}.{1}".format(node, cname)
                    pool_data = self.global_unique_value_pools[pool_key]
                    
                    with self.global_unique_pool_lock:
                        cursor = pool_data['cursor']
                        if cursor < pool_data['size']:
                            row[cname] = pool_data['pool'][cursor]
                            pool_data['cursor'] += 1
                            # Debug progress logging every 1000 values
                            if pool_data['cursor'] % 1000 == 0:
                                debug_print("{0}: Used {1}/{2} unique values for column {3}".format(
                                    node, pool_data['cursor'], pool_data['size'], cname))
                        else:
                            # Pool exhausted - this shouldn't happen if range is sufficient
                            print("ERROR: {0}: Exhausted unique value pool for {1} at row {2}".format(
                                node, cname, batch_idx), file=sys.stderr)
                            row[cname] = None
                    continue
                
                # PRIORITY 2: Check if this column has extended configuration (but not a global pool)
                col_config = populate_config.get(cname)
                
                if col_config and ("values" in col_config or "min" in col_config):
                    # Use extended configuration to generate value
                    base_value = generate_value_with_config(thread_rng, col, col_config)
                    
                    # Handle unique constraint for string types with extended config
                    # Only append suffix for single-column UNIQUE, not composite UNIQUE
                    if cname in single_unique_cols and dtype in ("varchar", "char", "text", "mediumtext", "longtext") and base_value is not None:
                        maxlen = int(col.char_max_length) if col.char_max_length else 255
                        base_str = str(base_value)
                        suffix = "_{0}".format(batch_idx)
                        max_base_len = maxlen - len(suffix)
                        if max_base_len < 1:
                            row[cname] = suffix[1:maxlen]
                        else:
                            row[cname] = (base_str[:max_base_len] + suffix)[:maxlen]
                        continue
                    
                    row[cname] = base_value
                    continue
                
                # Default value generation (unchanged from original logic)
                if "int" in dtype or dtype in ("bigint", "smallint", "mediumint", "tinyint"):
                    # Only use batch_idx for single-column UNIQUE, not composite UNIQUE
                    if cname in single_unique_cols:
                        row[cname] = batch_idx
                        continue
                    else:
                        base_value = thread_rng.randint(18, 80) if re.search(r"age|years? ", cname, re.I) else thread_rng.randint(0, 10000)
                elif dtype in ("decimal", "numeric", "float", "double"):
                    prec, scale = int(col.numeric_precision or 10), int(col.numeric_scale or 0)
                    base_value = rand_decimal_str(thread_rng, prec, scale)
                elif dtype in ("varchar", "char", "text", "mediumtext", "longtext"):
                    lname = cname.lower()
                    if "email" in lname:
                        base_value = rand_email(thread_rng)
                    elif "name" in lname:
                        base_value = rand_name(thread_rng)
                    elif "phone" in lname:
                        base_value = rand_phone(thread_rng)
                    else:
                        maxlen = int(col.char_max_length) if col.char_max_length else 24
                        base_value = rand_string(thread_rng, min(maxlen, 24))
                    
                    # Only append suffix for single-column UNIQUE, not composite UNIQUE
                    if cname in single_unique_cols and base_value is not None:
                        maxlen = int(col.char_max_length) if col.char_max_length else 255
                        base_str = str(base_value)
                        suffix = "_{0}".format(batch_idx)
                        max_base_len = maxlen - len(suffix)
                        if max_base_len < 1:
                            row[cname] = suffix[1:maxlen]
                        else:
                            row[cname] = (base_str[:max_base_len] + suffix)[:maxlen]
                        continue
                elif dtype in ("date", "datetime", "timestamp"):
                    base_value = rand_datetime(thread_rng). split(" ")[0] if dtype == "date" else rand_datetime(thread_rng)
                elif dtype == "enum":
                    m = re.findall(r"'((?:[^']|(?:''))*)'", col.column_type or "")
                    vals = [v.replace("''", "'") for v in m]
                    base_value = rand_choice(thread_rng, vals)
                elif dtype == "set":
                    # Parse SET values from column_type: SET('val1','val2','val3')
                    m = re.findall(r"'((?:[^']|(?:''))*)'", col.column_type or "")
                    set_values = [v.replace("''", "'") for v in m]
                    
                    if set_values:
                        # Generate random subset: select 0 to N values
                        num_values_to_select = thread_rng.randint(0, len(set_values))
                        
                        if num_values_to_select == 0:
                            base_value = ''  # Empty set
                        else:
                            # Shuffle and select subset
                            shuffled = list(set_values)
                            thread_rng.shuffle(shuffled)
                            selected = shuffled[:num_values_to_select]
                            
                            # Sort selected values to maintain consistent ordering
                            # (MySQL SET internally orders values by definition order)
                            # Re-sort by original position in set_values
                            selected_sorted = [v for v in set_values if v in selected]
                            base_value = ','.join(selected_sorted)
                    else:
                        base_value = ''  # No valid SET values, use empty
                elif col.is_nullable == "NO":
                    base_value = rand_string(thread_rng, 8)
                
                row[cname] = base_value
            
            if table_key in self.pk_next_vals and tmeta.pk_columns and len(tmeta.pk_columns) == 1:
                pk = tmeta.pk_columns[0]
                with self.pk_counter_lock:
                    row[pk] = self.pk_next_vals[table_key]
                    self.pk_next_vals[table_key] += 1
            
            valid = True
            for uc in unique_constraints:
                combo_tuple = tuple(row. get(col) for col in uc.columns)
                if not any(v is None for v in combo_tuple):
                    if combo_tuple in local_trackers[uc.constraint_name]:
                        print("CRITICAL ERROR: Duplicate in {0}.{1}: {2} at batch_idx={3}".format(
                            node, uc.constraint_name, combo_tuple, batch_idx), file=sys.stderr)
                        valid = False
                        break
                    local_trackers[uc.constraint_name]. add(combo_tuple)
            
            if valid:
                rows.append(row)
            else:
                print("ERROR: Skipping row at batch_idx {0}".format(batch_idx), file=sys.stderr)
        
        return rows
    
    def merge_unique_trackers(self, node):
        """Merge unique value tracking from generated rows"""
        rows = self.generated_rows. get(node, [])
        if not rows:
            return
        
        for row in rows:
            if not row:
                continue
            for uc in self.unique_constraints. get(node, []):
                value_tuple = tuple(row.get(col) for col in uc.columns)
                if not any(v is None for v in value_tuple):
                    self.unique_value_trackers[node][uc.constraint_name].add(value_tuple)
    
    def initialize_global_unique_pools(self, node, num_rows):
        """Initialize global unique value pools for a table's UNIQUE columns.
        
        This creates a single pool for each UNIQUE column that is shared across all batches,
        ensuring no duplicates within the same column across the entire table.
        """
        tmeta = self.metadata.get(node)
        if not tmeta:
            return
        
        unique_constraints = self.unique_constraints.get(node, [])
        populate_config = self.populate_columns_config.get(node, {})
        
        # Identify single-column UNIQUE constraints
        single_unique_cols = set()
        for uc in unique_constraints:
            if len(uc.columns) == 1:
                single_unique_cols.add(uc.columns[0])
        
        # Create global pools for UNIQUE columns that have min/max or values config
        for col_name in single_unique_cols:
            if col_name in populate_config and col_name not in tmeta.pk_columns:
                col_config = populate_config[col_name]
                # Only pre-allocate if config has min/max or values (not just column name)
                if "min" in col_config or "values" in col_config:
                    pool_key = "{0}.{1}".format(node, col_name)
                    
                    # Skip if pool already exists (shouldn't happen but defensive)
                    if pool_key in self.global_unique_value_pools:
                        continue
                    
                    col_meta = next((c for c in tmeta.columns if c.name == col_name), None)
                    if col_meta:
                        # Generate pool for ALL rows, not just batch size
                        unique_pool = generate_unique_value_pool(col_meta, col_config, num_rows, self.rng)
                        
                        if len(unique_pool) < num_rows:
                            print("WARNING: {0}: UNIQUE column {1} has insufficient unique values ({2} available, {3} needed)".format(
                                node, col_name, len(unique_pool), num_rows), file=sys.stderr)
                            print("  Consider expanding the range or reducing row count", file=sys.stderr)
                        
                        # Store pool with cursor at 0
                        self.global_unique_value_pools[pool_key] = {
                            'pool': unique_pool,
                            'cursor': 0,
                            'size': len(unique_pool)
                        }
                        
                        debug_print("{0}: Created global unique pool for {1} with {2} values".format(
                            node, col_name, len(unique_pool)))
    
    def generate_parallel(self, order, rows_per_table):
        """Generate all rows in parallel with chunked processing"""
        max_workers = self.args.threads
        
        debug_print("Generating rows with {0} threads... ".format(max_workers))
        
        for node in order:
            tmeta = self.metadata.get(node)
            if not tmeta:
                continue
            
            cfg = self.table_map.get(node)
            num_rows = rows_per_table. get(node, self.default_rows_per_table)
            
            # Initialize global unique value pools for this table BEFORE generating rows
            self.initialize_global_unique_pools(node, num_rows)
            
            if num_rows < 1000 or max_workers == 1:
                thread_rng = random.Random(self.args.seed + hash(node))
                rows = self.generate_batch_fast(node, 0, num_rows, thread_rng, tmeta, cfg)
                self.generated_rows[node] = rows
            else:
                chunk_size = max(100, num_rows // (max_workers * 4))
                chunks = [(i, min(i + chunk_size, num_rows)) for i in range(0, num_rows, chunk_size)]
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {}
                    for start_idx, end_idx in chunks:
                        thread_rng = random.Random(self.args.seed + hash(node) + start_idx)
                        future = executor.submit(self. generate_batch_fast, node, start_idx, end_idx, thread_rng, tmeta, cfg)
                        futures[future] = (start_idx, end_idx)
                    
                    all_rows = []
                    for future in as_completed(futures):
                        batch_rows = future.result()
                        all_rows.extend(batch_rows)
                    
                    self.generated_rows[node] = all_rows
            
            self.merge_unique_trackers(node)
            debug_print("Generated {0} rows for {1}".format(len(self.generated_rows[node]), node))
    
    def find_composite_fks_for_child(self, child_table):
        """Find composite FKs for a child table"""
        return [comp for comp in self.logical_composite_fks 
                if "{0}.{1}".format(comp['table_schema'], comp['table_name']) == child_table]
    
    def resolve_fks_batch(self, node, tmeta, cfg):
        """Resolve FKs with pre-allocated parent values for PK-FK columns"""
        rows = self.generated_rows[node]
        if not rows:
            return rows
        
        all_fk_columns = self.fk_columns.get(node, set())
        pk_fk_columns = set(tmeta.pk_columns) & all_fk_columns
        
        # Track which columns are in unique constraints (needed to avoid overwriting batch_idx values)
        unique_constraints = self.unique_constraints.get(node, [])
        all_unique_cols = set()
        for uc in unique_constraints:
            all_unique_cols.update(uc.columns)
        
        parent_caches = {}
        # For conditional FKs, store parent values keyed by constraint name
        conditional_fk_caches = {}
        for fk in self.fks:
            if "{0}.{1}".format(fk.table_schema, fk.table_name) == node:
                parent_table = "{0}.{1}".format(fk.referenced_table_schema, fk.referenced_table_name)
                if parent_table in self.generated_rows:
                    parent_rows = self.generated_rows[parent_table]
                    parent_col = fk.referenced_column_name
                    parent_vals = [r.get(parent_col) for r in parent_rows if r and r.get(parent_col) is not None]
                    if fk.condition:
                        # Conditional FK - store by constraint name
                        conditional_fk_caches[fk.constraint_name] = parent_vals
                    else:
                        # Unconditional FK - store by column name (backward compatible)
                        parent_caches[fk.column_name] = parent_vals
        
        # Group conditional FKs by column for priority resolution
        conditional_fks_by_column = defaultdict(list)
        for fk in self.fks:
            if "{0}.{1}".format(fk.table_schema, fk.table_name) == node and fk.condition:
                conditional_fks_by_column[fk.column_name].append(fk)
        
        # For columns with conditional FKs, combine all parent values into parent_caches
        # This enables Cartesian product generation with the full pool of possible values
        for fk_col, fk_list in conditional_fks_by_column.items():
            # Only populate parent_caches if there's no unconditional FK for this column
            if fk_col not in parent_caches:
                all_parent_vals = []
                for fk in fk_list:
                    cached_vals = conditional_fk_caches.get(fk.constraint_name, [])
                    all_parent_vals.extend(cached_vals)
                if all_parent_vals:
                    # Use unique values for Cartesian product
                    parent_caches[fk_col] = list(set(all_parent_vals))
                    debug_print("{0}: Conditional FK column {1} has {2} total unique parent values from {3} tables".format(
                        node, fk_col, len(parent_caches[fk_col]), len(fk_list)))
        
        composite_cfgs = self.find_composite_fks_for_child(node)
        composite_columns_all = set()
        for comp in composite_cfgs:
            composite_columns_all.update(comp["child_columns"])
        
        parent_row_caches = {}
        for comp in composite_cfgs:
            parent_table = "{0}.{1}". format(comp['referenced_table_schema'], comp['referenced_table_name'])
            if parent_table in self.generated_rows and parent_table not in parent_row_caches:
                parent_row_caches[parent_table] = self.generated_rows[parent_table]
        
        # Identify composite FKs that contain PK columns for hybrid Cartesian product
        composite_fk_with_pk = []
        composite_fk_pk_cols = set()
        for comp in composite_cfgs:
            fk_child_cols = set(comp["child_columns"])
            pk_overlap = fk_child_cols & set(tmeta.pk_columns)
            if pk_overlap:
                composite_fk_with_pk.append(comp)
                composite_fk_pk_cols.update(pk_overlap)
        
        # Pre-allocated PK tuples for composite PK with single-column FKs
        pre_allocated_pk_tuples = None
        pre_allocated_pk = None
        # Track which single-column FK-PK columns have pre-allocated values
        # (for partial tuple assignment when some PK columns are in composite FKs)
        pre_allocated_pk_cols = None
        
        if pk_fk_columns:
            debug_print("{0}: PK columns {1} are also FK columns - pre-allocating values".format(node, pk_fk_columns))
            
            if len(tmeta.pk_columns) == 1 and tmeta.pk_columns[0] in pk_fk_columns:
                # Single-column PK that is also an FK - existing logic
                pk_col = tmeta.pk_columns[0]
                parent_vals = parent_caches.get(pk_col, [])
                unique_parent_vals = list(set(parent_vals))
                
                if len(unique_parent_vals) < len(rows):
                    print("WARNING: {0} needs {1} rows but parent only has {2} unique values for PK-FK column {3}".format(
                        node, len(rows), len(unique_parent_vals), pk_col), file=sys.stderr)
                    print("  Truncating to {0} rows".format(len(unique_parent_vals)), file=sys.stderr)
                    rows = rows[:len(unique_parent_vals)]
                
                self.rng.shuffle(unique_parent_vals)
                pre_allocated_pk = unique_parent_vals[:len(rows)]
            elif len(tmeta.pk_columns) > 1:
                # Multi-column PK - check which PK columns are single-column FKs (not composite FKs)
                pk_cols_that_are_single_fks = set()
                for pk_col in tmeta.pk_columns:
                    if pk_col in pk_fk_columns and pk_col not in composite_columns_all:
                        pk_cols_that_are_single_fks.add(pk_col)
                
                # Debug output for PK column classification
                debug_print("{0}: PK columns: {1}".format(node, tmeta.pk_columns))
                debug_print("{0}: PK-FK columns: {1}".format(node, pk_fk_columns))
                debug_print("{0}: Composite FK columns: {1}".format(node, composite_columns_all))
                debug_print("{0}: Single-column FK-PK columns: {1}".format(node, pk_cols_that_are_single_fks))
                debug_print("{0}: Composite FKs with PK overlap: {1}".format(node, [c['constraint_name'] for c in composite_fk_with_pk]))
                
                # Generate hybrid Cartesian product if:
                # - At least 1 PK column is a single-column FK, OR
                # - Composite FKs contain PK columns that can be combined
                # This enables optimization when some PK columns are in composite FKs
                if pk_cols_that_are_single_fks or composite_fk_with_pk:
                    debug_print("{0}: {1} PK column(s) are single-column FKs - attempting hybrid Cartesian product".format(
                        node, len(pk_cols_that_are_single_fks)))
                    
                    # Build ordered list of single-column FK-PK columns (preserving PK column order)
                    ordered_single_fk_pk_cols = [col for col in tmeta.pk_columns if col in pk_cols_that_are_single_fks]
                    
                    # Build pools of unique FK values for each single-column FK-PK column
                    pk_value_pools = []
                    pool_sizes = []
                    for pk_col in ordered_single_fk_pk_cols:
                        parent_vals = parent_caches.get(pk_col, [])
                        unique_vals = list(set(parent_vals))
                        
                        debug_print("{0}: PK-FK column {1} has {2} parent values, {3} unique".format(
                            node, pk_col, len(parent_vals), len(unique_vals)))
                        
                        if not unique_vals:
                            print("WARNING: {0}: No parent values available for PK-FK column {1}".format(node, pk_col), file=sys.stderr)
                            print("  Parent cache keys: {0}".format(list(parent_caches.keys())), file=sys.stderr)
                            print("  This PK-FK column will prevent Cartesian product generation", file=sys.stderr)
                            pk_value_pools = []
                            break
                        pk_value_pools.append(unique_vals)
                        pool_sizes.append(len(unique_vals))
                    
                    # Extract unique combinations from composite FKs that contain PK columns
                    composite_fk_pk_combos = []
                    composite_fk_pk_combo_cols = []  # Ordered list of PK columns from composite FKs
                    
                    if composite_fk_with_pk:
                        # For each composite FK with PK overlap, extract unique parent combinations
                        for comp in composite_fk_with_pk:
                            parent_table = "{0}.{1}".format(comp['referenced_table_schema'], comp['referenced_table_name'])
                            parent_rows = parent_row_caches.get(parent_table, [])
                            
                            # Build enum validators for child columns to filter parent rows
                            enum_validators = {}
                            for child_col, parent_col in zip(comp["child_columns"], comp["referenced_columns"]):
                                child_col_meta = next((c for c in tmeta.columns if c.name == child_col), None)
                                if child_col_meta and child_col_meta.data_type and child_col_meta.data_type.lower() == "enum":
                                    m = re.findall(r"'((?:[^']|(?:''))*)'", child_col_meta.column_type or "")
                                    valid_values = set([v.replace("''", "'") for v in m])
                                    enum_validators[parent_col] = valid_values
                            
                            # Filter parent rows by enum constraints before extracting combinations
                            # Note: Rows with NULL values in enum columns are excluded since NULL is not a valid enum value
                            if enum_validators:
                                original_count = len(parent_rows)
                                valid_parent_rows = []
                                for pr in parent_rows:
                                    if pr and all(pr.get(pcol) in valid_vals for pcol, valid_vals in enum_validators.items()):
                                        valid_parent_rows.append(pr)
                                parent_rows = valid_parent_rows
                                debug_print("{0}: Filtered parent rows from {1} to {2} based on enum constraints".format(
                                    node, original_count, len(parent_rows)))
                            
                            # Get PK columns that are in this composite FK (preserving PK column order)
                            pk_cols_in_this_fk = [col for col in tmeta.pk_columns if col in comp["child_columns"]]
                            # Map child columns to parent columns
                            child_to_parent_map = dict(zip(comp["child_columns"], comp["referenced_columns"]))
                            parent_cols_for_pk = [child_to_parent_map[col] for col in pk_cols_in_this_fk]
                            
                            debug_print("{0}: Composite FK {1} maps PK columns {2} to parent columns {3}".format(
                                node, comp['constraint_name'], pk_cols_in_this_fk, parent_cols_for_pk))
                            
                            # Extract unique combinations from FILTERED parent rows
                            unique_combos = set()
                            for parent_row in parent_rows:
                                if parent_row:
                                    combo = tuple(parent_row.get(pcol) for pcol in parent_cols_for_pk)
                                    if not any(v is None for v in combo):
                                        unique_combos.add(combo)
                            
                            if unique_combos:
                                composite_fk_pk_combos = list(unique_combos)
                                composite_fk_pk_combo_cols = pk_cols_in_this_fk
                                debug_print("{0}: Found {1} unique composite FK combinations for PK columns {2}".format(
                                    node, len(composite_fk_pk_combos), composite_fk_pk_combo_cols))
                                # Use first composite FK with PK overlap; subsequent ones ignored
                                # (multiple overlapping composite FKs would require more complex merging)
                                break
                    
                    # Generate hybrid Cartesian product if we have data
                    can_generate = (pk_value_pools or composite_fk_pk_combos)
                    
                    if can_generate:
                        # Calculate maximum possible combinations
                        max_combinations = 1
                        
                        if composite_fk_pk_combos:
                            max_combinations *= len(composite_fk_pk_combos)
                            debug_print("{0}: Composite FK contributes {1} combinations".format(
                                node, len(composite_fk_pk_combos)))
                        
                        for size in pool_sizes:
                            max_combinations *= size
                        
                        debug_print("{0}: Total max combinations: {1}".format(node, max_combinations))
                        
                        needed_rows = len(rows)
                        if max_combinations < needed_rows:
                            print("WARNING: {0} needs {1} rows but only {2} unique PK combinations available".format(
                                node, needed_rows, max_combinations), file=sys.stderr)
                            print("  Truncating to {0} rows".format(max_combinations), file=sys.stderr)
                            rows = rows[:max_combinations]
                            needed_rows = max_combinations
                        
                        # Generate hybrid Cartesian product
                        all_pk_cols_in_order = []
                        
                        # Build combined list of all PK columns being pre-allocated (in PK column order)
                        for pk_col in tmeta.pk_columns:
                            if pk_col in composite_fk_pk_combo_cols or pk_col in ordered_single_fk_pk_cols:
                                all_pk_cols_in_order.append(pk_col)
                        
                        # Generate hybrid Cartesian product: composite FK combos × single-column FK values
                        if composite_fk_pk_combos and pk_value_pools:
                            # Hybrid: composite FK combos × single-column FK values
                            debug_print("{0}: Generating hybrid Cartesian product".format(node))
                            
                            if needed_rows < max_combinations and max_combinations > 100000:
                                # Random sampling for large pools
                                debug_print("{0}: Using random sampling ({1} of {2} combinations)".format(
                                    node, needed_rows, max_combinations))
                                
                                used_combos = set()
                                pre_allocated_pk_tuples = []
                                
                                # Shuffle for randomness
                                self.rng.shuffle(composite_fk_pk_combos)
                                for pool in pk_value_pools:
                                    self.rng.shuffle(pool)
                                
                                max_attempts = needed_rows * 10
                                attempts = 0
                                while len(pre_allocated_pk_tuples) < needed_rows and attempts < max_attempts:
                                    # Pick random composite combo
                                    comp_combo = self.rng.choice(composite_fk_pk_combos)
                                    # Pick random single-column values
                                    single_combo = tuple(self.rng.choice(pool) for pool in pk_value_pools)
                                    
                                    # Merge in PK column order
                                    full_combo = []
                                    comp_idx, single_idx = 0, 0
                                    for pk_col in all_pk_cols_in_order:
                                        if pk_col in composite_fk_pk_combo_cols:
                                            col_pos = composite_fk_pk_combo_cols.index(pk_col)
                                            full_combo.append(comp_combo[col_pos])
                                        elif pk_col in ordered_single_fk_pk_cols:
                                            col_pos = ordered_single_fk_pk_cols.index(pk_col)
                                            full_combo.append(single_combo[col_pos])
                                    
                                    full_combo = tuple(full_combo)
                                    if full_combo not in used_combos:
                                        used_combos.add(full_combo)
                                        pre_allocated_pk_tuples.append(full_combo)
                                    attempts += 1
                                
                                if len(pre_allocated_pk_tuples) < needed_rows:
                                    # Fallback to full generation
                                    debug_print("{0}: Random sampling got {1}, falling back to full generation".format(
                                        node, len(pre_allocated_pk_tuples)))
                                    all_combinations = []
                                    for comp_combo in composite_fk_pk_combos:
                                        for single_combo in itertools.product(*pk_value_pools):
                                            full_combo = []
                                            for pk_col in all_pk_cols_in_order:
                                                if pk_col in composite_fk_pk_combo_cols:
                                                    col_pos = composite_fk_pk_combo_cols.index(pk_col)
                                                    full_combo.append(comp_combo[col_pos])
                                                elif pk_col in ordered_single_fk_pk_cols:
                                                    col_pos = ordered_single_fk_pk_cols.index(pk_col)
                                                    full_combo.append(single_combo[col_pos])
                                            all_combinations.append(tuple(full_combo))
                                    self.rng.shuffle(all_combinations)
                                    pre_allocated_pk_tuples = all_combinations[:needed_rows]
                            else:
                                # Full Cartesian product
                                all_combinations = []
                                for comp_combo in composite_fk_pk_combos:
                                    for single_combo in itertools.product(*pk_value_pools):
                                        full_combo = []
                                        for pk_col in all_pk_cols_in_order:
                                            if pk_col in composite_fk_pk_combo_cols:
                                                col_pos = composite_fk_pk_combo_cols.index(pk_col)
                                                full_combo.append(comp_combo[col_pos])
                                            elif pk_col in ordered_single_fk_pk_cols:
                                                col_pos = ordered_single_fk_pk_cols.index(pk_col)
                                                full_combo.append(single_combo[col_pos])
                                        all_combinations.append(tuple(full_combo))
                                self.rng.shuffle(all_combinations)
                                pre_allocated_pk_tuples = all_combinations[:needed_rows]
                        
                        elif composite_fk_pk_combos:
                            # Only composite FK combos (no single-column FK-PK columns)
                            debug_print("{0}: Using only composite FK combinations".format(node))
                            all_pk_cols_in_order = composite_fk_pk_combo_cols
                            self.rng.shuffle(composite_fk_pk_combos)
                            pre_allocated_pk_tuples = composite_fk_pk_combos[:needed_rows]
                        
                        elif pk_value_pools:
                            # Only single-column FK-PK columns (original logic path)
                            debug_print("{0}: Using only single-column FK Cartesian product".format(node))
                            all_pk_cols_in_order = ordered_single_fk_pk_cols
                            
                            if needed_rows < max_combinations and max_combinations > 100000:
                                # Random sampling
                                debug_print("{0}: Using random sampling ({1} of {2} combinations)".format(
                                    node, needed_rows, max_combinations))
                                
                                used_combos = set()
                                pre_allocated_pk_tuples = []
                                
                                for pool in pk_value_pools:
                                    self.rng.shuffle(pool)
                                
                                max_attempts = needed_rows * 10
                                attempts = 0
                                while len(pre_allocated_pk_tuples) < needed_rows and attempts < max_attempts:
                                    combo = tuple(self.rng.choice(pool) for pool in pk_value_pools)
                                    if combo not in used_combos:
                                        used_combos.add(combo)
                                        pre_allocated_pk_tuples.append(combo)
                                    attempts += 1
                                
                                if len(pre_allocated_pk_tuples) < needed_rows:
                                    debug_print("{0}: Random sampling got {1}, falling back to full generation".format(
                                        node, len(pre_allocated_pk_tuples)))
                                    all_combinations = list(itertools.product(*pk_value_pools))
                                    self.rng.shuffle(all_combinations)
                                    pre_allocated_pk_tuples = all_combinations[:needed_rows]
                            else:
                                all_combinations = list(itertools.product(*pk_value_pools))
                                self.rng.shuffle(all_combinations)
                                pre_allocated_pk_tuples = all_combinations[:needed_rows]
                        
                        # Store the column order for tuple assignment
                        pre_allocated_pk_cols = all_pk_cols_in_order
                        debug_print("{0}: Pre-allocated {1} unique PK tuples for columns {2}".format(
                            node, len(pre_allocated_pk_tuples), pre_allocated_pk_cols))
                    else:
                        # No data available for Cartesian product
                        debug_print("{0}: Cannot generate Cartesian product - missing parent values".format(node))
                        debug_print("{0}: All FK columns: {1}".format(node, all_fk_columns))
                        debug_print("{0}: Parent caches available for: {1}".format(node, list(parent_caches.keys())))
                else:
                    # No single-column FK-PK columns and no composite FKs with PK overlap
                    debug_print("{0}: No single-column FK-PK columns and no composite FKs with PK overlap".format(node))
        
        filtered_parent_caches = {}
        for comp in composite_cfgs:
            fk_child_cols = comp["child_columns"]
            parent_table = "{0}.{1}". format(comp['referenced_table_schema'], comp['referenced_table_name'])
            parent_cols = comp["referenced_columns"]
            parent_rows = parent_row_caches. get(parent_table, [])
            
            if parent_rows:
                enum_validators = {}
                for child_col, parent_col in zip(fk_child_cols, parent_cols):
                    child_col_meta = next((c for c in tmeta. columns if c.name == child_col), None)
                    if child_col_meta and child_col_meta.data_type and child_col_meta.data_type. lower() == "enum":
                        m = re.findall(r"'((?:[^']|(?:''))*)'", child_col_meta.column_type or "")
                        valid_values = set([v.replace("''", "'") for v in m])
                        enum_validators[parent_col] = valid_values
                
                if enum_validators:
                    valid_parent_rows = []
                    for pr in parent_rows:
                        if pr and all(pr.get(pcol) in valid_vals for pcol, valid_vals in enum_validators. items()):
                            valid_parent_rows.append(pr)
                    filtered_parent_caches[comp['constraint_name']] = valid_parent_rows
                    
                    if not valid_parent_rows:
                        print("Warning: No valid parent rows in {0} for {1} due to enum restrictions".format(
                            parent_table, node), file=sys.stderr)
                else:
                    filtered_parent_caches[comp['constraint_name']] = parent_rows
        
        # Detect composite FKs that overlap with composite PKs (requires uniqueness tracking)
        composite_pk_fk_overlap = {}
        for comp in composite_cfgs:
            fk_child_cols = set(comp["child_columns"])
            pk_cols = set(tmeta.pk_columns)
            overlap = fk_child_cols & pk_cols
            
            # Check if Cartesian product pre-assignment already ensures PK uniqueness.
            # If pre_allocated_pk_tuples exists, some PK columns have been pre-assigned
            # and their uniqueness is already guaranteed by the Cartesian product.
            if overlap and len(tmeta.pk_columns) > 1 and pre_allocated_pk_tuples and pre_allocated_pk_cols:
                pre_assigned_pk_set = set(pre_allocated_pk_cols)
                overlap_with_pre_assigned = overlap & pre_assigned_pk_set
                # Skip restrictive overlap checking if either:
                # - The composite FK's overlap includes pre-assigned columns (uniqueness already handled)
                # - We have at least 2 pre-assigned PK columns (guarantees sufficient uniqueness)
                should_skip_overlap_check = overlap_with_pre_assigned or len(pre_assigned_pk_set) >= 2
                if should_skip_overlap_check:
                    debug_print("{0}: Skipping composite PK-FK overlap check for {1} - pre-assigned columns {2} ensure uniqueness".format(
                        node, comp['constraint_name'], pre_allocated_pk_cols))
                    continue
            
            if overlap and len(tmeta.pk_columns) > 1:
                # This composite FK overlaps with a multi-column PK
                composite_pk_fk_overlap[comp['constraint_name']] = {
                    'fk_cols': comp["child_columns"],
                    'pk_cols': tmeta.pk_columns,
                    'overlap': overlap
                }
        
        # Track used PK combinations for composite PK uniqueness
        used_composite_pk_combos = set()
        
        # Track which composite FKs have been skipped (for logging purposes)
        logged_skipped_fks = set()
        
        resolved_rows = []
        skipped_rows = 0
        
        for row_idx, row in enumerate(rows):
            if not row:
                continue
            
            temp_row = dict(row)
            row_skipped = False
            
            # If we have pre-allocated PK tuples (at least 2 single-column FK-PK columns),
            # assign them first to guarantee partial PK uniqueness.
            # Remaining PK columns (in composite FKs) will be assigned by composite FK resolution.
            if pre_allocated_pk_tuples and row_idx < len(pre_allocated_pk_tuples):
                pk_tuple = pre_allocated_pk_tuples[row_idx]
                # Use pre_allocated_pk_cols which contains only the single-column FK-PK columns
                for col_idx, pk_col in enumerate(pre_allocated_pk_cols):
                    temp_row[pk_col] = pk_tuple[col_idx]
            
            for comp in composite_cfgs:
                fk_child_cols = comp["child_columns"]
                parent_table = "{0}.{1}".format(comp['referenced_table_schema'], comp['referenced_table_name'])
                parent_cols = comp["referenced_columns"]
                valid_parent_rows = filtered_parent_caches.get(comp['constraint_name'], [])
                
                if not valid_parent_rows:
                    continue
                
                # Check if any FK columns were pre-assigned by batch_idx for uniqueness
                skip_this_fk = False
                for child_col in fk_child_cols:
                    if child_col in all_unique_cols and temp_row.get(child_col) is not None:
                        child_col_meta = next((c for c in tmeta.columns if c. name == child_col), None)
                        if child_col_meta:
                            dtype = (child_col_meta. data_type or "").lower()
                            if "int" in dtype or dtype in ("bigint", "smallint", "mediumint", "tinyint"):
                                skip_this_fk = True
                                break
                
                if skip_this_fk:
                    continue
                
                # Skip composite FK if all its PK-overlapping columns were pre-assigned via hybrid Cartesian
                if pre_allocated_pk_tuples and pre_allocated_pk_cols:
                    fk_pk_overlap = set(fk_child_cols) & set(tmeta.pk_columns)
                    pre_assigned_set = set(pre_allocated_pk_cols)
                    if fk_pk_overlap and fk_pk_overlap.issubset(pre_assigned_set):
                        if comp['constraint_name'] not in logged_skipped_fks:
                            debug_print("{0}: Skipping composite FK {1} - PK columns {2} already pre-assigned via hybrid Cartesian product".format(
                                node, comp['constraint_name'], fk_pk_overlap))
                            logged_skipped_fks.add(comp['constraint_name'])
                        continue
                
                has_pk_fk = any(child_col in pk_fk_columns for child_col in fk_child_cols)
                
                if has_pk_fk and pre_allocated_pk:
                    pk_col_in_composite = next(c for c in fk_child_cols if c in pk_fk_columns)
                    pk_idx_in_composite = fk_child_cols.index(pk_col_in_composite)
                    parent_col_for_pk = parent_cols[pk_idx_in_composite]
                    
                    target_val = pre_allocated_pk[row_idx]
                    matching_parent_rows = [pr for pr in valid_parent_rows if pr and pr.get(parent_col_for_pk) == target_val]
                    
                    if matching_parent_rows:
                        parent_row = self.rng.choice(matching_parent_rows)
                        for child_col, parent_col in zip(fk_child_cols, parent_cols):
                            temp_row[child_col] = parent_row.get(parent_col)
                elif comp['constraint_name'] in composite_pk_fk_overlap:
                    # This composite FK overlaps with composite PK - need to ensure uniqueness
                    pk_cols = tmeta.pk_columns
                    
                    # Shuffle parent rows to get random selection
                    shuffled_parents = list(valid_parent_rows)
                    self.rng.shuffle(shuffled_parents)
                    
                    found_valid_parent = False
                    for parent_row in shuffled_parents:
                        if not parent_row:
                            continue
                        
                        # Simulate assignment and check PK uniqueness
                        test_row = dict(temp_row)
                        for child_col, parent_col in zip(fk_child_cols, parent_cols):
                            test_row[child_col] = parent_row.get(parent_col)
                        
                        pk_tuple = tuple(test_row.get(col) for col in pk_cols)
                        
                        if pk_tuple not in used_composite_pk_combos:
                            # This parent maintains PK uniqueness - use it
                            for child_col, parent_col in zip(fk_child_cols, parent_cols):
                                temp_row[child_col] = parent_row.get(parent_col)
                            used_composite_pk_combos.add(pk_tuple)
                            found_valid_parent = True
                            break
                    
                    if not found_valid_parent:
                        # No parent row found that maintains uniqueness
                        row_skipped = True
                        skipped_rows += 1
                        break
                else:
                    parent_row = self.rng.choice(valid_parent_rows)
                    for child_col, parent_col in zip(fk_child_cols, parent_cols):
                        temp_row[child_col] = parent_row.get(parent_col)
            
            if row_skipped:
                continue
            
            # Track which columns have been assigned by conditional FKs
            assigned_by_conditional_fk = set()
            
            # First, resolve conditional FKs - evaluate conditions and apply matching FK
            for fk_col, fk_list in conditional_fks_by_column.items():
                if fk_col in composite_columns_all:
                    continue
                if cfg and any(sf["column"] == fk_col for sf in cfg.get("static_fks", [])):
                    continue
                if pre_allocated_pk_tuples and fk_col in pk_fk_columns:
                    continue
                
                # Find the first FK whose condition matches
                for fk in fk_list:
                    if evaluate_fk_condition(fk.condition, temp_row):
                        parent_vals = conditional_fk_caches.get(fk.constraint_name, [])
                        if parent_vals:
                            temp_row[fk_col] = self.rng.choice(parent_vals)
                            assigned_by_conditional_fk.add(fk_col)
                            debug_print("{0}: Conditional FK {1} matched (condition: {2}), assigned {3}={4}".format(
                                node, fk.constraint_name, fk.condition, fk_col, temp_row[fk_col]))
                            break  # Found matching FK, stop checking others for this column
                        else:
                            debug_print("{0}: Conditional FK {1} matched but no parent values available".format(
                                node, fk.constraint_name))
                    else:
                        debug_print("{0}: Skipping FK {1} - condition not met ({2})".format(
                            node, fk.constraint_name, fk.condition))
            
            # Then, resolve unconditional FKs (skip columns already handled by conditional FKs)
            for fk in self.fks:
                if "{0}.{1}".format(fk.table_schema, fk.table_name) != node:
                    continue
                
                # Skip conditional FKs - they were handled above
                if fk.condition:
                    continue
                
                fk_col = fk.column_name
                
                # Skip if already assigned by a conditional FK
                if fk_col in assigned_by_conditional_fk:
                    continue
                
                if fk_col in composite_columns_all:
                    continue
                if cfg and any(sf["column"] == fk_col for sf in cfg.get("static_fks", [])):
                    continue
                
                # Skip if this FK column was already assigned via pre_allocated_pk_tuples
                if pre_allocated_pk_tuples and fk_col in pk_fk_columns:
                    continue
                
                col_meta = next((c for c in tmeta.columns if c.name == fk_col), None)
                if col_meta and col_meta.is_nullable == "NO":
                    if pre_allocated_pk and fk_col in pk_fk_columns:
                        temp_row[fk_col] = pre_allocated_pk[row_idx]
                    else:
                        parent_vals = parent_caches.get(fk_col, [])
                        temp_row[fk_col] = self.rng.choice(parent_vals) if parent_vals else None
            
            resolved_rows.append(temp_row)
        
        if skipped_rows > 0:
            print("WARNING: {0}: Skipped {1} rows due to insufficient unique parent combinations for composite PK-FK".format(
                node, skipped_rows), file=sys.stderr)
        
        return resolved_rows
    
    def _generate_deletes(self, order):
        """Generate DELETE statements for cleanup"""
        self.delete_sql_lines = []
        for node in reversed(order):
            tmeta = self.metadata.get(node)
            if not tmeta:
                continue
            self.delete_sql_lines.append("\n-- Deleting rows from {0}\n".format(node))
            rows = self.generated_rows.get(node, [])
            for row in rows:
                if not row:
                    continue
                if tmeta.pk_columns and len(tmeta.pk_columns) == 1:
                    pk = tmeta.pk_columns[0]
                    pkval = row.get(pk)
                    if pkval is not None and not (isinstance(pkval, str) and pkval.startswith("@")):
                        self.delete_sql_lines.append("DELETE FROM `{0}`.`{1}` WHERE `{2}` = {3};\n".format(
                            tmeta.schema, tmeta.name, pk, sql_literal(pkval)))
                        continue
                clauses = []
                for col in tmeta.columns:
                    v = row.get(col. name)
                    if v is not None and not (isinstance(v, str) and v.startswith("@")):
                        clauses.append("`{0}` = {1}".format(col.name, sql_literal(v)))
                if clauses:
                    self.delete_sql_lines.append("DELETE FROM `{0}`.`{1}` WHERE {2};\n".format(
                        tmeta.schema, tmeta.name, ' AND '.join(clauses)))
    
    def generate(self):
        """Main generation pipeline"""
        self.introspect()
        self.apply_static_fk_sampling()
        self.detect_forced_explicit_parents()
        self.prepare_pk_sequences()
        self.validate_not_null_fks()
        self.validate_conditional_fks()
        
        nodes, edges = build_dependency_graph(self.config, self.fks, self.logical_composite_fks)
        order = topo_sort(nodes, edges)
        
        for tn, tmeta in self.metadata.items():
            if tmeta.auto_increment and tn not in self.forced_explicit_parents:
                self.interleave_last_var[tn] = "@last_{0}_{1}".format(slugify(tmeta.schema), slugify(tmeta.name))
        
        rows_per_table = {}
        for node in order:
            cfg = self.table_map.get(node, {})
            rows = cfg.get("rows") or self.default_rows_per_table
            rows_per_table[node] = int(rows)
        
        self.generate_parallel(order, rows_per_table)
        
        debug_print("Resolving FKs and generating SQL...")
        for node in order:
            tmeta = self.metadata.get(node)
            if not tmeta:
                continue
            cfg = self.table_map.get(node)
            
            rows = self.resolve_fks_batch(node, tmeta, cfg)
            self.generated_rows[node] = rows
            
            if rows:
                interleave = (node in self.interleave_last_var) and (node not in self.forced_explicit_parents)
                
                cols_to_include = []
                for col in tmeta.columns:
                    skip_col = col.name in tmeta.pk_columns and tmeta.auto_increment and node not in self.forced_explicit_parents and all(r.get(col.name) is None for r in rows)
                    if not skip_col and any(r.get(col.name) is not None for r in rows):
                        cols_to_include.append(col. name)
                
                self.insert_sql_lines. append("\n-- Inserting {0} rows into {1}\n".format(len(rows), node))
                
                if interleave:
                    for row in rows:
                        row_values = [row.get(c) for c in cols_to_include]
                        self.insert_sql_lines.append(render_insert_statement(tmeta.schema, tmeta.name, cols_to_include, [row_values], False))
                        self.insert_sql_lines. append("SET {0} = LAST_INSERT_ID();\n".format(self.interleave_last_var[node]))
                else:
                    batch_size = self.args.batch_size
                    for i in range(0, len(rows), batch_size):
                        chunk = rows[i:i+batch_size]
                        rows_values = [[r.get(c) for c in cols_to_include] for r in chunk]
                        self. insert_sql_lines.append(render_insert_statement(
                            tmeta.schema, tmeta.name, cols_to_include, rows_values, True, 
                            max_rows_per_statement=batch_size))
                
                debug_print("Generated SQL for {0}".format(node))
        
        debug_print("Generating DELETE statements...")
        self._generate_deletes(order)
    
    def write_output(self, out_sql_path, out_delete_path=None):
        header = "-- Synthetic data generated {0}Z\n-- Host: {1}, Seed: {2}, Threads: {3}, Batch: {4}\n\n".format(
            datetime.utcnow().isoformat(), self.args.src_host, self.args.seed, self.args.threads, self.args.batch_size)
        try:
            with open(out_sql_path, "w", encoding="utf-8") as f:
                f.write(header)
                f.writelines(self.insert_sql_lines)
                f.write("\n-- End of inserts\n")
        except Exception as e:
            print("Error writing {0}: {1}".format(out_sql_path, e), file=sys.stderr)
            sys.exit(1)
        
        if out_delete_path:
            try:
                with open(out_delete_path, "w", encoding="utf-8") as f:
                    f.write("-- DELETE statements (reverse order)\n-- WARNING: Review before running!\n\n")
                    f.writelines(self.delete_sql_lines)
                    f.write("\n-- End of deletes\n")
            except Exception as e:
                print("Error writing {0}: {1}". format(out_delete_path, e), file=sys.stderr)
                sys.exit(1)

def parse_args():
    p = argparse.ArgumentParser(description="Generate synthetic SQL data from MySQL schema (optimized version)")
    p.add_argument("--config", required=True, help="JSON config file path")
    p.add_argument("--src-host", required=True, help="MySQL host")
    p.add_argument("--src-user", required=True, help="MySQL user")
    p.add_argument("--out-sql", required=True, help="Output INSERT SQL file")
    p.add_argument("--src-port", type=int, default=3306, help="MySQL port (default: 3306)")
    p.add_argument("--src-password", default=None, help="MySQL password")
    p.add_argument("--ask-pass", action="store_true", help="Prompt for password")
    p. add_argument("--out-delete", default=None, help="Output DELETE SQL file")
    p.add_argument("--rows", type=int, default=None, help="Rows per table (default: 100)")
    p.add_argument("--scale", type=float, default=None, help="Scale rows by factor")
    p.add_argument("--sample-size", type=int, default=1000, help="Static FK sample size")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    p.add_argument("--hmac-key", default=None, help="HMAC key for pseudonymization")
    p.add_argument("--threads", type=int, default=4, help="Number of parallel threads (default: 4)")
    p.add_argument("--batch-size", type=int, default=100, help="Rows per INSERT statement (default: 100)")
    p.add_argument("--debug", action="store_true", help="Enable debug output")
    return p.parse_args()

def main():
    args = parse_args()
    GLOBALS["debug"] = args.debug
    cfg = load_config(args.config)
    conn = connect_mysql(args)
    try:
        gen = FastSyntheticGenerator(conn, args, cfg)
        gen.generate()
        gen.write_output(args.out_sql, args. out_delete)
        print("✓ Wrote INSERT statements to {0}".format(args.out_sql))
        if args.out_delete:
            print("✓ Wrote DELETE statements to {0}".format(args.out_delete))
        print("✓ Generated data for {0} table(s) using {1} threads with batch size {2}".format(
            len(cfg), args.threads, args.batch_size))
    except Exception as e:
        print("Error: {0}".format(e), file=sys.stderr)
        if GLOBALS["debug"]:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
