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
    return [FKMeta(*r, is_logical=False) for r in cur.fetchall()]

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
                single_fks.append(FKMeta(lfk. get("constraint_name", "LOGICAL_{0}_{1}".format(tname, cname)), tschema, tname, cname, ref_schema, ref_table, ref_column, True))
            elif "child_columns" in lfk and "referenced_columns" in lfk:
                child_cols, parent_cols = tuple(lfk["child_columns"]), tuple(lfk["referenced_columns"])
                ref_schema, ref_table = lfk["referenced_schema"], lfk["referenced_table"]
                if ignore_self_refs and ref_schema == tschema and ref_table == tname:
                    continue
                composite_fks.append({"constraint_name": lfk.get("constraint_name", "LOGICAL_{0}_{1}".format(tname, '_'.join(child_cols))), "table_schema": tschema, "table_name": tname, "child_columns": child_cols, "referenced_table_schema": ref_schema, "referenced_table_name": ref_table, "referenced_columns": parent_cols, "population_rate": lfk.get("population_rate")})
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
    
    def generate_batch_fast(self, node, start_idx, end_idx, thread_rng, tmeta, cfg):
        """Generate a batch of rows with guaranteed unique values"""
        rows = []
        table_key = node
        populate_columns = cfg.get("populate_columns") if cfg else None
        fk_cols = self.fk_columns.get(table_key, set())
        
        unique_constraints = self.unique_constraints.get(table_key, [])
        
        single_unique_cols = set()
        composite_unique_constraints = []
        
        for uc in unique_constraints:
            if len(uc.columns) == 1:
                single_unique_cols.add(uc.columns[0])
            else:
                composite_unique_constraints.append(uc)
        
        all_unique_cols = set(single_unique_cols)
        for uc in composite_unique_constraints:
            all_unique_cols.update(uc.columns)
        
        local_trackers = {uc.constraint_name: set() for uc in unique_constraints}
        
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
                
                is_in_unique = cname in all_unique_cols
                
                if col.is_nullable == "YES" and not is_in_unique:
                    if populate_columns is None or cname not in populate_columns:
                        continue
                
                base_value = None
                
                if "int" in dtype or dtype in ("bigint", "smallint", "mediumint", "tinyint"):
                    if is_in_unique:
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
                    
                    if is_in_unique and base_value is not None:
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
        for fk in self.fks:
            if "{0}.{1}".format(fk.table_schema, fk.table_name) == node:
                parent_table = "{0}.{1}". format(fk.referenced_table_schema, fk.referenced_table_name)
                if parent_table in self.generated_rows:
                    parent_rows = self.generated_rows[parent_table]
                    parent_col = fk.referenced_column_name
                    parent_vals = [r.get(parent_col) for r in parent_rows if r and r.get(parent_col) is not None]
                    parent_caches[fk. column_name] = parent_vals
        
        composite_cfgs = self.find_composite_fks_for_child(node)
        composite_columns_all = set()
        for comp in composite_cfgs:
            composite_columns_all.update(comp["child_columns"])
        
        parent_row_caches = {}
        for comp in composite_cfgs:
            parent_table = "{0}.{1}". format(comp['referenced_table_schema'], comp['referenced_table_name'])
            if parent_table in self.generated_rows and parent_table not in parent_row_caches:
                parent_row_caches[parent_table] = self.generated_rows[parent_table]
        
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
                
                # Generate Cartesian product if at least 2 PK columns are single-column FKs
                # This enables optimization even when some PK columns are in composite FKs
                if len(pk_cols_that_are_single_fks) >= 2:
                    # At least 2 PK columns are single-column FKs - generate Cartesian product for those
                    debug_print("{0}: {1} of {2} PK columns are single-column FKs - generating Cartesian product".format(
                        node, len(pk_cols_that_are_single_fks), len(tmeta.pk_columns)))
                    
                    # Build ordered list of single-column FK-PK columns (preserving PK column order)
                    ordered_single_fk_pk_cols = [col for col in tmeta.pk_columns if col in pk_cols_that_are_single_fks]
                    
                    # Build pools of unique FK values for each single-column FK-PK column
                    pk_value_pools = []
                    pool_sizes = []
                    for pk_col in ordered_single_fk_pk_cols:
                        parent_vals = parent_caches.get(pk_col, [])
                        unique_vals = list(set(parent_vals))
                        if not unique_vals:
                            print("WARNING: {0}: No parent values available for PK-FK column {1}".format(node, pk_col), file=sys.stderr)
                            pk_value_pools = []
                            break
                        pk_value_pools.append(unique_vals)
                        pool_sizes.append(len(unique_vals))
                    
                    if pk_value_pools:
                        # Calculate maximum possible combinations
                        max_combinations = 1
                        for size in pool_sizes:
                            max_combinations *= size
                        
                        debug_print("{0}: FK value pools: {1}, max combinations: {2}".format(
                            node, 
                            dict(zip(ordered_single_fk_pk_cols, pool_sizes)),
                            max_combinations))
                        
                        needed_rows = len(rows)
                        if max_combinations < needed_rows:
                            print("WARNING: {0} needs {1} rows but only {2} unique PK combinations available from FK values".format(
                                node, needed_rows, max_combinations), file=sys.stderr)
                            print("  FK column pool sizes: {0}".format(dict(zip(ordered_single_fk_pk_cols, pool_sizes))), file=sys.stderr)
                            print("  Truncating to {0} rows".format(max_combinations), file=sys.stderr)
                            rows = rows[:max_combinations]
                            needed_rows = max_combinations
                        
                        # Optimize: if we need fewer rows than total combinations, 
                        # generate randomly instead of full Cartesian product
                        if needed_rows < max_combinations and max_combinations > 100000:
                            # For large pools, sample randomly to avoid memory issues
                            debug_print("{0}: Using random sampling ({1} of {2} combinations)".format(
                                node, needed_rows, max_combinations))
                            
                            used_combos = set()
                            pre_allocated_pk_tuples = []
                            
                            # Shuffle pools for randomness
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
                                # Fallback: generate all combinations if random sampling failed
                                debug_print("{0}: Random sampling got {1}, falling back to full generation".format(
                                    node, len(pre_allocated_pk_tuples)))
                                all_combinations = list(itertools.product(*pk_value_pools))
                                self.rng.shuffle(all_combinations)
                                pre_allocated_pk_tuples = all_combinations[:needed_rows]
                        else:
                            # Generate all combinations using Cartesian product
                            all_combinations = list(itertools.product(*pk_value_pools))
                            self.rng.shuffle(all_combinations)
                            
                            # Take only as many as we need
                            pre_allocated_pk_tuples = all_combinations[:needed_rows]
                        
                        # Store the column order for partial tuple assignment
                        pre_allocated_pk_cols = ordered_single_fk_pk_cols
                        debug_print("{0}: Pre-allocated {1} unique PK tuples for columns {2}".format(
                            node, len(pre_allocated_pk_tuples), pre_allocated_pk_cols))
        
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
            
            for fk in self.fks:
                if "{0}.{1}".format(fk.table_schema, fk.table_name) != node:
                    continue
                
                fk_col = fk.column_name
                if fk_col in composite_columns_all:
                    continue
                if cfg and any(sf["column"] == fk_col for sf in cfg.get("static_fks", [])):
                    continue
                
                # Skip if this FK column was already assigned via pre_allocated_pk_tuples
                if pre_allocated_pk_tuples and fk_col in pk_fk_columns:
                    continue
                
                col_meta = next((c for c in tmeta.columns if c. name == fk_col), None)
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
