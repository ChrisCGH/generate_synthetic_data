#!/usr/bin/env python3
"""Utility functions and data structures for synthetic data generation"""
import hashlib, hmac, re, random, sys
from datetime import datetime, timedelta
from collections import namedtuple

GLOBALS = {"debug": False}


def parse_date(date_str):
    """
    Parse date string in various formats.
    Supports: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, ISO format
    
    Returns: datetime object or None if parsing fails
    """
    if not date_str:
        return None
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def parse_populate_columns_config(table_cfg):
    """
    Parse populate_columns configuration supporting both formats:
    - String: "column_name" (backward compatible)
    - Object: {"column": "name", "min": X, "max": Y} or {"column": "name", "values": [...]}
    
    Returns: dict mapping column_name -> config_object
    """
    populate_cols = {}
    for item in table_cfg.get("populate_columns", []):
        if isinstance(item, str):
            # Backward compatible: simple column name
            populate_cols[item] = {"column": item}
        elif isinstance(item, dict):
            # Extended format
            col_name = item.get("column")
            if col_name:
                populate_cols[col_name] = item
            else:
                print("WARNING: populate_columns entry missing 'column' field: {0}".format(item), file=sys.stderr)
    return populate_cols


def validate_populate_column_config(col_meta, config):
    """
    Validate that the configuration is appropriate for the column type.
    
    Args:
        col_meta: ColumnMeta object
        config: dict with configuration for the column
    
    Returns: bool indicating if configuration is valid (warnings are printed but don't fail)
    """
    if not config:
        return True
    
    dtype = (col_meta.data_type or "").lower()
    
    if "values" in config and "min" in config:
        print("WARNING: Column {0} has both 'values' and 'min/max' - 'values' will take precedence".format(
            col_meta.name), file=sys.stderr)
    
    if "min" in config and "max" in config:
        min_val = config["min"]
        max_val = config["max"]
        
        # Type-specific validation for integer types
        if dtype in ("int", "integer", "bigint", "smallint", "tinyint", "mediumint"):
            if not isinstance(min_val, int) or not isinstance(max_val, int):
                print("WARNING: Column {0} is integer type but min/max are not integers".format(
                    col_meta.name), file=sys.stderr)
        
        # Min < Max validation for numeric types
        if dtype in ("int", "integer", "bigint", "smallint", "tinyint", "mediumint", 
                     "decimal", "numeric", "float", "double", "real"):
            if min_val >= max_val:
                print("ERROR: Column {0} has min >= max ({1} >= {2})".format(
                    col_meta.name, min_val, max_val), file=sys.stderr)
                return False
        
        # Date validation
        if dtype in ("date", "datetime", "timestamp"):
            min_date = parse_date(str(min_val))
            max_date = parse_date(str(max_val))
            if min_date is None:
                print("ERROR: Column {0} has invalid min date format: {1}".format(
                    col_meta.name, min_val), file=sys.stderr)
                return False
            if max_date is None:
                print("ERROR: Column {0} has invalid max date format: {1}".format(
                    col_meta.name, max_val), file=sys.stderr)
                return False
            if min_date >= max_date:
                print("ERROR: Column {0} has min date >= max date ({1} >= {2})".format(
                    col_meta.name, min_val, max_val), file=sys.stderr)
                return False
    
    return True

def debug_print(*args, **kwargs):
    if GLOBALS["debug"]:
        print("[DEBUG]", *args, **kwargs)

def slugify(s):
    return re.sub(r"[^0-9a-zA-Z_]+", "_", s or "")

def hmac_hex(key_bytes, value):
    return hmac.new(key_bytes, value.encode("utf-8"), hashlib.sha256).hexdigest()

def pseudonymize_value(value, key_bytes, kind="generic"):
    if value is None:
        return None
    if kind == "email" and "@" in value:
        user, domain = value.split("@", 1)
        return "{0}@{1}".format(hmac_hex(key_bytes, user)[:16], domain)
    if kind == "phone":
        digits = re.sub(r"\D", "", value)
        h = hmac_hex(key_bytes, digits or value)[:10]
        return "{0}-{1}-{2}".format(h[:3], h[3:6], h[6:10])
    return hmac_hex(key_bytes, value)[:24]

def rand_choice(rng, seq):
    return rng.choice(seq) if seq else None

def rand_decimal_str(rng, precision, scale):
    whole_digits = precision - scale
    max_whole = 10**whole_digits - 1 if whole_digits > 0 else 0
    whole_part = 0 if max_whole <= 0 else rng.randint(0, max_whole)
    if scale > 0:
        frac_part = rng.randint(0, 10**scale - 1)
        return "{0}.{1}".format(whole_part, str(frac_part). zfill(scale))
    return str(whole_part)

def rand_string(rng, length=12):
    return "".join(rng.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(length))

def rand_name(rng):
    firsts = ["Alice","Bob","Charlie","Dana","Eve","Frank","Grace","Heidi","Ivan","Judy"]
    lasts = ["Smith","Johnson","Williams","Jones","Brown","Davis","Miller","Wilson"]
    return "{0} {1}".format(rng.choice(firsts), rng.choice(lasts))

def rand_email(rng, name=None):
    domains = ["example.com","example.org","test.com"]
    if name:
        uname = re.sub(r"[^a-z0-9]", ".", name. lower()). strip(".")
        return "{0}@{1}". format(uname[:16], rng.choice(domains))
    return "{0}@{1}".format(rand_string(rng,8). lower(), rng.choice(domains))

def rand_phone(rng):
    return "{0}-{1}-{2}".format(rng.randint(200,999), rng.randint(200,999), str(rng.randint(0,9999)). zfill(4))

def rand_datetime(rng, start_year=2010, end_year=None):
    if end_year is None:
        end_year = datetime.utcnow().year
    start = datetime(start_year,1,1)
    end = datetime(end_year,12,31,23,59,59)
    delta = end - start
    secs = rng.randint(0, int(delta.total_seconds()))
    return (start + timedelta(seconds=secs)).strftime("%Y-%m-%d %H:%M:%S")


def generate_value_with_config(rng, col, config=None):
    """
    Generate a random value for a column, optionally using extended configuration.
    
    Args:
        rng: Random number generator
        col: ColumnMeta object
        config: Optional dict with 'min', 'max', or 'values' keys
    
    Returns: Generated value appropriate for the column type
    """
    if config is None:
        config = {}
    
    dtype = (col.data_type or "").lower()
    
    # Check if specific values are provided
    if "values" in config:
        debug_print("Column {0}: Using values list {1}".format(col.name, config["values"]))
        return rng.choice(config["values"])
    
    # Check for min/max range
    min_val = config.get("min")
    max_val = config.get("max")
    has_range = min_val is not None and max_val is not None
    
    # Handle integer types with ranges
    if "int" in dtype or dtype in ("bigint", "smallint", "mediumint", "tinyint"):
        if has_range:
            debug_print("Column {0}: Using int range [{1}, {2}]".format(col.name, min_val, max_val))
            return rng.randint(int(min_val), int(max_val))
        # Default integer generation
        if re.search(r"age|years? ", col.name, re.I):
            return rng.randint(18, 80)
        return rng.randint(0, 10000)
    
    # Handle decimal/float types with ranges
    elif dtype in ("decimal", "numeric", "float", "double", "real"):
        if has_range:
            debug_print("Column {0}: Using decimal range [{1}, {2}]".format(col.name, min_val, max_val))
            return round(rng.uniform(float(min_val), float(max_val)), 2)
        # Default decimal generation
        prec = int(col.numeric_precision or 10)
        scale = int(col.numeric_scale or 0)
        return rand_decimal_str(rng, prec, scale)
    
    # Handle date/datetime/timestamp types with ranges
    elif dtype in ("date", "datetime", "timestamp"):
        if has_range:
            min_date = parse_date(str(min_val))
            max_date = parse_date(str(max_val))
            if min_date and max_date:
                debug_print("Column {0}: Using date range [{1}, {2}]".format(col.name, min_val, max_val))
                delta = max_date - min_date
                random_days = rng.randint(0, max(0, delta.days))
                random_date = min_date + timedelta(days=random_days)
                
                if dtype == "date":
                    return random_date.strftime("%Y-%m-%d")
                else:  # datetime/timestamp
                    # Add random time component
                    random_seconds = rng.randint(0, 86399)
                    random_datetime = random_date + timedelta(seconds=random_seconds)
                    return random_datetime.strftime("%Y-%m-%d %H:%M:%S")
        # Default date generation
        return rand_datetime(rng).split(" ")[0] if dtype == "date" else rand_datetime(rng)
    
    # Handle string types
    elif dtype in ("varchar", "char", "text", "mediumtext", "longtext"):
        lname = col.name.lower()
        if "email" in lname:
            return rand_email(rng)
        elif "name" in lname:
            return rand_name(rng)
        elif "phone" in lname:
            return rand_phone(rng)
        else:
            maxlen = int(col.char_max_length) if col.char_max_length else 24
            return rand_string(rng, min(maxlen, 24))
    
    # Handle enum types
    elif dtype == "enum":
        m = re.findall(r"'((?:[^']|(?:''))*)'", col.column_type or "")
        vals = [v.replace("''", "'") for v in m]
        return rng.choice(vals) if vals else None
    
    # Default: return random string for non-nullable columns
    elif col.is_nullable == "NO":
        return rand_string(rng, 8)
    
    return None


ColumnMeta = namedtuple("ColumnMeta", ["name","data_type","is_nullable","column_type","column_key","extra","char_max_length","numeric_precision","numeric_scale","column_default"])
FKMeta = namedtuple("FKMeta", ["constraint_name","table_schema","table_name","column_name","referenced_table_schema","referenced_table_name","referenced_column_name","is_logical","condition"])
TableMeta = namedtuple("TableMeta", ["schema","name","columns","pk_columns","auto_increment","engine"])
UniqueConstraint = namedtuple("UniqueConstraint", ["constraint_name","columns"])

def parse_fk_condition(condition_str):
    """
    Parse a simple FK condition like "T = 'some_string'"
    Returns: dict with 'column', 'operator', 'value' or None if parsing fails
    """
    if not condition_str:
        return None
    
    # Simple equality check: "column = 'value'"
    match = re.match(r"^\s*(\w+)\s*=\s*'([^']*)'\s*$", condition_str)
    if match:
        return {
            'column': match.group(1),
            'operator': '=',
            'value': match.group(2)
        }
    
    # Add support for other patterns as needed
    return None

def evaluate_fk_condition(condition_str, row):
    """
    Evaluate a FK condition against a row.
    Returns True if condition is met, False otherwise.
    If condition is None or empty, returns True (unconditional FK).
    """
    if not condition_str:
        return True
    
    parsed = parse_fk_condition(condition_str)
    if not parsed:
        debug_print("WARNING: Could not parse condition: {0}".format(condition_str))
        return False
    
    discriminator_col = parsed['column']
    discriminator_value = row.get(discriminator_col)
    
    if parsed['operator'] == '=':
        return discriminator_value == parsed['value']
    
    return False

def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        if value.startswith("@") and re.match(r"^@[0-9A-Za-z_]+$", value):
            return value
        return "'" + value.replace("'", "''") + "'"
    return str(value)

def render_insert_statement(schema, table, colnames, rows_values, multirow=True, max_rows_per_statement=1000):
    """Render INSERT with configurable batch size to avoid max_allowed_packet"""
    if not rows_values:
        return ""
    cols = ",".join("`{0}`".format(c) for c in colnames)
    
    if multirow and len(rows_values) > 1:
        statements = []
        for i in range(0, len(rows_values), max_rows_per_statement):
            chunk = rows_values[i:i+max_rows_per_statement]
            vals = ["(" + ",".join(sql_literal(v) for v in rv) + ")" for rv in chunk]
            statements.append("INSERT INTO `{0}`. `{1}` ({2}) VALUES\n{3};\n".format(
                schema, table, cols, ",\n".join(vals)))
        return "".join(statements)
    else:
        stmts = []
        for rv in rows_values:
            vals = "(" + ",".join(sql_literal(v) for v in rv) + ")"
            stmts.append("INSERT INTO `{0}`.`{1}` ({2}) VALUES {3};".format(schema, table, cols, vals))
        return "\n".join(stmts) + "\n"
