#!/usr/bin/env python3
"""Utility functions and data structures for synthetic data generation"""
import hashlib, hmac, re, random
from datetime import datetime, timedelta
from collections import namedtuple

GLOBALS = {"debug": False}

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
