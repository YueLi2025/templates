#!/usr/bin/env python3
"""
SQL canonicalizer: variable-stripping templatization.
Preserves exact SQL structure; replaces only identifiers and literals with placeholders.
Rules: tables -> table_name, columns -> col_name,
      table aliases -> table_alias_placeholder0,1,..., column aliases -> col_alias_placeholder0,1,...
      numbers -> NUM, strings -> STR, dates/times -> DATE.
"""

import re
import csv
import json
import os
import sys
from io import StringIO

# Try sqlparse for tokenization (optional)
try:
    import sqlparse
    from sqlparse.sql import IdentifierList, Identifier, Token
    from sqlparse.tokens import Keyword, Name, Number, String, Literal
    HAS_SQLPARSE = True
except ImportError:
    HAS_SQLPARSE = False


def _split_comments(sql: str):
    """Split SQL into segments (is_comment, text). Comments are preserved as-is."""
    segments = []
    i = 0
    n = len(sql)
    in_single = False
    in_double = False
    code_start = 0
    while i < n:
        if in_single:
            if sql[i] == "'" and (i + 1 >= n or sql[i + 1] != "'"):
                in_single = False
            elif sql[i] == "\\":
                i += 1  # skip escaped char
            i += 1
            continue
        if in_double:
            if sql[i] == '"' and (i + 1 >= n or sql[i + 1] != '"'):
                in_double = False
            elif sql[i] == "\\":
                i += 1
            i += 1
            continue
        if not in_single and not in_double and i + 2 <= n and sql[i : i + 2] == "--":
            if i > code_start:
                segments.append((False, sql[code_start:i]))
            start = i
            i += 2
            while i < n and sql[i] != "\n":
                i += 1
            if i < n:
                i += 1
            segments.append((True, sql[start:i]))
            code_start = i
            continue
        if not in_single and not in_double and i + 2 <= n and sql[i : i + 2] == "/*":
            if i > code_start:
                segments.append((False, sql[code_start:i]))
            start = i
            i += 2
            while i < n - 1 and sql[i : i + 2] != "*/":
                i += 1
            i = i + 2 if i < n - 1 else n
            segments.append((True, sql[start:i]))
            code_start = i
            continue
        if sql[i] == "'" and not in_double:
            in_single = True
        elif sql[i] == '"' and not in_single:
            in_double = True
        i += 1
    if code_start < n:
        segments.append((False, sql[code_start:n]))
    return segments


def _replace_literals(sql: str) -> str:
    """Replace string, date, and numeric literals with placeholders. Preserves structure."""
    out = sql

    # 1. Date/timestamp/time literals (before string literals to avoid matching quote in DATE '...')
    # DATE '...', TIMESTAMP '...', TIME '...', INTERVAL '...'
    date_pattern = r"(?i)\b(DATE|TIMESTAMP|TIME|INTERVAL)\s*'([^']|'')*'"
    out = re.sub(date_pattern, r"DATE", out)

    # 2. String literals: single-quoted (allow '' for escape)
    def repl_str_single(m):
        return "STR"
    out = re.sub(r"'([^']|'')*'", repl_str_single, out)
    # Double-quoted strings (often identifiers in standard SQL but some DBs use for strings)
    out = re.sub(r'"([^"\\]|\\.)*"', repl_str_single, out)

    # 3. Numbers: integer, decimal, scientific
    out = re.sub(r"\b\d+\.?\d*([eE][-+]?\d+)?\b", "NUM", out)
    out = re.sub(r"\b\.\d+([eE][-+]?\d+)?\b", "NUM", out)

    return out


def _is_numeric_token(w: str) -> bool:
    """True if token looks like a number (so we don't treat it as column when parsing raw SQL)."""
    if not w:
        return False
    if w.isdigit():
        return True
    try:
        float(w)
        return True
    except ValueError:
        return False


def _build_boolean_col_refs(schema, tables, alias_to_table):
    """
    Build set of strings that denote a boolean column reference in SQL.
    schema: dict (table_name, column_name) -> type. Returns list of ref strings, sorted by length desc.
    """
    if not schema:
        return []
    refs = set()
    tables_set = set(tables)
    for (t, c), typ in schema.items():
        if typ.lower() not in ("boolean", "bool"):
            continue
        refs.add(f"{t}.{c}")
        for alias, tbl in alias_to_table.items():
            if tbl == t:
                refs.add(f"{alias}.{c}")
    for (t, c), typ in schema.items():
        if typ.lower() not in ("boolean", "bool"):
            continue
        if t in tables_set:
            refs.add(c)
    return sorted(refs, key=len, reverse=True)


def _replace_literals_bird(sql: str, boolean_col_refs: list) -> str:
    """
    Replace literals with placeholders: num, string, date, boolean (lowercase).
    When a literal is compared to a boolean column (in condition), use "boolean".
    """
    out = sql
    for ref in boolean_col_refs:
        ref_esc = re.escape(ref)
        out = re.sub(r"\b" + ref_esc + r"\s*=\s*(\d+)\b", ref + " = boolean", out)
        out = re.sub(r"\b" + ref_esc + r"\s*=\s*'([^']|'')*'", ref + " = boolean", out)
        out = re.sub(r"\b(\d+)\s*=\s*" + ref_esc + r"\b", "boolean = " + ref, out)
        out = re.sub(r"'([^']|'')*'\s*=\s*" + ref_esc + r"\b", "boolean = " + ref, out)
    date_pattern = r"(?i)\b(DATE|TIMESTAMP|TIME|INTERVAL)\s*'([^']|'')*'"
    out = re.sub(date_pattern, "date", out)
    out = re.sub(r"'([^']|'')*'", "string", out)
    out = re.sub(r'"([^"\\]|\\.)*"', "string", out)
    out = re.sub(r"\b\d+\.?\d*([eE][-+]?\d+)?\b", "num", out)
    out = re.sub(r"\b\.\d+([eE][-+]?\d+)?\b", "num", out)
    return out


def _collect_identifiers_regex(sql: str, raw_sql: bool = False):
    """
    Collect table names, table aliases, column aliases, and column names using regex context.
    Returns (tables, table_alias_map, col_alias_map, columns, alias_to_table).
    If raw_sql=True, do not add numeric-only tokens to columns (for use before literal replacement).
    """
    cte_ctx = r'(?i)\bWITH\s+(\w+)\s+AS\s+'
    not_table_alias_kw = {
        'ON', 'USING', 'WHERE', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'JOIN',
        'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'AND', 'OR', 'BY', 'SELECT',
        'FROM', 'AS', 'WITH', 'END', 'THEN', 'ELSE', 'WHEN', 'NULL', 'TRUE', 'FALSE',
    }
    lookahead = r'(?=\s+ON|\s+USING|\s*\)|\s*,|\s+GROUP|\s+ORDER|\s+WHERE|\s+HAVING|\s+LIMIT|\s+OFFSET|\s*;|\s+JOIN|\s+LEFT|\s+INNER|\s+RIGHT|\s+CROSS|\s+FULL|\s+OUTER|\s*$)'
    # FROM/JOIN table [AS] alias — table alias (explicit "table AS alias" or "table alias")
    from_join_with_as = r'(?i)(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN|OUTER\s+JOIN)\s+(\w+)\s+AS\s+(\w+)' + lookahead
    from_join_no_as = r'(?i)(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN|OUTER\s+JOIN)\s+(\w+)(?:\s+(\w+))' + lookahead
    # FROM/JOIN table only (no alias) — e.g. "FROM lists_users WHERE"
    from_join_table_only = r'(?i)(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN|OUTER\s+JOIN)\s+(\w+)' + lookahead
    # AS alias (for column alias we take only those not already table aliases)
    as_alias = r'(?i)\bAS\s+(\w+)(?:\s*[,\)]|\s+[A-Z_]|\s*$)'

    tables = set()
    table_alias_order = []   # table aliases: T1, T2, s, t, o
    alias_to_table = {}      # table alias -> actual table name (for schema-aware replacement)
    col_alias_order = []     # column aliases: avg_snqi, tol_category

    # CTE names
    for m in re.finditer(cte_ctx, sql):
        tables.add(m.group(1))

    # Table aliases: FROM/JOIN table AS alias
    for m in re.finditer(from_join_with_as, sql):
        t, a = m.group(1), m.group(2)
        tables.add(t)
        if a.upper() not in not_table_alias_kw:
            alias_to_table[a] = t
            if a not in table_alias_order:
                table_alias_order.append(a)

    # Table aliases: FROM/JOIN table alias (no AS — so second word must not be "AS")
    for m in re.finditer(from_join_no_as, sql):
        t, a = m.group(1), m.group(2)
        tables.add(t)
        if a and a.upper() != 'AS' and a.upper() not in not_table_alias_kw:
            alias_to_table[a] = t
            if a not in table_alias_order:
                table_alias_order.append(a)

    # Table only (no alias): FROM/JOIN table followed by WHERE, GROUP, etc.
    for m in re.finditer(from_join_table_only, sql):
        tables.add(m.group(1))

    table_aliases_set = set(table_alias_order)

    # Column aliases: AS alias that is not a table alias (e.g. in SELECT expr AS col_alias)
    col_alias_kw = (
        'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'BY', 'HAVING', 'ON', 'AND', 'OR',
        'END', 'THEN', 'ELSE', 'WHEN', 'NULL', 'TRUE', 'FALSE', 'WITH', 'AS', 'DISTINCT',
        'FILTER', 'WITHIN', 'OVER', 'PARTITION', 'BETWEEN', 'LIKE', 'IN', 'IS', 'NOT',
        'EXISTS', 'CASE', 'ASC', 'DESC', 'LIMIT', 'OFFSET',
    )
    for m in re.finditer(as_alias, sql):
        a = m.group(1)
        if a.upper() in col_alias_kw:
            continue
        if a in table_aliases_set:
            continue  # already table alias
        if a not in col_alias_order:
            col_alias_order.append(a)

    # Qualified column: qualifier.col — qualifier can be table alias or table name
    columns = set()
    for m in re.finditer(r'\b(\w+)\.(\w+)\b', sql):
        qual, col = m.group(1), m.group(2)
        if qual.upper() not in ('ON', 'BY', 'AND', 'OR', 'SELECT', 'FROM'):
            columns.add(col)
        if qual not in table_alias_order and qual not in tables and qual.upper() not in (
            'NUM', 'STR', 'DATE', 'GROUP', 'ORDER', 'WITHIN', 'OVER', 'PARTITION',
            'BETWEEN', 'LIKE', 'IN', 'IS', 'NOT', 'EXISTS', 'CASE', 'THEN', 'ELSE', 'WHEN', 'END', 'TRUE', 'FALSE', 'NULL',
        ):
            if qual not in table_alias_order:
                table_alias_order.append(qual)
                table_aliases_set.add(qual)

    table_alias_map = {a: f"table_alias_placeholder{i}" for i, a in enumerate(table_alias_order)}
    col_alias_map = {a: f"col_alias_placeholder{i}" for i, a in enumerate(col_alias_order)}
    col_aliases_set = set(col_alias_order)

    # Placeholder names for replacement checks (so we don't treat them as columns)
    placeholder_keywords = {
        'NUM', 'STR', 'DATE', 'TABLE_NAME', 'COL_NAME',
        'TABLE_ALIAS_PLACEHOLDER0', 'TABLE_ALIAS_PLACEHOLDER1', 'TABLE_ALIAS_PLACEHOLDER2',
        'COL_ALIAS_PLACEHOLDER0', 'COL_ALIAS_PLACEHOLDER1', 'COL_ALIAS_PLACEHOLDER2',
        'num', 'string', 'date', 'boolean',
        'table_alias0', 'table_alias1', 'table_alias2', 'column_alias0', 'column_alias1', 'column_alias2',
    }
    keywords = {
        'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'HAVING', 'ON', 'AND', 'OR',
        'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'FULL', 'AS', 'DISTINCT',
        'NULL', 'TRUE', 'FALSE', 'BETWEEN', 'LIKE', 'IN', 'IS', 'NOT', 'EXISTS',
        'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'ASC', 'DESC', 'LIMIT', 'OFFSET',
        'WITH', 'FILTER', 'WITHIN', 'OVER', 'PARTITION', 'RANGE', 'ROWS',
        'USING', 'TABLE', 'INTO', 'UPDATE', 'SET', 'VALUES', 'INSERT', 'DELETE',
        'CREATE', 'ALTER', 'DROP', 'INDEX', 'PRIMARY', 'KEY', 'REFERENCES',
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'PERCENTILE_CONT',
        'ABS', 'ROUND', 'STRING_AGG', 'JSON_BUILD_OBJECT', 'JSON_OBJECT_AGG',
        'COALESCE', 'NULLIF', 'CAST', 'EXTRACT', 'UNNEST', 'ARRAY',
        'SUBSTR', 'SUBSTRING', 'STRFTIME', 'LENGTH', 'CONCAT',
        'REPLACE', 'TRIM', 'UPPER', 'LOWER', 'INSTR', 'DATE', 'YEAR', 'MONTH',
        'LATERAL', 'CROSS', 'UNION', 'EXCEPT', 'INTERSECT', 'ALL', 'ANY',
        'SIGNAL', 'NOISE', 'SNQI', 'SSM', 'TOLS', 'MCS', 'RPI', 'BFR', 'LIF', 'CCS', 'CIP',
    } | placeholder_keywords
    for m in re.finditer(r'\b(\w+)\b', sql):
        w = m.group(1)
        if w.upper() in keywords:
            continue
        if w in tables or w in table_alias_map or w in col_alias_map:
            continue
        if re.match(r'^table_alias_placeholder\d+$', w, re.I) or re.match(r'^col_alias_placeholder\d+$', w, re.I):
            continue
        if w in ('table_name', 'col_name'):
            continue
        if w in ('NUM', 'STR', 'DATE', 'num', 'string', 'date', 'boolean'):
            continue
        if raw_sql and _is_numeric_token(w):
            continue
        columns.add(w)

    return tables, table_alias_map, col_alias_map, columns, alias_to_table


def _apply_identifier_replacements(
    text: str,
    tables,
    table_alias_map,
    col_alias_map,
    columns,
    alias_to_table=None,
    schema=None,
) -> str:
    """Apply table alias, column alias, table, and column replacements to already literal-replaced text.
    If schema is provided (dict (table_name, col_name) -> type), use schema types for columns when possible.
    """
    out = text
    # Schema: replace columns with type (num/string/date) when we have (table, col) -> type
    cols_replaced_by_schema = set()
    if schema and alias_to_table is not None:
        # 1) Qualified columns: alias.col -> alias.type, table.col -> table_name.type
        for alias, table in alias_to_table.items():
            for (t, col), typ in schema.items():
                if t != table:
                    continue
                out = re.sub(r"\b" + re.escape(alias) + r"\." + re.escape(col) + r"\b", alias + "." + typ, out)
        for table in tables:
            for (t, col), typ in schema.items():
                if t != table:
                    continue
                out = re.sub(r"\b" + re.escape(table) + r"\." + re.escape(col) + r"\b", "table_name." + typ, out)
        # 2) Unqualified columns: col -> type when column has unique type across schema
        col_to_types = {}
        for (t, c), typ in schema.items():
            col_to_types.setdefault(c, set()).add(typ)
        for col in sorted(columns, key=len, reverse=True):
            if col in col_to_types and len(col_to_types[col]) == 1:
                typ = next(iter(col_to_types[col]))
                out = re.sub(r"\b" + re.escape(col) + r"\b", typ, out)
                cols_replaced_by_schema.add(col)
    # Standard replacements
    for alias in sorted(table_alias_map.keys(), key=len, reverse=True):
        out = re.sub(r"\b" + re.escape(alias) + r"\b", table_alias_map[alias], out)
    for alias in sorted(col_alias_map.keys(), key=len, reverse=True):
        out = re.sub(r"\b" + re.escape(alias) + r"\b", col_alias_map[alias], out)
    for t in sorted(tables, key=len, reverse=True):
        if t in table_alias_map:
            continue
        out = re.sub(r"\b" + re.escape(t) + r"\b", "table_name", out)
    for col in sorted(columns, key=len, reverse=True):
        if col.upper() in ("NUM", "STR", "DATE", "TABLE_NAME", "COL_NAME"):
            continue
        if col in ("num", "string", "date", "boolean"):
            continue
        if re.match(r"^table_alias_placeholder\d+$", col, re.I) or re.match(r"^col_alias_placeholder\d+$", col, re.I):
            continue
        if re.match(r"^table_alias\d+$", col, re.I) or re.match(r"^column_alias\d+$", col, re.I):
            continue
        if col in col_alias_map:
            continue
        if schema and col in cols_replaced_by_schema:
            continue
        out = re.sub(r"\." + re.escape(col) + r"\b", ".col_name", out)
        out = re.sub(r"\b" + re.escape(col) + r"\b", "col_name", out)
    return out


def canonicalize_sql(sql: str, schema=None) -> str:
    """
    Canonicalize SQL: replace literals and identifiers with placeholders.
    Preserves exact structure and comments; only replaces identifiers and literals in code.
    If schema is provided (dict (table_name, column_name) -> "num"|"string"|"date"), use schema types for columns.
    """
    if not sql or not sql.strip():
        return sql

    segments = _split_comments(sql)
    non_comment_text = "".join(t for is_c, t in segments if not is_c)
    if not non_comment_text.strip():
        return sql

    out_flat = _replace_literals(non_comment_text)
    tables, table_alias_map, col_alias_map, columns, alias_to_table = _collect_identifiers_regex(out_flat)

    result = []
    for is_comment, text in segments:
        if is_comment:
            result.append(text)
        else:
            seg_out = _replace_literals(text)
            result.append(
                _apply_identifier_replacements(
                    seg_out,
                    tables,
                    table_alias_map,
                    col_alias_map,
                    columns,
                    alias_to_table=alias_to_table,
                    schema=schema,
                )
            )
    return "".join(result)


def canonicalize_sql_bird(sql: str, db_id: str = "", schema_by_db: dict = None) -> str:
    """
    BIRD canonicalization: table_name, col_name; table_alias0, table_alias1, ...; column_alias0, column_alias1, ...;
    literals: num, string, date; for boolean columns in conditions use "boolean".
    Uses variable_list_bird schema: (table, column) -> type (num, string, date, boolean).
    """
    if not sql or not sql.strip():
        return sql

    segments = _split_comments(sql)
    non_comment_text = "".join(t for is_c, t in segments if not is_c)
    if not non_comment_text.strip():
        return sql

    schema = (schema_by_db or {}).get(db_id) or {}
    tables, table_alias_map, col_alias_map, columns, alias_to_table = _collect_identifiers_regex(
        non_comment_text, raw_sql=True
    )
    boolean_col_refs = _build_boolean_col_refs(schema, tables, alias_to_table)

    # BIRD placeholder names: table_alias0, column_alias0 (not table_alias_placeholder0)
    table_alias_map_bird = {a: f"table_alias{i}" for i, a in enumerate(table_alias_map)}
    col_alias_map_bird = {a: f"column_alias{i}" for i, a in enumerate(col_alias_map)}

    result = []
    for is_comment, text in segments:
        if is_comment:
            result.append(text)
        else:
            seg_out = _replace_literals_bird(text, boolean_col_refs)
            result.append(
                _apply_identifier_replacements(
                    seg_out,
                    tables,
                    table_alias_map_bird,
                    col_alias_map_bird,
                    columns,
                    alias_to_table=alias_to_table,
                    schema=None,
                )
            )
    return "".join(result)


def load_schema_csv(schema_path: str):
    """Load schema from CSV. If CSV has 'database' column, returns dict db_id -> ((table, col) -> type).
    Otherwise returns dict with single key 'movie_platform' for backward compatibility."""
    with open(schema_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)
    if "database" in fieldnames:
        by_db = {}
        for row in rows:
            db = row.get("database", "").strip()
            t, c, typ = row.get("table_name"), row.get("column_name"), row.get("column_type")
            if db and t and c and typ:
                by_db.setdefault(db, {})[(t.strip(), c.strip())] = typ.strip()
        return by_db
    schema = {}
    for row in rows:
        t, c, typ = row.get("table_name"), row.get("column_name"), row.get("column_type")
        if t and c and typ:
            schema[(t.strip(), c.strip())] = typ.strip()
    return {"movie_platform": schema}


def process_bird23_jsonl(input_path: str, output_path: str, schema_by_db: dict = None) -> None:
    """Read JSONL (e.g. bird23-train-filtered), add canonical_sql from SQL field, write JSONL.
    If schema_by_db is provided, e.g. {"movie_platform": schema_dict}, use schema for that db_id.
    """
    count = 0
    with open(input_path, "r", encoding="utf-8") as fin, open(output_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            gold = obj.get("SQL", obj.get("sql", ""))
            db_id = obj.get("db_id", "")
            schema = (schema_by_db or {}).get(db_id)
            obj["canonical_sql"] = canonicalize_sql(gold, schema=schema)
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
            count += 1
    print(f"Wrote {count} rows to {output_path}")


def process_bird23_jsonl_bird(
    input_path: str, output_path: str, variable_list_path: str
) -> None:
    """
    BIRD canonicalization: read variable_list_bird.csv (database, table_name, column_name, column_type),
    then for each row use canonicalize_sql_bird(SQL, db_id, schema_by_db).
    Writes JSONL with canonical_sql.
    """
    schema_by_db = load_schema_csv(variable_list_path)
    count = 0
    with open(input_path, "r", encoding="utf-8") as fin, open(output_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            gold = obj.get("SQL", obj.get("sql", ""))
            db_id = obj.get("db_id", "")
            obj["canonical_sql"] = canonicalize_sql_bird(gold, db_id=db_id, schema_by_db=schema_by_db)
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
            count += 1
    print(f"Wrote {count} rows to {output_path}")


def process_bird23_csv_bird(
    input_path: str, output_path: str, variable_list_path: str
) -> None:
    """
    BIRD canonicalization from CSV: read variable_list_bird.csv, then read input CSV (db_id, question, evidence, SQL),
    add canonical_sql, write output CSV.
    """
    schema_by_db = load_schema_csv(variable_list_path)
    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or []) + ["canonical_sql"]
    for row in rows:
        gold = row.get("SQL", row.get("sql", ""))
        db_id = row.get("db_id", "")
        row["canonical_sql"] = canonicalize_sql_bird(gold, db_id=db_id, schema_by_db=schema_by_db)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {output_path}")


def main():
    input_path = "livesqlbench-base-lite.csv"
    output_path = "livesqlbench-base-lite-canonical.csv"
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    if len(sys.argv) > 2:
        output_path = sys.argv[2]

    # BIRD (bird23-train-filtered): BIRD canonicalization with variable_list_bird.csv; use CSV if available
    if "bird23" in input_path or (not input_path.endswith(".csv") and "filtered" in input_path):
        base = input_path.rstrip("/").replace(".csv", "")
        out = output_path if len(sys.argv) > 2 else (base + "-canonical.csv" if input_path.endswith(".csv") else base + "-canonical")
        csv_in = input_path if input_path.endswith(".csv") else (base + ".csv")
        csv_out = out if out.endswith(".csv") else (out + ".csv")
        use_csv = input_path.endswith(".csv") or os.path.exists(csv_in)
        for var_path in ["BIRD/variable_list_bird.csv", "variable_list_bird.csv"]:
            if os.path.exists(var_path):
                if use_csv:
                    process_bird23_csv_bird(csv_in, csv_out, variable_list_path=var_path)
                else:
                    process_bird23_jsonl_bird(input_path, out, variable_list_path=var_path)
                return
        schema_path = "movie_platform_schema.csv"
        schema_by_db = load_schema_csv(schema_path) if os.path.exists(schema_path) else {}
        process_bird23_jsonl(input_path, out, schema_by_db=schema_by_db)
        return

    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames) + ["canonical_sql"]

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in rows:
            gold = row.get("gold_sql", "")
            row["canonical_sql"] = canonicalize_sql(gold)
            writer.writerow(row)

    print(f"Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
