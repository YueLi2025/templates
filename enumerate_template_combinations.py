#!/usr/bin/env python3
"""
Exhaust all possible (table, column) combinations for each canonical template
using variable_list_bird.csv.
(1) Number of tables in combinations = number of tables in the template.
(2) Number of columns in combinations = number of columns in the template.
(3) Column types respected: AVG/SUM -> num, WHERE = string -> string, etc.
"""

import csv
import re
import os
import sys
from collections import defaultdict
from itertools import product

VARIABLE_LIST = os.path.join(os.path.dirname(__file__), "variable_list_bird.csv")
TEMPLATES_CSV = os.path.join(os.path.dirname(__file__), "bird23_canonical_templates.csv")
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "bird23_template_combinations.csv")


def load_schema(path: str):
    """db_id -> {(table_name, column_name): column_type}"""
    by_db = defaultdict(dict)
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            db = (row.get("database") or "").strip()
            t = (row.get("table_name") or "").strip()
            c = (row.get("column_name") or "").strip()
            typ = (row.get("column_type") or "").strip().lower()
            if db and t and c and typ:
                by_db[db][(t, c)] = typ
    return dict(by_db)


def parse_template(sql: str):
    """
    Parse template to get:
    - num_tables: number of table slots (table_alias0, table_alias1, ... or 1 if no aliases)
    - column_types: list of allowed type sets, one per table slot (columns per table)
    Returns: (num_tables, column_types).
    """
    sql_lower = " " + sql.lower() + " "
    # Count table aliases: table_alias0, table_alias1, ...
    alias_indices = set(re.findall(r"table_alias(\d+)", sql_lower))
    if alias_indices:
        num_tables = max(int(i) for i in alias_indices) + 1
    else:
        # Single table: FROM table_name (no alias)
        num_tables = 1

    # Per-alias type constraint: alias i's col_name has what allowed types?
    # Search for table_aliasN.col_name in context (AVG/SUM, = string, = num, = boolean, LIKE string)
    def type_for_alias(alias_idx: int):
        alias_ref = f"table_alias{alias_idx}.col_name"
        allowed = None
        # AVG( table_aliasN.col_name ) / SUM( ... )
        if re.search(r"\b(avg|sum)\s*\(\s*" + re.escape(alias_ref), sql_lower):
            allowed = _merge_type(allowed, {"num"})
        if re.search(r"\b(min|max)\s*\(\s*" + re.escape(alias_ref), sql_lower):
            allowed = _merge_type(allowed, {"num", "date"}) if allowed is None else allowed
        # WHERE ... table_aliasN.col_name = string (or = num, = boolean)
        if re.search(re.escape(alias_ref) + r"\s*=\s*string\b", sql_lower):
            allowed = _merge_type(allowed, {"string"})
        if re.search(re.escape(alias_ref) + r"\s*=\s*num\b", sql_lower):
            allowed = _merge_type(allowed, {"num"})
        if re.search(re.escape(alias_ref) + r"\s*=\s*boolean\b", sql_lower):
            allowed = _merge_type(allowed, {"boolean"})
        if re.search(re.escape(alias_ref) + r"\s*like\s*string", sql_lower):
            allowed = _merge_type(allowed, {"string"})
        # Unqualified col_name (single-table template): apply global constraints
        if num_tables == 1 and "col_name" in sql_lower:
            if re.search(r"\b(avg|sum)\s*\(", sql_lower):
                allowed = _merge_type(allowed, {"num"})
            if "= string" in sql_lower or "=string" in sql_lower:
                allowed = _merge_type(allowed, {"string"})
            if "= num" in sql_lower or "=num" in sql_lower:
                allowed = _merge_type(allowed, {"num"})
            if "= boolean" in sql_lower or "=boolean" in sql_lower:
                allowed = _merge_type(allowed, {"boolean"})
            if "like string" in sql_lower:
                allowed = _merge_type(allowed, {"string"})
        if allowed is None:
            allowed = {"num", "string", "date", "boolean"}
        return allowed

    if num_tables == 1:
        # Single table: one column type set (may have one col or multiple cols; we use one slot per table)
        allowed = None
        if re.search(r"\b(avg|sum)\s*\(", sql_lower):
            allowed = _merge_type(allowed, {"num"})
        if "= string" in sql_lower or "=string" in sql_lower:
            allowed = _merge_type(allowed, {"string"})
        if "= num" in sql_lower or "=num" in sql_lower:
            allowed = _merge_type(allowed, {"num"})
        if "= boolean" in sql_lower or "=boolean" in sql_lower:
            allowed = _merge_type(allowed, {"boolean"})
        if "like string" in sql_lower:
            allowed = _merge_type(allowed, {"string"})
        if "substr(" in sql_lower:
            allowed = _merge_type(allowed, {"string", "date"}) if allowed is None else allowed
        if allowed is None:
            allowed = {"num", "string", "date", "boolean"}
        column_types = [allowed]
    else:
        column_types = [type_for_alias(i) for i in range(num_tables)]

    return num_tables, column_types


def _merge_type(current, new_set):
    if current is None:
        return new_set
    return current & new_set if isinstance(current, set) else current


def enumerate_combinations(schema_by_db, templates_path, output_path, limit=None):
    """Enumerate (table_1, ..., table_N, col_1, ..., col_N) so table count and column count match template.
    Streams rows to CSV to avoid holding all in memory.
    If limit is set (e.g. 2), only process the first limit templates.
    """
    templates = []
    with open(templates_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            templates.append((int(row["template_id"]), (row["canonical_sql"] or "").strip()))
            if limit is not None and len(templates) >= limit:
                break

    max_tables = 1
    for _, sql in templates:
        if sql:
            n, _ = parse_template(sql)
            if n > max_tables:
                max_tables = n
    table_fnames = ["table_name"] + [f"table_name_{i+1}" for i in range(1, max_tables)]
    column_fnames = ["column_name"] + [f"column_name_{i+1}" for i in range(1, max_tables)]
    fieldnames = ["template_id", "db_id"] + table_fnames + column_fnames

    count = 0
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for template_id, sql in templates:
            if not sql:
                continue
            num_tables, column_types = parse_template(sql)
            for db_id, schema in schema_by_db.items():
                tables_list = sorted(set(t for t, c in schema))
                if not tables_list:
                    continue
                for table_combo in product(tables_list, repeat=num_tables):
                    slot_cols = []
                    for i, table in enumerate(table_combo):
                        allowed = column_types[i] if i < len(column_types) else {"num", "string", "date", "boolean"}
                        cols = [c for (t, c), typ in schema.items() if t == table and typ in allowed]
                        slot_cols.append(cols)
                    if any(not s for s in slot_cols):
                        continue
                    for col_combo in product(*slot_cols):
                        row = {"template_id": template_id, "db_id": db_id}
                        for i in range(max_tables):
                            k = "table_name" if i == 0 else f"table_name_{i+1}"
                            row[k] = table_combo[i] if i < num_tables else ""
                        for i in range(max_tables):
                            k = "column_name" if i == 0 else f"column_name_{i+1}"
                            row[k] = col_combo[i] if i < num_tables else ""
                        writer.writerow(row)
                        count += 1
    return count


def main():
    schema_by_db = load_schema(VARIABLE_LIST)
    print(f"Loaded {len(schema_by_db)} databases from {VARIABLE_LIST}")
    limit = None
    out_path = OUTPUT_CSV
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            out_path = OUTPUT_CSV.replace(".csv", f"_first{limit}.csv") if limit else OUTPUT_CSV
            print(f"Limit: first {limit} templates -> {out_path}")
        except ValueError:
            pass
    n = enumerate_combinations(schema_by_db, TEMPLATES_CSV, out_path, limit=limit)
    print(f"Wrote {n} combinations to {out_path}")


if __name__ == "__main__":
    main()
