#!/usr/bin/env python3
"""
Build a CSV of all database schemas from train_column_meaning.json.
Keys are "db_id|table_name|column_name"; datatype is inferred from the description.
Output CSV: database, table_name, column_name, column_type.
Types: num, string, date, boolean, binary (only these five; inferred from real meaning of definitions).
"""

import json
import csv
import re

INPUT_PATH = "train_column_meaning.json"
OUTPUT_PATH = "all_databases_schema.csv"


def infer_column_type(description: str) -> str:
    """Infer column type from description. Returns: num, string, date, boolean, or binary."""
    d = description.lower()
    # string (early checks)
    if "text critique" in d or "critiques" in d or ("critic" in d and "text" in d):
        return "string"
    # binary: blob / binary data (images, documents) - check before boolean so "binary data" isn't treated as two-option
    if "blob" in d or "binary large object" in d:
        return "binary"
    if "binary data" in d and ("image" in d or "photo" in d or "picture" in d or "document" in d or "logo" in d or "stores" in d):
        return "binary"
    # boolean: two-option / binary-choice semantics (not "binary data" storage)
    if "'true' or 'false'" in d or "'true' or \"false\"" in d or " 'true' " in d and " 'false' " in d:
        return "boolean"
    if "values are 'true'" in d or "response 'true'" in d or "('true')" in d and "('false')" in d:
        return "boolean"
    if " (1) or not (0)" in d or "(1) or not (0)" in d or " or not (0)" in d or "(1) or not " in d:
        return "boolean"
    if re.search(r"1 for .+ 0 for|0 for .+ 1 for", d) or re.search(r"where 0 means .+ 1 means|where 1 means .+ 0 means", d):
        return "boolean"
    if re.search(r"0 for .+ 1 for|1 for .+ 0 for", d):
        return "boolean"
    if "indicates if " in d and re.search(r"\(1\)|\(0\)| 1 | 0 ", d) and (" or not " in d or " or 0 " in d or " or 1 " in d):
        return "boolean"
    if "indicates whether" in d and re.search(r"possible values being '[^']+' or '[^']+'\"?", d) and d.count(" or ") == 1 and d.count("'") == 4:
        return "boolean"
    if "possible values being 'male' or 'female'" in d or "possible values being 'female' or 'male'" in d:
        return "boolean"
    if "'m' for male" in d and "'f' for female" in d:
        return "boolean"
    if "'m' for " in d and "'f' for " in d and ("male" in d or "female" in d or "gender" in d or "sex" in d):
        return "boolean"
    if "two possible values" in d or " only two possible values" in d or " two values " in d:
        return "boolean"
    if "winner' or " in d and "nominee" in d and "result" in d:
        return "boolean"
    if "original' or 'spoken'" in d or "spoken' or 'original'" in d:
        return "boolean"
    if "subscriber' or 'customer'" in d or "customer' or 'subscriber'" in d:
        return "boolean"
    if "bat' or 'field'" in d or "field' or 'bat'" in d:
        return "boolean"
    if "summer' or 'winter'" in d or "winter' or 'summer'" in d:
        return "boolean"
    if "paid' or 'free'" in d or "free' or 'paid'" in d:
        return "boolean"
    if "left-hand bat' or 'right-hand bat'" in d:
        return "boolean"
    if "official ('t') or not ('f')" in d or "('t') or not ('f')" in d:
        return "boolean"
    if "married ('m') or single ('s')" in d or "single ('s')" in d and "married" in d:
        return "boolean"
    if "flag" in d and ("0 for " in d or "1 for " in d or "(0)" in d or "(1)" in d) and ("or not" in d or " or 0 " in d or " or 1 " in d):
        return "boolean"
    if "eligible for a trial" in d or "eligible for trial" in d or "1 for eligible" in d or "0 for not eligible" in d:
        return "boolean"
    if ("subscriber" in d or "trialist" in d or "payment" in d or "has_payment" in d) and ("(1)" in d or " 1 " in d) and ("(0)" in d or " 0 " in d or "or not" in d):
        return "boolean"
    # date / timestamp (check before num so "ranging from" in date descriptions doesn't become num)
    if any(
        x in d
        for x in (
            "date when",
            "yyyy-mm-dd",
            "timestamp",
            "datetime",
            "date and time",
            "date a list",
            "records the date",
            "formatted as 'yyyy-mm-dd",
            "formatted as yyyy-mm-dd",
            "formatted as a date",
            "formatted as date",
            "utc datetime",
            "when each list was",
            "when a user submitted",
            "specific dates",
            "specific date",
            "stores dates",
            "dates as text",
            "text-formatted dates",
            "of type date",
            "type: date",
            "(type: date)",
            "a datetime type",
            "of type datetime",
        )
    ):
        return "date"
    if re.search(r"\b(date|timestamp|datetime)\s+(column|value|format|when)", d):
        return "date"
    if re.search(r"formatted as (text )?strings?.*\d{4}-\d{2}-\d{2}", d):
        return "date"
    if re.search(r"dates? indicating when", d):
        return "date"
    # numeric
    if re.search(r"\binteger\b", d) or re.search(r"\b(an? )?integer\b", d):
        return "num"
    if re.search(r"\breal\b", d) or "real number" in d:
        return "num"
    if "count of" in d or "number of" in d or "scale of 1" in d or "scale from 1" in d:
        return "num"
    if "the number of" in d:
        return "num"
    if "represented as an integer" in d or "integer column" in d or "integer type" in d:
        return "num"
    if "integer id" in d or "integer identifier" in d or "integer value" in d or "integer indicating" in d:
        return "num"
    if "tracks the count" in d:
        return "num"
    if "unique identifier" in d and "id" in d:
        if "text-type" in d or "text type" in d or "text identifiers" in d:
            return "string"
        return "num"
    if "ranging from" in d or "scale of 1" in d:
        return "num"
    # string
    if "text-type" in d or "text type" in d or "text identifiers" in d:
        return "string"
    if re.search(r"\btext\b", d) or " as text" in d or "text column" in d:
        return "string"
    if "url" in d or "urls" in d:
        return "string"
    if "name" in d or "title" in d or "description" in d or "critique" in d:
        return "string"
    if "address" in d or "city" in d or "code" in d or "category" in d:
        return "string"
    if "language" in d:
        return "string"
    return "string"


def main():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for key, desc in data.items():
        parts = key.split("|")
        if len(parts) != 3:
            continue
        db_id, table_name, column_name = (p.strip() for p in parts)
        if not db_id or not table_name or not column_name:
            continue
        column_type = infer_column_type(desc)
        rows.append(
            {
                "database": db_id,
                "table_name": table_name,
                "column_name": column_name,
                "column_type": column_type,
            }
        )

    rows.sort(key=lambda r: (r["database"], r["table_name"], r["column_name"]))

    with open(OUTPUT_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["database", "table_name", "column_name", "column_type"],
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        writer.writerows(rows)

    db_count = len({r["database"] for r in rows})
    print(f"Wrote {len(rows)} rows ({db_count} databases) to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
