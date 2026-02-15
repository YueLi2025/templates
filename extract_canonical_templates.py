#!/usr/bin/env python3
"""
Analyze canonical_sql column of bird23-train-filtered-canonical.csv,
extract all distinct templates, and write to a new CSV.
"""

import csv
import os

INPUT_CSV = os.path.join(os.path.dirname(__file__), "bird23-train-filtered-canonical.csv")
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "bird23_canonical_templates.csv")


def main():
    template_to_count = {}
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = (row.get("canonical_sql") or "").strip()
            if t:
                template_to_count[t] = template_to_count.get(t, 0) + 1

    # Sort by count descending, then by template string for stable order
    distinct = sorted(
        [(sql, template_to_count[sql]) for sql in template_to_count],
        key=lambda x: (-x[1], x[0]),
    )

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["template_id", "canonical_sql", "count"])
        for i, (sql, count) in enumerate(distinct, start=1):
            writer.writerow([i, sql, count])

    print(f"Read {sum(c for _, c in distinct)} rows from {INPUT_CSV}")
    print(f"Wrote {len(distinct)} distinct templates to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
