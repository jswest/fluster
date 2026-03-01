"""Fetch a ~1,000-row toy dataset from 20 Newsgroups for testing fluster.

Usage:
    uv run python demo/fetch_newsgroups.py

Writes twenty_newsgroups_1k.csv into the demo/ directory.
"""

import csv
import random
from collections import Counter
from pathlib import Path

from sklearn.datasets import fetch_20newsgroups

random.seed(42)

data = fetch_20newsgroups(subset="all", remove=("headers", "footers", "quotes"))

# Group by category, sample 50 per category → ~1,000 rows
by_cat: dict[str, list[str]] = {}
for text, label in zip(data.data, data.target):
    cat = data.target_names[label]
    by_cat.setdefault(cat, []).append(text)

rows = []
for cat, texts in sorted(by_cat.items()):
    sampled = random.sample(texts, min(50, len(texts)))
    for i, text in enumerate(sampled):
        clean = " ".join(text.split()).strip()
        if clean:
            rows.append({"name": f"{cat}_{i:03d}", "category": cat, "text": clean})

random.shuffle(rows)

out = Path(__file__).parent / "twenty_newsgroups_1k.csv"
with open(out, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["name", "category", "text"])
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to {out}")
cats = Counter(r["category"] for r in rows)
for cat, count in sorted(cats.items()):
    print(f"  {cat}: {count}")
