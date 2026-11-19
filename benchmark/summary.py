#!/usr/bin/env python3
"""Print an aggregate.json as a fixed-width table."""
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "build/eval-quick")
rows = json.loads((path / "aggregate.json").read_text())
for r in rows:
    print(f"{r['case']:<16}{r['phase']:<9}n={r['points']:<9} "
          f"ops/s={r['median_ops_per_second']:>9.0f} "
          f"sec={r.get('median_seconds', 0):>7.3f} "
          f"p99={r['median_p99_us'] / 1000:>7.1f}ms "
          f"rss={r['max_rss_bytes'] / 2**20:5.1f}MiB")
