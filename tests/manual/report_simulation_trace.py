"""Summarize an existing simulation trace without rerunning or hashing data.

Usage: pixi run python tests/manual/report_simulation_trace.py TRACE.jsonl
Memory values are process RSS, not the memory owned by an individual operation.
Self time excludes the union of direct child intervals, so nested time is not
counted twice. Overlapping work in other threads may still affect wall times.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("trace", type=Path)
args = parser.parse_args()
spans = [json.loads(line) for line in args.trace.read_text(encoding="utf-8").splitlines()]
children = defaultdict(list)
for span in spans:
    children[(span["run_id"], span.get("parent_id"))].append(span)


def child_duration(span):
    """Sum child wall intervals once, including when intervals overlap."""
    intervals = sorted(
        (max(span["start_seconds"], child["start_seconds"]),
         min(span["end_seconds"], child["end_seconds"]))
        for child in children[(span["run_id"], span["step_id"])]
    )
    total = 0.0
    end = span["start_seconds"]
    for start, stop in intervals:
        if stop > max(start, end):
            total += stop - max(start, end)
        end = max(end, stop)
    return total


summary = defaultdict(lambda: {"calls": 0, "self": 0.0, "inclusive": 0.0, "peak_rss": 0})
for span in spans:
    item = summary[(span.get("phase", "setup"), span["operation"])]
    item["calls"] += 1
    item["self"] += max(0.0, span["wall_seconds"] - child_duration(span))
    item["inclusive"] += span["wall_seconds"]
    item["peak_rss"] = max(item["peak_rss"], span["rss_peak_bytes"])

print("Operation totals (self seconds, inclusive seconds, calls, peak process RSS GiB):")
for (phase, name), item in sorted(summary.items(), key=lambda entry: -entry[1]["self"])[:30]:
    print(f"{phase or 'setup':6} {name:42} {item['self']:8.1f} {item['inclusive']:8.1f} "
          f"{item['calls']:5} {item['peak_rss'] / 1024**3:7.2f}")

print("\nHighest process RSS observed in an operation (nested operations can overlap):")
for span in sorted(spans, key=lambda item: -item["rss_peak_bytes"])[:12]:
    print(f"{span.get('phase', 'setup') or 'setup':6} step={span.get('step', '-')} "
          f"batch={span.get('search_batch', '-')} scenario={span.get('scenario', '-')} "
          f"rss={span['rss_peak_bytes'] / 1024**3:.2f} GiB {span['operation']}")

print("\nInner-loop wall time and peak process RSS per step:")
for span in sorted((item for item in spans if item["operation"] == "inner loop"),
                   key=lambda item: item.get("step", 0)):
    print(f"step={span.get('step', '-')} seconds={span['wall_seconds']:.1f} "
          f"peak_rss={span['rss_peak_bytes'] / 1024**3:.2f} GiB")
