"""Compare outputs of the expanded and shared-component flow benchmarks."""
import argparse
import json
from pathlib import Path

import polars as pl


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("expanded", type=Path)
parser.add_argument("shared", type=Path)
args = parser.parse_args()
for name, keys, value in [("od", ["from", "to", "mode"], "volume"),
                          ("occupation", ["to", "activity"], "occupation")]:
    first, second = args.expanded / f"{name}.parquet", args.shared / f"{name}.parquet"
    if not first.exists() and not second.exists():
        continue
    if not first.exists() or not second.exists():
        raise ValueError(f"Both output folders must have {name} data.")
    a, b = pl.read_parquet(first), pl.read_parquet(second)
    both = (a.join(b, on=keys, how="full", coalesce=True, suffix="_shared")
            .with_columns(pl.col(value).fill_null(0), pl.col(value + "_shared").fill_null(0))
            .with_columns((pl.col(value) - pl.col(value + "_shared")).abs().alias("difference")))
    absolute = float(both["difference"].sum())
    total = float(both[value].sum())
    print("COMPONENT_COMPARISON", json.dumps({"output": name, "expanded_cells": a.height,
          "shared_cells": b.height, "union_cells": both.height,
          "maximum_absolute_difference": float(both["difference"].max() or 0),
          "sum_absolute_difference": absolute, "relative_absolute_difference": absolute / total if total else 0,
          "expanded_total": total}), flush=True)
