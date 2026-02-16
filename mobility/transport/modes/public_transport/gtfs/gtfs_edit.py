import hashlib
import logging
import math
import re
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class NewStop:
    stop_id: str
    stop_name: str
    stop_lat: float
    stop_lon: float


class GTFSFeed:
    def __init__(self, zip_path: str | Path):
        self.zip_path = Path(zip_path)
        self.tables: dict[str, pd.DataFrame] = {}

    def load(self) -> "GTFSFeed":
        with zipfile.ZipFile(self.zip_path, "r") as z:
            for name in z.namelist():
                if name.endswith(".txt"):
                    with z.open(name) as f:
                        self.tables[name] = pd.read_csv(f)
        return self

    def save(self, out_zip_path: str | Path) -> Path:
        out_zip_path = Path(out_zip_path)
        buf = BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for name, df in self.tables.items():
                z.writestr(name, df.to_csv(index=False))
        buf.seek(0)
        out_zip_path.write_bytes(buf.read())
        return out_zip_path


def apply_gtfs_edits(gtfs_files, gtfs_edits, edits_folder: str | Path):
    edits_folder = Path(edits_folder)
    edits_folder.mkdir(parents=True, exist_ok=True)

    def matches_rule(rule, gtfs_path):
        if rule.get("path"):
            return str(Path(rule["path"])) == str(Path(gtfs_path))
        if rule.get("match"):
            return rule["match"] in str(gtfs_path)
        return False

    def core_id(x):
        m = re.search(r"(\d{5,})$", str(x))
        return m.group(1) if m else str(x)

    def has_chain(gtfs_path, from_id, to_id):
        with zipfile.ZipFile(gtfs_path, "r") as z:
            with z.open("stop_times.txt") as f:
                st = pd.read_csv(f, usecols=["trip_id", "stop_sequence", "stop_id"])
        st = st.sort_values(["trip_id", "stop_sequence"])
        st["a"] = st["stop_id"].map(core_id)
        st["b"] = st.groupby("trip_id", sort=False)["a"].shift(-1)
        return ((st["a"] == core_id(from_id)) & (st["b"] == core_id(to_id))).any()

    def expand_ops(rule_ops):
        """Duplique les ops avec bidirectional=True en ajoutant l'op inversee."""
        expanded = []
        for op in rule_ops:
            expanded.append(op)
            if op.get("op") == "insert_stop_between" and op.get("bidirectional"):
                op_rev = dict(op)
                op_rev["from_stop_id"], op_rev["to_stop_id"] = op["to_stop_id"], op["from_stop_id"]
                # On peut garder bidirectional=True ou le mettre a False pour eviter re-expansion.
                op_rev["bidirectional"] = False
                expanded.append(op_rev)
        return expanded

    # --- Precompute chain hits for each (from,to) actually needed ---
    chain_hits = {}
    for rule in gtfs_edits or []:
        for op in expand_ops(rule.get("ops", [])):
            if op.get("op") != "insert_stop_between":
                continue
            key = (op["from_stop_id"], op["to_stop_id"])
            if key in chain_hits:
                continue

            hits = []
            for p in gtfs_files:
                try:
                    if has_chain(p, key[0], key[1]):
                        hits.append(p)
                except Exception:
                    pass
            chain_hits[key] = hits

    # --- Build mapping: gtfs_path -> list of ops to apply ---
    ops_by_gtfs = {}
    for rule in gtfs_edits or []:
        mode = (rule.get("mode") or "explicit").lower().strip()
        if mode not in {"explicit", "all"}:
            raise ValueError("rule.mode must be 'explicit' or 'all'")

        for op in expand_ops(rule.get("ops", [])):
            if op.get("op") != "insert_stop_between":
                continue

            key = (op["from_stop_id"], op["to_stop_id"])
            hits = chain_hits.get(key, [])

            if mode == "explicit":
                targets = [p for p in gtfs_files if matches_rule(rule, p)]
                not_covered = [p for p in hits if p not in targets]
                if not_covered:
                    logging.info(
                        "[GTFS edit] Note: chain %s -> %s also found in %s GTFS not targeted (mode=explicit).",
                        key[0],
                        key[1],
                        len(not_covered),
                    )
                    for p in not_covered:
                        logging.info("[GTFS edit]   - %s", p)
            else:
                targets = hits
                logging.info(
                    "[GTFS edit] mode=all: applying chain %s -> %s edit to %s GTFS.",
                    key[0],
                    key[1],
                    len(targets),
                )

            for p in targets:
                ops_by_gtfs.setdefault(p, []).append(op)

    # --- Apply edits (inchange) ---
    new_files = []
    for gtfs_path in gtfs_files:
        ops = ops_by_gtfs.get(gtfs_path)
        if not ops:
            new_files.append(gtfs_path)
            continue

        h = hashlib.md5((str(gtfs_path) + str(ops)).encode("utf-8")).hexdigest()[:12]
        src_p = Path(gtfs_path)
        out_p = edits_folder / f"{src_p.stem}__edited_{h}{src_p.suffix}"

        if out_p.exists():
            logging.info("[GTFS edit] Using cached edited GTFS: %s", out_p)
            new_files.append(str(out_p))
            continue

        logging.info("[GTFS edit] Editing GTFS: %s", src_p.name)
        feed = GTFSFeed(src_p).load()

        for op in ops:
            ns = op["new_stop"]
            insert_stop_between(
                feed=feed,
                from_stop_id=op["from_stop_id"],
                to_stop_id=op["to_stop_id"],
                new_stop=NewStop(
                    stop_id=ns["stop_id"],
                    stop_name=ns["stop_name"],
                    stop_lat=ns["stop_lat"],
                    stop_lon=ns["stop_lon"],
                ),
                dwell_time_s=int(op.get("dwell_time_s", 45)),
                extra_run_time_s=int(op.get("extra_run_time_s", 30)),
                split_ratio=float(op.get("split_ratio", 0.5)),
                propagate=str(op.get("propagate", "after")),
            )

        feed.save(out_p)
        logging.info("[GTFS edit] Saved edited GTFS: %s", out_p)
        new_files.append(str(out_p))

    return new_files


def insert_stop_between(
    feed: GTFSFeed,
    from_stop_id: str,
    to_stop_id: str,
    new_stop: NewStop,
    dwell_time_s: int = 45,
    extra_run_time_s: int = 30,
    split_ratio: float = 0.5,
    propagate: str = "after",  # "after" | "before" | "symmetric"
) -> None:
    """
    Insert a new stop between two consecutive stops A -> B for all trips.

    Assumptions (heuristic, not operationally exact):
    - We split the original A->B travel time using split_ratio to position the new stop temporally.
    - We add dwell_time_s at the new stop.
    - We add extra_run_time_s to represent braking/acceleration overhead.
    - We propagate the resulting delay either after B (default), before A, or symmetrically.

    Matching:
    - We match stops by their numeric suffix (e.g. "...87118257"), to ignore StopPoint/StopArea prefixes.
    """
    propagate = (propagate or "after").lower().strip()
    if propagate not in {"after", "before", "symmetric"}:
        raise ValueError("propagate must be one of: after, before, symmetric")
    if not (0.0 <= float(split_ratio) <= 1.0):
        raise ValueError("split_ratio must be between 0 and 1")

    logging.info(
        "[GTFS edit] insert_stop_between: start (from=%s, to=%s, new=%s)",
        from_stop_id,
        to_stop_id,
        new_stop.stop_id,
    )

    # --- Inline helpers (kept minimal) ---
    id_re = re.compile(r"(\d{5,})$")

    def core_id(x: str) -> str:
        if not isinstance(x, str):
            return ""
        m = id_re.search(x)
        return m.group(1) if m else x

    def hms_to_s(x: str):
        # Returns int seconds or NA.
        if not isinstance(x, str) or x == "":
            return pd.NA
        try:
            h, m, s = x.split(":")
            return int(h) * 3600 + int(m) * 60 + int(s)
        except Exception:
            return pd.NA

    def s_to_hms(x) -> str:
        # GTFS allows hours > 24, keep as-is (no modulo).
        if x is None or (isinstance(x, float) and math.isnan(x)) or pd.isna(x):
            return ""
        x = int(round(float(x)))
        h = x // 3600
        m = (x % 3600) // 60
        s = x % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    from_core = core_id(from_stop_id)
    to_core = core_id(to_stop_id)

    # --- stops.txt: minimal insert (do not try to rebuild full StopArea/StopPoint hierarchy) ---
    if "stops.txt" not in feed.tables:
        raise ValueError("stops.txt missing in GTFS feed")
    if "stop_times.txt" not in feed.tables:
        raise ValueError("stop_times.txt missing in GTFS feed")

    stops = feed.tables["stops.txt"]
    if (stops["stop_id"] == new_stop.stop_id).sum() == 0:
        row = {
            "stop_id": new_stop.stop_id,
            "stop_name": new_stop.stop_name,
            "stop_lat": new_stop.stop_lat,
            "stop_lon": new_stop.stop_lon,
        }
        # Ensure required cols exist; keep other cols untouched / NA.
        for col in row.keys():
            if col not in stops.columns:
                stops[col] = pd.NA
        feed.tables["stops.txt"] = pd.concat([stops, pd.DataFrame([row])], ignore_index=True)

    # --- stop_times.txt: edit trip-by-trip ---
    st = feed.tables["stop_times.txt"].copy()
    st["arrival_s"] = pd.to_numeric(st["arrival_time"].map(hms_to_s), errors="coerce")
    st["departure_s"] = pd.to_numeric(st["departure_time"].map(hms_to_s), errors="coerce")
    st.sort_values(["trip_id", "stop_sequence"], inplace=True)

    # We accumulate "insertions" + "shifts" and apply them once at the end.
    insert_rows = []
    seq_shifts = []  # (trip_id, start_seq, +1)
    time_shifts = []  # (trip_id, mode, boundary_seq, seconds)

    modified_trips = set()
    delta_total = int(dwell_time_s) + int(extra_run_time_s)

    for trip_id, g in st.groupby("trip_id", sort=False):
        g = g.sort_values("stop_sequence")

        seq = g["stop_sequence"].to_list()
        stop_ids = g["stop_id"].to_list()
        stop_core = [core_id(s) for s in stop_ids]

        for i in range(len(stop_core) - 1):
            if stop_core[i] != from_core or stop_core[i + 1] != to_core:
                continue

            dep_a = g.iloc[i]["departure_s"]
            arr_b = g.iloc[i + 1]["arrival_s"]
            if pd.isna(dep_a) or pd.isna(arr_b):
                # If times are missing, we skip this occurrence (keeps edit safe).
                continue

            seq_a = int(seq[i])
            seq_b = int(seq[i + 1])

            dep_a = int(round(float(dep_a)))
            arr_b = int(round(float(arr_b)))

            # Original travel time between A departure and B arrival.
            t_ab = max(0, arr_b - dep_a)

            # Place the new stop along A->B time axis.
            t_an = int(t_ab * float(split_ratio))

            arr_n = dep_a + t_an
            dep_n = arr_n + int(dwell_time_s)

            insert_rows.append(
                {
                    "trip_id": trip_id,
                    "arrival_time": s_to_hms(arr_n),
                    "departure_time": s_to_hms(dep_n),
                    "arrival_s": arr_n,
                    "departure_s": dep_n,
                    "stop_id": new_stop.stop_id,
                    "stop_sequence": seq_a + 1,
                }
            )

            modified_trips.add(trip_id)

            # 1) shift stop_sequence for all following stops
            seq_shifts.append((trip_id, seq_a + 1))

            # 2) shift times according to strategy
            if propagate == "after":
                time_shifts.append((trip_id, "after", seq_b, delta_total))
            elif propagate == "before":
                time_shifts.append((trip_id, "before", seq_a, delta_total))
            else:
                half = int(round(delta_total / 2))
                time_shifts.append((trip_id, "before", seq_a, half))
                time_shifts.append((trip_id, "after", seq_b, delta_total - half))

    # Apply shifts (vectorized masks)
    for trip_id, start_seq in seq_shifts:
        mask = (st["trip_id"] == trip_id) & (st["stop_sequence"] >= start_seq)
        st.loc[mask, "stop_sequence"] = st.loc[mask, "stop_sequence"] + 1

    for trip_id, mode, boundary_seq, seconds in time_shifts:
        if mode == "after":
            mask = (st["trip_id"] == trip_id) & (st["stop_sequence"] >= boundary_seq)
            st.loc[mask, "arrival_s"] = st.loc[mask, "arrival_s"] + seconds
            st.loc[mask, "departure_s"] = st.loc[mask, "departure_s"] + seconds
        else:  # before
            mask = (st["trip_id"] == trip_id) & (st["stop_sequence"] <= boundary_seq)
            st.loc[mask, "arrival_s"] = st.loc[mask, "arrival_s"] - seconds
            st.loc[mask, "departure_s"] = st.loc[mask, "departure_s"] - seconds

    if insert_rows:
        st = pd.concat([st, pd.DataFrame(insert_rows)], ignore_index=True)
        st.sort_values(["trip_id", "stop_sequence"], inplace=True)

    # Back to GTFS time strings.
    st["arrival_s"] = pd.to_numeric(st["arrival_s"], errors="coerce").round().astype("Int64")
    st["departure_s"] = pd.to_numeric(st["departure_s"], errors="coerce").round().astype("Int64")
    st["arrival_time"] = st["arrival_s"].map(lambda x: "" if pd.isna(x) else s_to_hms(int(x)))
    st["departure_time"] = st["departure_s"].map(lambda x: "" if pd.isna(x) else s_to_hms(int(x)))
    st.drop(columns=["arrival_s", "departure_s"], inplace=True)

    feed.tables["stop_times.txt"] = st

    logging.info(
        "[GTFS edit] insert_stop_between: done (%s trips modified, %s stop_times rows inserted)",
        len(modified_trips),
        len(insert_rows),
    )
