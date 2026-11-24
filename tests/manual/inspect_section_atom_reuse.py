"""Count repeated timed step/profile inputs across sections in captured batches."""
import runpy
from pathlib import Path
from time import perf_counter

import polars as pl

import mobility.trips.group_day_trips.plans.factored_plan_scoring as scoring


original = scoring.score_factored_sections
calls = []


def inspect_atoms(*args, **kwargs):
    cache = kwargs.get("cache")
    previous = cache.scores if cache is not None else None
    result = original(*args, **kwargs)
    calls.append(1)
    if len(calls) == 3 and cache is not None and cache.sections is not None:
        started = perf_counter()
        profiles = ["country", "csp", "utility_profile_id"]
        atoms = (result.section_scores.select(profiles + ["_section_id"])
                 .join(cache.sections, on="_section_id")
                 .explode("_atoms").unnest("_atoms"))
        unique = atoms.select(profiles + ["activity", "from", "to", "mode", "time",
                                          "duration_per_pers", "_timing_feasible"]).unique()
        print(f"SECTION_ATOMS rows={atoms.height} unique={unique.height} "
              f"seconds={perf_counter() - started:.3f}", flush=True)
        keys = profiles + ["_section_id"]
        atom_keys = profiles + ["activity", "from", "to", "mode", "time",
                                "duration_per_pers", "_timing_feasible"]
        new_sections = result.section_scores.select(keys).join(previous.select(keys), on=keys, how="anti")
        new_atoms = (new_sections.join(cache.sections, on="_section_id")
                     .explode("_atoms").unnest("_atoms").select(atom_keys).unique())
        old_atoms = (previous.select(keys).join(cache.sections, on="_section_id")
                     .explode("_atoms").unnest("_atoms").select(atom_keys).unique())
        unseen = new_atoms.join(old_atoms, on=atom_keys, how="anti")
        print(f"SECTION_NEW_ATOMS new_sections={new_sections.height} new_atoms={new_atoms.height} "
              f"previously_seen={new_atoms.height - unseen.height} unseen={unseen.height}", flush=True)
    return result


scoring.score_factored_sections = inspect_atoms
runpy.run_path(str(Path(__file__).with_name("check_section_scoring_batches.py")), run_name="__main__")
