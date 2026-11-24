"""Equivalent populations share scoring without losing their original identities."""

from dataclasses import replace
from types import SimpleNamespace

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts, ScoringLookup, ScoringMemoryBudget
from mobility.trips.group_day_trips.plans.factored_plan_scoring import SectionScoreCache
from mobility.trips.group_day_trips.plans.plan_updater import CandidatePlanEvaluation, PlanUpdater
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS, add_plan_id
from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.trips.group_day_trips.plans.compact_candidate_steps import (
    CompactCandidateSteps, ScoringCandidateSteps, MEMBERSHIP_COLUMNS, PLAN_COLUMNS, STEP_COLUMNS,
)


def test_scoring_lookups_share_only_fixed_matching_inputs():
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.run_keys = list(range(8))
    table = pl.DataFrame({"x": [1]})
    state = SimpleNamespace(demand_groups=table, activity_dur=table, destination_saturation=table)
    states = {i: state for i in coordinator.run_keys}
    states[2] = SimpleNamespace(demand_groups=table, activity_dur=table, destination_saturation=table.clone())
    costs = object()
    state_assets = {
        i: SimpleNamespace(iteration=1, transport_costs=costs, inputs={"resolved_activity_parameters": {"home": 1}})
        for i in coordinator.run_keys
    }
    state_assets[3].inputs = {"resolved_activity_parameters": {"home": 2}}
    population = SimpleNamespace(transport_zones=object(), population_segments=[])
    update = SimpleNamespace(min_activity_time_constant=1.0, use_destination_shadow_prices=False)
    coordinator.runs = {
        i: SimpleNamespace(population=population, parameters=SimpleNamespace(plan_update=update))
        for i in coordinator.run_keys
    }
    coordinator.runs[4] = SimpleNamespace(
        population=population,
        parameters=SimpleNamespace(plan_update=SimpleNamespace(
            min_activity_time_constant=2.0, use_destination_shadow_prices=False,
        )),
    )
    coordinator.runs[6] = SimpleNamespace(
        population=SimpleNamespace(transport_zones=population.transport_zones, population_segments=["different"]),
        parameters=SimpleNamespace(plan_update=update),
    )
    coordinator.runs[7] = SimpleNamespace(
        population=population,
        parameters=SimpleNamespace(plan_update=SimpleNamespace(
            min_activity_time_constant=1.0, use_destination_shadow_prices=True,
        )),
    )
    cost_assets = {i: costs for i in coordinator.run_keys}
    cost_assets[5] = object()
    budget = ScoringMemoryBudget(max_bytes=1000)
    lookups = coordinator._build_scoring_lookups(states, state_assets, cost_assets, 1, budget)
    assert lookups[0] is lookups[1] and lookups[0] is not None
    assert len({id(lookups[i]) for i in (0, 2, 3, 4, 5, 6, 7)}) == 7
    sections = pl.DataFrame({"_section_id": [0], "_atoms": [[1, 2]]})
    coordinator._section_structure_cache = {0: sections}
    fresh_budget = ScoringMemoryBudget(max_bytes=1000)
    lookups = coordinator._build_scoring_lookups(states, state_assets, cost_assets, 1, fresh_budget)
    assert lookups[0] is lookups[1]
    assert lookups[0].section_cache.sections.equals(sections)
    assert lookups[0].section_cache.scores is None
    assert lookups[2].section_cache is None
    assert fresh_budget.used_bytes == sections.estimated_size()
    assert coordinator._section_structure_cache == {}
    lookups = coordinator._build_scoring_lookups(states, state_assets, cost_assets, 1, budget)
    assert lookups[0] is lookups[1]
    assert len({id(lookups[i]) for i in (0, 2, 3, 4, 5, 6, 7)}) == 7
    cost_assets[0] = InMemoryAsset({"test": "matching costs"})
    cost_assets[1] = InMemoryAsset({"test": "matching costs"})
    lookups = coordinator._build_scoring_lookups(states, state_assets, cost_assets, 1, budget)
    assert lookups[0] is lookups[1] and lookups[0] is not None


def test_compact_lookup_takes_priority_over_reusable_section_scores():
    sections = pl.DataFrame({"value": range(100)})
    section_scores = pl.DataFrame({"value": range(100)})
    initial_lookup = pl.DataFrame({"value": range(100)})
    expanded_lookup = pl.DataFrame({"value": range(200)})
    budget = ScoringMemoryBudget(max_bytes=(sections.estimated_size() + section_scores.estimated_size()
                                            + expanded_lookup.estimated_size() - 1))
    cache = SectionScoreCache(budget=budget)
    cache.retain(sections, section_scores)
    lookup = ScoringLookup(budget=budget, section_cache=cache)
    lookup.retain(initial_lookup)
    lookup.retain(expanded_lookup)
    assert lookup.scores.equals(expanded_lookup)
    assert cache.sections is None and cache.scores is None
    assert budget.used_bytes == expanded_lookup.estimated_size()


def test_candidate_evaluations_share_only_matching_inputs():
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.scenarios = list(range(5))
    table = pl.DataFrame({"x": [1]})
    state = SimpleNamespace(
        candidate_plan_steps=None, current_plans=table, current_plan_steps=table,
        demand_groups=table, destination_saturation=table, activity_dur=table,
        home_night_dur=table, stay_home_plan=table,
    )
    states = {key: state for key in coordinator.scenarios}
    costs = object()
    state_assets = {
        key: SimpleNamespace(
            iteration=1, transport_costs=costs, updater=PlanUpdater(),
            inputs={"resolved_activity_parameters": {"home": 1}, "activity_time_rigidity_by_activity": {"home": 1}},
        )
        for key in coordinator.scenarios
    }
    zones = object()
    plan_update = SimpleNamespace(min_activity_time_constant=1.0)
    behavior = SimpleNamespace(scope_at=lambda _: BehaviorChangeScope.MODE_REPLANNING)
    coordinator.runs = {
        key: SimpleNamespace(
            population=SimpleNamespace(transport_zones=zones),
            parameters=SimpleNamespace(plan_update=plan_update, behavior_change=behavior),
        )
        for key in coordinator.scenarios
    }
    sources = {key: SimpleNamespace(mode_sequences=SimpleNamespace(population_segments=[]))
               for key in coordinator.scenarios}
    shared_steps, shared_factored = object(), object()
    parts = {key: (shared_steps, None) for key in coordinator.scenarios}
    factored = {key: shared_factored for key in coordinator.scenarios}
    factored[2] = object()
    pool = SimpleNamespace(
        candidate_step_parts_by_run_key=parts, scoring_candidate_steps_by_run_key=factored,
        candidate_plan_steps_by_run_key={}, time_sequence_steps_by_run_key={key: table for key in coordinator.scenarios},
    )
    contexts = object()
    setups = {key: (contexts, table, table) for key in coordinator.scenarios}
    state_assets[3].transport_costs = object()
    state_assets[4].inputs = {
        "resolved_activity_parameters": {"home": 2}, "activity_time_rigidity_by_activity": {"home": 1},
    }
    args = dict(
        pool=pool, states=states, state_assets=state_assets, sources=sources, cost_assets=None,
        iteration=1, scoring_setups=setups, compact_indices={}, forced_contexts={}, restrict_scoring=False,
    )
    assert coordinator._group_candidate_evaluations(**args) == [[0, 1], [2], [3], [4]]
    args["restrict_scoring"] = True
    args["forced_contexts"] = {0: table, 1: table.clone()}
    assert coordinator._group_candidate_evaluations(**args) == [[0], [1], [2], [3], [4]]
    args["restrict_scoring"] = False
    first_history = CompactCandidateSteps(table, table, ("x",), unit_map=table)
    states[0] = SimpleNamespace(**{**vars(state), "candidate_plan_steps": first_history})
    states[1] = SimpleNamespace(**{**vars(state), "candidate_plan_steps": replace(first_history)})
    assert coordinator._group_candidate_evaluations(**args)[0] == [0, 1]
    states[1].candidate_plan_steps = replace(first_history, membership=table.clone())
    assert coordinator._group_candidate_evaluations(**args)[0] == [0]
    states[1].candidate_plan_steps = first_history
    custom_updater = SimpleNamespace()
    state_assets[0].updater = custom_updater
    state_assets[1].updater = custom_updater
    assert coordinator._group_candidate_evaluations(**args)[:2] == [[0], [1]]


def test_contexts_preserve_population_profiles_and_stable_ids():
    demand = pl.DataFrame({
        "demand_group_id": [1, 2, 3, 4, 5], "demand_subgroup_id": [0] * 5,
        "home_zone_id": [10] * 5, "country": ["FR"] * 5,
        "csp": [1, 1, 2, 1, 1], "n_persons": [20., 30., 40., 50., 60.],
        "allowed_activities": [["work", "home"], ["home", "work"],
                               ["home", "work"], ["home", "work"], ["home"]],
    })
    profiles = demand.select("demand_group_id", "demand_subgroup_id").with_columns(
        utility_profile_id=pl.Series([0, 0, 0, 1, 0]),
    )
    costs = SimpleNamespace(get_utility_profiles=lambda groups, segments: (profiles, {}))
    contexts = ScoringContexts.build(demand, costs, None)
    assert contexts.demand_groups.height == 4
    assert contexts.demand_groups["n_persons"].sum() == 200.
    shared = contexts.unit_map.filter(pl.col("demand_group_id").is_in([1, 2]))
    assert shared["_score_context_id"].n_unique() == 1
    reversed_contexts = ScoringContexts.build(demand.reverse(), costs, None)
    assert_frame_equal(contexts.unit_map.sort("demand_group_id"),
                       reversed_contexts.unit_map.sort("demand_group_id"))

    # The same physical alternative is stored once, with its full history.
    steps = demand.head(2).select("demand_group_id", "demand_subgroup_id").with_columns(
        activity_seq_id=pl.lit(1), time_seq_id=pl.lit(2),
        dest_seq_id=pl.lit(3), mode_seq_id=pl.lit(4), seq_step_index=pl.lit(0),
        first_seen_iteration=pl.Series([1, 2]), last_seen_iteration=pl.Series([2, 3]),
        last_active_iteration=pl.Series([2, 1]),
    )
    compact = contexts.collapse_step_rows(steps.lazy())
    assert compact.height == 1
    assert compact["first_seen_iteration"].item() == 1
    assert compact["last_seen_iteration"].item() == 3
    assert compact["last_active_iteration"].item() == 2
    expanded = contexts.expand_step_rows(compact).collect()
    assert expanded["demand_group_id"].sort().to_list() == [1, 2]

    allocation = steps.drop("seq_step_index", "first_seen_iteration", "last_seen_iteration",
                            "last_active_iteration").with_columns(
        n_persons=pl.Series([20., 30.]), utility=pl.lit(5.),
    )
    assert contexts.collapse_allocations(allocation)["n_persons"].item() == 50.


def test_context_reuse_uses_demand_and_cost_input_identity():
    demand = pl.DataFrame({
        "demand_group_id": [1, 2], "demand_subgroup_id": [0, 0],
        "home_zone_id": [10, 10], "country": ["FR", "FR"], "csp": [1, 1],
        "n_persons": [20.0, 30.0], "allowed_activities": [["home"], ["home"]],
    })
    calls = []

    class Costs(InMemoryAsset):
        def __init__(self, changed):
            super().__init__({"changed_profile": changed})

        def get_utility_profiles(self, groups, _segments):
            calls.append(True)
            return groups.select("demand_group_id", "demand_subgroup_id").with_columns(
                utility_profile_id=pl.when((pl.col("demand_group_id") == 2) & self.inputs["changed_profile"])
                .then(pl.lit(1)).otherwise(pl.lit(0)).cast(pl.UInt32),
            ), {}

    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator._scoring_context_cache = {}
    segments = []
    original = coordinator._get_scoring_contexts(0, demand, Costs(False), segments)
    assert coordinator._get_scoring_contexts(1, demand, Costs(False), segments) is original
    assert len(calls) == 1
    changed = coordinator._get_scoring_contexts(1, demand, Costs(True), segments)
    assert changed is not original and changed.demand_groups.height == 2
    changed_demand = demand.with_columns(n_persons=pl.col("n_persons") + 10)
    other = coordinator._get_scoring_contexts(0, changed_demand, Costs(False), segments)
    assert other is not original and other.demand_groups["n_persons"].sum() == 70.0
    assert len(calls) == 3
    by_inputs = coordinator._get_scoring_contexts(0, demand, Costs(False), segments, demand_identity="population")
    equivalent = coordinator._get_scoring_contexts(
        1, demand.clone(), Costs(False), [], demand_identity="population",
    )
    assert equivalent is by_inputs
    different_population = coordinator._get_scoring_contexts(
        1, demand, Costs(False), segments, demand_identity="other-population",
    )
    assert different_population is not by_inputs


def test_factored_candidates_preserve_active_history_and_reject_inconsistent_steps():
    units = pl.DataFrame({
        "demand_group_id": [1, 2], "demand_subgroup_id": [0, 0], "_score_context_id": [0, 0],
    }).with_columns(pl.all().cast(pl.UInt32))
    contexts = ScoringContexts(pl.DataFrame(), units)
    rows = units.drop("_score_context_id").join(
        pl.DataFrame({"seq_step_index": [0, 1], "from": [1, 2], "to": [2, 1]}), how="cross"
    ).with_columns(
        pl.col("seq_step_index").cast(pl.UInt8),
        activity_seq_id=pl.lit(1, dtype=pl.UInt32), time_seq_id=pl.lit(1, dtype=pl.UInt32),
        dest_seq_id=pl.lit(1, dtype=pl.UInt32), mode_seq_id=pl.lit(1, dtype=pl.UInt32),
        country=pl.lit("fr"), csp=pl.lit("worker"), activity=pl.lit("work"), mode=pl.lit("car"),
        duration_per_pers=pl.lit(1.0), departure_time=pl.lit(8.0), arrival_time=pl.lit(9.0),
        next_departure_time=pl.lit(10.0), iteration=pl.lit(1, dtype=pl.UInt16),
        first_seen_iteration=pl.lit(1, dtype=pl.UInt16), last_seen_iteration=pl.lit(1, dtype=pl.UInt16),
        last_active_iteration=pl.lit(None, dtype=pl.UInt16),
    )
    steps = rows.select(STEP_COLUMNS).unique()
    members = rows.select(MEMBERSHIP_COLUMNS).unique()
    shared = ScoringCandidateSteps(steps, members.lazy())
    previous = CompactCandidateSteps(steps, members, tuple(rows.columns))
    current = rows.filter(pl.col("demand_group_id") == 1).select(PLAN_KEY_COLS).unique()
    active = rows.filter(pl.col("demand_group_id") == 1).with_columns(
        last_seen_iteration=pl.lit(5, dtype=pl.UInt16)
    )
    # The occupied row from the previous state wins over the pool's active row.
    raw = pl.concat([rows, active, rows.filter(pl.col("demand_group_id") == 1)]).unique(
        subset=PLAN_KEY_COLS + ["seq_step_index"], keep="last"
    )
    actual = contexts.collapse_factored_step_rows(shared, active, previous, current)
    assert actual is not None
    expanded, physical, memberships = contexts.collapse_factored_step_rows(
        shared, active, previous, current, return_parts=True,
    )
    assert_frame_equal(expanded.sort(PLAN_KEY_COLS + ["seq_step_index"]), actual.sort(PLAN_KEY_COLS + ["seq_step_index"]))
    assert_frame_equal(
        memberships.join(physical, on=PLAN_COLUMNS).select(actual.columns).sort(PLAN_KEY_COLS + ["seq_step_index"]),
        actual.sort(PLAN_KEY_COLS + ["seq_step_index"]),
    )
    unexpanded, physical_only, memberships_only = contexts.collapse_factored_step_rows(
        shared, active, previous, current, return_parts=True, expand=False,
    )
    assert unexpanded is None
    assert_frame_equal(
        memberships_only.join(physical_only, on=PLAN_COLUMNS).select(actual.columns)
        .sort(PLAN_KEY_COLS + ["seq_step_index"]),
        actual.sort(PLAN_KEY_COLS + ["seq_step_index"]),
    )
    keys = PLAN_KEY_COLS + ["seq_step_index"]
    assert_frame_equal(actual.sort(keys), contexts.collapse_step_rows(raw).select(actual.columns).sort(keys))
    shared_history = CompactCandidateSteps(
        steps, members.filter(pl.col("demand_group_id") == 1).with_columns(
            demand_group_id=pl.lit(0, dtype=pl.UInt32)
        ), tuple(rows.columns), unit_map=units,
    )
    occupied = pl.concat([
        current, current, current.with_columns(demand_group_id=pl.lit(2, dtype=pl.UInt32)),
        current.with_columns(mode_seq_id=pl.lit(99, dtype=pl.UInt32)),
    ]).lazy()
    for history in (previous, shared_history):
        expected = history.memberships().join(occupied, on=PLAN_KEY_COLS, how="semi").collect()
        selected = history.memberships_for_plans(occupied).collect()
        assert_frame_equal(selected.sort(PLAN_KEY_COLS), expected.sort(PLAN_KEY_COLS))
    from_shared_history = contexts.collapse_factored_step_rows(shared, active, shared_history, current)
    assert_frame_equal(from_shared_history.sort(keys), actual.sort(keys))
    other_steps = steps.with_columns(mode_seq_id=pl.lit(2, dtype=pl.UInt32), mode=pl.lit("walk"))
    other_members = members.with_columns(mode_seq_id=pl.lit(2, dtype=pl.UInt32))
    both = ScoringCandidateSteps(
        pl.concat([steps, other_steps]), pl.concat([members, other_members]).lazy(),
    )
    all_candidates = contexts.collapse_factored_step_rows(both, active, previous, current)
    early = contexts.collapse_factored_step_rows(
        both, active, previous, current,
        context_filter=lambda groups: groups.filter(pl.col("mode_seq_id") == 1),
    )
    assert_frame_equal(early.sort(keys), all_candidates.filter(pl.col("mode_seq_id") == 1).sort(keys))
    assert contexts.collapse_factored_step_rows(
        shared, active.with_columns(pl.col("to") + 10), previous, current
    ) is None
    assert contexts.collapse_factored_step_rows(
        shared, active.filter(pl.col("seq_step_index") == 0), previous, current
    ) is None

    varied = members.with_columns(
        iteration=pl.when(pl.col("demand_group_id") == 2).then(pl.lit(2, dtype=pl.UInt16))
        .otherwise(pl.col("iteration"))
    )
    merged = contexts.collapse_factored_step_rows(
        ScoringCandidateSteps(steps, varied.lazy()), None, None, current.head(0)
    )
    assert merged is not None
    assert merged["iteration"].unique().to_list() == [1]
    varied_rows = rows.with_columns(
        iteration=pl.when(pl.col("demand_group_id") == 2).then(pl.lit(2, dtype=pl.UInt16))
        .otherwise(pl.col("iteration"))
    )
    assert_frame_equal(merged.sort(keys), contexts.collapse_step_rows(varied_rows).select(merged.columns).sort(keys))


def test_compact_mnl_preserves_population_weighted_flows(monkeypatch):
    # Two equivalent populations contribute to the same physical OD choices.
    demand = pl.DataFrame({
        "demand_group_id": [1, 2], "demand_subgroup_id": [0, 0],
        "n_persons": [20., 30.],
    })
    compact_demand = pl.DataFrame({
        "demand_group_id": [0], "demand_subgroup_id": [0], "n_persons": [50.],
    })
    contexts = ScoringContexts(compact_demand, demand.select(
        "demand_group_id", "demand_subgroup_id",
    ).with_columns(_score_context_id=pl.lit(0)))
    choices = demand.select("demand_group_id", "demand_subgroup_id").join(
        pl.DataFrame({"dest_seq_id": [1, 2], "utility": [1., 2.]}), how="cross",
    ).with_columns(
        activity_seq_id=pl.lit(1), time_seq_id=pl.lit(1), mode_seq_id=pl.lit(1),
        is_feasible=pl.lit(True),
    ).with_row_index("plan_id")
    compact_choices = choices.filter(pl.col("demand_group_id") == 1).with_columns(
        demand_group_id=pl.lit(0),
    )
    updater = PlanUpdater()
    monkeypatch.setattr(updater, "get_current_plan_steps", lambda allocation, steps: pl.DataFrame())
    def allocate(rows, scoring_contexts):
        evaluation = SimpleNamespace(
            possible_plan_utility=rows, possible_plan_steps=pl.DataFrame(),
            scoring_contexts=scoring_contexts,
        )
        allocation, _ = updater.estimate_mnl_allocation(evaluation, demand, logit_scale=0.25)
        return allocation.group_by("dest_seq_id").agg(pl.col("n_persons").sum()).sort("dest_seq_id")
    expanded = allocate(choices, None)
    compact = allocate(compact_choices, contexts)
    assert compact["n_persons"].sum() == pytest.approx(50.)
    assert_frame_equal(expanded, compact)


def test_reusing_scores_maps_timing_and_population_back_to_each_demand_unit():
    units = pl.DataFrame({
        "demand_group_id": [1, 2], "demand_subgroup_id": [0, 0], "_score_context_id": [0, 0],
    }).with_columns(pl.all().cast(pl.UInt32))
    contexts = ScoringContexts(pl.DataFrame(), units)
    current = units.drop("_score_context_id").with_columns(
        activity_seq_id=pl.lit(1, dtype=pl.UInt32), time_seq_id=pl.lit(1, dtype=pl.UInt32),
        dest_seq_id=pl.lit(1, dtype=pl.UInt32), mode_seq_id=pl.lit(1, dtype=pl.UInt32),
        n_persons=pl.Series([20.0, 30.0]), utility=pl.lit(1.0),
    )
    # Stay-home has no entry in the modeled-time mapping and retains its time.
    current = pl.concat([current, current.head(1).with_columns(
        activity_seq_id=pl.lit(0, dtype=pl.UInt32), time_seq_id=pl.lit(0, dtype=pl.UInt32),
        dest_seq_id=pl.lit(0, dtype=pl.UInt32), mode_seq_id=pl.lit(0, dtype=pl.UInt32),
        n_persons=pl.lit(10.0),
    )])
    current, old_index = add_plan_id(current, previous_index=None)
    choices = pl.DataFrame({
        "demand_group_id": [0, 0], "demand_subgroup_id": [0, 0],
        "activity_seq_id": [0, 1], "time_seq_id": [0, 2],
        "dest_seq_id": [0, 1], "mode_seq_id": [0, 1],
        "plan_id": [0, 1], "utility": [0.0, 5.0], "is_feasible": [True, True],
    }).with_columns(pl.col(PLAN_KEY_COLS + ["plan_id"]).cast(pl.UInt32))
    steps = choices.with_columns(
        seq_step_index=pl.lit(1, dtype=pl.UInt8), activity=pl.lit("home"),
        mode=pl.when(pl.col("mode_seq_id") == 0).then(pl.lit("walk")).otherwise(pl.lit("car")),
        departure_time=pl.lit(8.0), arrival_time=pl.lit(9.0), next_departure_time=pl.lit(24.0),
    )
    mapping = choices.filter(pl.col("mode_seq_id") == 1).select(PLAN_KEY_COLS).with_columns(
        previous_time_seq_id=pl.lit(1, dtype=pl.UInt32),
    )
    history = steps.filter(pl.col("mode_seq_id") == 1).drop("plan_id", "utility", "is_feasible").with_columns(
        country=pl.lit("FR"), csp=pl.lit("worker"), iteration=pl.lit(1, dtype=pl.UInt16),
        first_seen_iteration=pl.lit(1, dtype=pl.UInt16), last_seen_iteration=pl.lit(1, dtype=pl.UInt16),
        last_active_iteration=pl.lit(None, dtype=pl.UInt16), activity=pl.lit("work"),
        **{"from": pl.lit(1), "to": pl.lit(2)}, mode=pl.lit("car"),
        duration_per_pers=pl.lit(1.0), departure_time=pl.lit(8.0), arrival_time=pl.lit(9.0),
        next_departure_time=pl.lit(10.0),
    )
    # A single context and step cannot save space until the memberships are
    # expanded to both demand units. Preserve that history without expanding steps.
    assert isinstance(CompactCandidateSteps.from_frame(history), pl.DataFrame)
    evaluation = CandidatePlanEvaluation(
        candidate_plan_steps=history,
        possible_plan_steps=steps.lazy(), possible_plan_utility=choices,
        plan_id_index=pl.DataFrame(), scoring_contexts=contexts, time_mapping=mapping,
    )
    expanded = PlanUpdater().expand_context_evaluation(evaluation, current, old_index)
    assert isinstance(expanded.candidate_plan_steps, CompactCandidateSteps)
    assert expanded.candidate_plan_steps.unit_map is not None
    assert expanded.candidate_plan_steps.membership.height == 1
    assert expanded.candidate_plan_steps.memberships().collect().height == 2
    assert_frame_equal(
        expanded.candidate_plan_steps.lazy().collect().sort(PLAN_KEY_COLS),
        contexts.expand_step_rows(history).collect().sort(PLAN_KEY_COLS),
    )
    assert expanded.current_plans["n_persons"].sort().to_list() == [10.0, 20.0, 30.0]
    assert expanded.current_plans["time_seq_id"].unique().sort().to_list() == [0, 2]
    assert expanded.current_plans["plan_id"].n_unique() == 3
    assert expanded.possible_plan_utility.height == 4
    assert expanded.possible_plan_steps.collect().height == 4
    assert expanded.current_plans.join(
        expanded.possible_plan_utility, on=PLAN_KEY_COLS + ["plan_id"], how="anti"
    ).height == 0
    shared = PlanUpdater().expand_context_evaluation(
        evaluation, current, old_index, share_transition_probabilities=True,
    )
    assert isinstance(shared.possible_plan_utility, pl.LazyFrame)
    assert_frame_equal(shared.plan_id_index, expanded.plan_id_index)
    assert_frame_equal(shared.current_plans.sort(PLAN_KEY_COLS), expanded.current_plans.sort(PLAN_KEY_COLS))
    compact_index = choices.select(PLAN_KEY_COLS + ["plan_id"])
    compact_origins = contexts.collapse_allocations(expanded.current_plans).join(compact_index, on=PLAN_KEY_COLS)
    compact_evaluation = replace(
        evaluation, plan_id_index=compact_index, current_plans=compact_origins,
        time_sequence_steps=pl.DataFrame({"time_seq_id": [0, 2], "reference_time_seq_id": [0, 1]}).cast(pl.UInt32),
    )
    extra_choice = choices.filter(pl.col("mode_seq_id") == 1).with_columns(
        mode_seq_id=pl.lit(2, dtype=pl.UInt32), plan_id=pl.lit(2, dtype=pl.UInt32),
    )
    extra_steps = steps.filter(pl.col("mode_seq_id") == 1).with_columns(
        mode_seq_id=pl.lit(2, dtype=pl.UInt32), plan_id=pl.lit(2, dtype=pl.UInt32),
    )
    full_steps = pl.concat([steps, extra_steps]).with_columns(
        distance=pl.lit(1.0), time=pl.lit(1.0), duration_per_pers=pl.lit(1.0),
        **{"from": pl.lit(1), "to": pl.lit(2)},
    ).lazy()
    with_extra = replace(
        compact_evaluation, possible_plan_steps=full_steps,
        possible_plan_utility=pl.concat([choices, extra_choice]),
    )
    settings = {"transport_zones": None, "enable_transition_distance_model": False,
                "transition_distance_threshold": float("inf"), "transition_utility_pruning_delta": 3.0,
                "transition_logit_scale": 1.0, "min_transition_utility_gain": 0.0}
    reduced = MultiScenarioRunAsset._compact_completed_candidate_history(
        with_extra, PlanUpdater(), BehaviorChangeScope.NO_TRANSITIONS,
        keep_all_steps=False, transition_parameters=settings,
    )
    assert set(reduced.possible_plan_steps["plan_id"].to_list()) == {0, 1}
    class NoRepeatedEligibility(PlanUpdater):
        def build_allowed_plan_transitions(self, *args, **kwargs):
            raise AssertionError("Compaction should reuse the supplied target IDs")

    reused = MultiScenarioRunAsset._compact_completed_candidate_history(
        with_extra, NoRepeatedEligibility(), BehaviorChangeScope.NO_TRANSITIONS,
        keep_all_steps=False, transition_parameters=settings,
        reachable_ids=pl.DataFrame({"plan_id": [0, 1]}, schema={"plan_id": pl.UInt32}),
    )
    assert_frame_equal(reused.possible_plan_steps.sort("plan_id"), reduced.possible_plan_steps.sort("plan_id"))
    forced_steps = MultiScenarioRunAsset._compact_completed_candidate_history(
        replace(with_extra, possible_plan_utility=with_extra.possible_plan_utility.with_columns(
            is_feasible=pl.col("mode_seq_id") != 1,
        )), PlanUpdater(), BehaviorChangeScope.NO_TRANSITIONS,
        keep_all_steps=False, transition_parameters=settings,
    )
    assert set(forced_steps.possible_plan_steps["plan_id"].to_list()) == {0, 1, 2}
    complete = MultiScenarioRunAsset._compact_completed_candidate_history(
        with_extra, PlanUpdater(), BehaviorChangeScope.NO_TRANSITIONS,
        keep_all_steps=True, transition_parameters=settings,
    )
    assert set(complete.possible_plan_steps["plan_id"].to_list()) == {0, 1, 2}
    finite_distance = MultiScenarioRunAsset._compact_completed_candidate_history(
        with_extra, PlanUpdater(), BehaviorChangeScope.NO_TRANSITIONS, keep_all_steps=False,
        transition_parameters={**settings, "enable_transition_distance_model": True,
                               "transition_distance_threshold": 1.0},
    )
    assert set(finite_distance.possible_plan_steps["plan_id"].to_list()) == {0, 1, 2}
    narrowed = PlanUpdater().expand_context_evaluation(
        compact_evaluation, current, old_index, BehaviorChangeScope.NO_TRANSITIONS,
        share_transition_probabilities=True, transition_parameters={"transport_zones": None},
    )
    assert narrowed.plan_id_index.height == 5
    assert expanded.plan_id_index.height == 6
    assert_frame_equal(narrowed.current_plans.drop("plan_id").sort(PLAN_KEY_COLS),
                       expanded.current_plans.drop("plan_id").sort(PLAN_KEY_COLS))
    assert_frame_equal(narrowed.plan_id_index.join(old_index, on=PLAN_KEY_COLS + ["plan_id"]), old_index)
    infeasible = replace(
        compact_evaluation,
        possible_plan_utility=choices.with_columns(is_feasible=pl.col("mode_seq_id") == 0),
    )
    forced = PlanUpdater().expand_context_evaluation(
        infeasible, current, old_index, BehaviorChangeScope.NO_TRANSITIONS,
        share_transition_probabilities=True, transition_parameters={"transport_zones": None},
    )
    # Both units can be forced to stay home even though the normal scope
    # would keep them on their current plans.
    assert forced.plan_id_index.height == 6
    updater = PlanUpdater()
    for scope in BehaviorChangeScope:
        limited = updater.expand_context_evaluation(
            compact_evaluation, current, old_index, scope,
            share_transition_probabilities=True, transition_parameters={"transport_zones": None},
        )
        actual = updater.get_context_transition_probabilities(limited, scope, transport_zones=None)
        reference_choices = expanded.possible_plan_utility.join(
            compact_evaluation.time_sequence_steps, on="time_seq_id", how="left"
        )
        expected = updater.get_transition_probabilities(
            expanded.current_plans, reference_choices.lazy(), pl.DataFrame(), scope, transport_zones=None,
        )
        columns = [column for column in expected.columns if column not in ("plan_id_from", "plan_id_trans")]
        keys = PLAN_KEY_COLS + [column + "_trans" for column in PLAN_COLUMNS]
        assert_frame_equal(actual.select(columns).sort(keys), expected.select(columns).sort(keys))
    for threshold in (0.0, 100.0):
        distance_settings = {
            "transport_zones": None, "enable_transition_distance_model": True,
            "transition_distance_threshold": threshold, "transition_distance_friction": 0.5,
        }
        limited = updater.expand_context_evaluation(
            compact_evaluation, current, old_index, BehaviorChangeScope.FULL_REPLANNING,
            share_transition_probabilities=True, transition_parameters=distance_settings,
        )
        actual = updater.get_context_transition_probabilities(
            limited, BehaviorChangeScope.FULL_REPLANNING, **distance_settings,
        )
        expected = updater.get_transition_probabilities(
            expanded.current_plans, reference_choices.lazy(), expanded.possible_plan_steps.collect(),
            BehaviorChangeScope.FULL_REPLANNING, **distance_settings,
        )
        columns = [column for column in expected.columns if column not in ("plan_id_from", "plan_id_trans")]
        assert_frame_equal(actual.select(columns).sort(keys), expected.select(columns).sort(keys))
        if threshold == 100.0:
            assert actual["tau_transition"].max() > 0.0
