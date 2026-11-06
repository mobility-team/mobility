import pytest
import polars as pl

from mobility.trips.group_day_trips.core.parameters import (
    GroupDayTripsActivitySequenceParameters,
    GroupDayTripsParameters,
    GroupDayTripsRunParameters,
    GroupDayTripsOutputParameters,
    GroupDayTripsAdaptiveSearchParameters,
    BehaviorChangePhase,
)


@pytest.mark.parametrize(
    ("effort", "expected"),
    [("low", (1, 1, 1, 3, 1)), ("medium", (3, 3, 3, 5, 3)), ("high", (6, 5, 5, 8, 5))],
)
def test_effort_resolves_search_and_replication_settings(effort, expected):
    parameters = GroupDayTripsParameters(effort=effort)
    assert (
        parameters.activity_sequences.k_activity_sequences,
        parameters.destination_sequences.k_destination_sequences,
        parameters.mode_sequences.k_mode_sequences,
        parameters.adaptive_search.max_inner_iterations,
        parameters.run.n_replications,
    ) == expected
    assert [parameters.run.seed_for_replication(i) for i in range(expected[-1])] == list(range(expected[-1]))


def test_effort_preserves_explicit_nested_values_and_infers_seeds():
    parameters = GroupDayTripsParameters(
        effort="high",
        activity_sequences=GroupDayTripsActivitySequenceParameters(k_activity_sequences=None),
        run=GroupDayTripsRunParameters(seeds=[10, 20]),
    )
    assert parameters.activity_sequences.k_activity_sequences is None
    assert parameters.destination_sequences.k_destination_sequences == 5
    assert parameters.run.n_replications == 2
    assert parameters.with_replication(1).run.seed == 20


def test_effort_label_does_not_change_resolved_input_identity():
    from mobility.runtime.assets.input_hashing import hash_inputs

    low = GroupDayTripsParameters(effort="low")
    manual = GroupDayTripsParameters(
        effort="high", run={"n_replications": 1},
        activity_sequences={"k_activity_sequences": 1},
        destination_sequences={"k_destination_sequences": 1},
        mode_sequences={"k_mode_sequences": 1},
        adaptive_search={"max_search_batches": 3},
    )
    assert hash_inputs(low.model_dump()) == hash_inputs(manual.model_dump())


def test_conflicting_explicit_seed_count_is_rejected():
    with pytest.raises(ValueError, match="one seed per replication"):
        GroupDayTripsRunParameters(n_replications=3, seeds=[10, 20])


def test_step_and_batch_names_keep_the_existing_settings():
    assert GroupDayTripsRunParameters(n_steps=4).n_iterations == 4
    assert GroupDayTripsAdaptiveSearchParameters(max_search_batches=7).max_inner_iterations == 7
    assert BehaviorChangePhase(start_step=3, scope="mode_replanning").start_iteration == 3
    assert GroupDayTripsOutputParameters(checkpoint_each_step=True).checkpoint_each_iteration
    with pytest.raises(ValueError, match="either n_steps or n_iterations"):
        GroupDayTripsRunParameters(n_steps=2, n_iterations=2)
    with pytest.raises(ValueError, match="either seed or seeds"):
        GroupDayTripsRunParameters(seed=2, seeds=[3])


def test_calibration_bins_keep_boundaries_labels_and_missing_values():
    from mobility.trips.group_day_trips.evaluation.calibration_plan_steps import distance_bin_expr, time_bin_expr

    values = pl.DataFrame({"distance": [-1.0, 0.0, 1.0, 1e6, None, float("nan")],
                           "time": [-1.0, 0.0, 1.0 / 12, 1e9 / 60, None, float("nan")]})
    result = values.select(distance_bin_expr(), time_bin_expr())
    assert result[:, 0].to_list() == ["[-inf, 0)", "[0, 1)", "[1, 5)", "[1000000, inf)", None, None]
    assert result[:, 1].to_list() == ["[-inf, 0)", "[0, 5)", "[5, 10)", "[1000000000, inf)", None, None]
