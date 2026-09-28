"""Check donor borrowing and the independent original-day reference."""

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal

from mobility.surveys.programme_pooling import (
    ProgrammePoolingParameters, build_donors, pool_programmes,
    pooling_diagnostics, validate_pooling, _fit_programme_probabilities,
)
from mobility.trips.group_day_trips.evaluation.population_weighted_plan_steps import (
    _weight_reference_observations,
)
from mobility.trips.group_day_trips.evaluation.model_trip_count_loss import build_trip_count_distribution


def respondent_steps():
    """Include matching schedules with different modes, distances and weights."""
    rows = []
    for day in range(40):
        csp = "fr-8a" if day == 39 else "fr-3"
        for index, activity, departure, arrival, next_departure in [
            (1, "work", 8.0, 9.0, 17.0), (2, "home", 17.0, 18.0, 18.0),
        ]:
            rows.append(dict(
                individual_id=day, day_id=day, is_weekday=True,
                city_category="R" if day < 2 else "C", csp=csp, n_cars="1",
                pondki=float(day % 3 + 1), activity_seq_id=1, time_seq_id=1,
                seq_step_index=index, activity=activity, departure_time=departure,
                arrival_time=arrival, next_departure_time=next_departure,
                duration_per_pers=next_departure - arrival,
                mode="car" if day % 2 else "walk", distance=float(day + 1),
                travel_time=arrival - departure,
                survey_name="fr-EMP-2019", country="fr",
            ))
    return pl.DataFrame(rows)


def test_pooling_keeps_original_reference_and_borrows_for_empty_segments():
    steps = respondent_steps()
    donors = build_donors(steps, "fr-EMP-2019")
    targets = pl.DataFrame(dict(is_weekday=[True] * 3, city_category=["R", "C", "X"],
                                csp=["fr-3"] * 3, n_cars=["1"] * 3))
    narrow = pool_programmes(donors, targets, ProgrammePoolingParameters(enabled=True, pooling_strength=1))
    broad = pool_programmes(donors, targets, ProgrammePoolingParameters(enabled=True, pooling_strength=100))
    keys = ["city_category", "donor_day_id"]
    assert_frame_equal(narrow.select(*keys, "p_reference").sort(keys),
                       broad.select(*keys, "p_reference").sort(keys))
    assert not np.allclose(narrow["p_sampling"], broad["p_sampling"])
    assert np.allclose(broad.group_by("city_category").agg(pl.col("p_sampling").sum())["p_sampling"], 1)
    assert broad.filter(pl.col("city_category") == "X")["p_reference"].sum() == 0
    assert "fr-8a" not in broad["source_segment"].struct.field("csp").to_list()
    assert broad.select("city_category", "donor_day_id").n_unique() == broad.height
    diagnostics = pooling_diagnostics(narrow)
    assert diagnostics.height == 3
    assert diagnostics.filter(pl.col("city_category") == "C")["borrowed_mass"][0] < diagnostics.filter(pl.col("city_category") == "R")["borrowed_mass"][0]

    # Modes and distances remain separate even though every schedule matches.
    reference = _weight_reference_observations(steps)
    rural = reference.filter(pl.col("city_category") == "R")
    assert rural["mode"].n_unique() == 2
    assert np.isclose((rural["distance"] * rural["p_reference"]).sum(), 10 / 3)


def test_reference_weights_count_each_day_once_and_are_stable_after_reordering():
    steps = respondent_steps()
    donors = build_donors(steps, "fr-EMP-2019")
    reordered = build_donors(steps.reverse(), "fr-EMP-2019")
    assert_frame_equal(donors, reordered)
    assert donors.height == 40
    rural = donors.filter(pl.col("city_category") == "R")
    assert np.allclose(rural["p_reference"], [1 / 3, 2 / 3])


def test_mode_and_distance_do_not_affect_sampling_and_held_out_scores():
    steps = respondent_steps()
    changed = steps.with_columns(mode=pl.lit("bicycle"), distance=pl.lit(999.0))
    settings = ProgrammePoolingParameters(enabled=True)
    donors = build_donors(steps, "fr-EMP-2019")
    targets = donors.select("is_weekday", "city_category", "csp", "n_cars").unique()
    assert_frame_equal(pool_programmes(donors, targets, settings),
                       pool_programmes(build_donors(changed, "fr-EMP-2019"), targets, settings))
    scores = validate_pooling(steps, "fr-EMP-2019", settings, seed=7)
    assert scores.height > 0
    assert scores.filter(pl.col("covered"))["absolute_error"].max() < 1e-10
    assert_frame_equal(scores, validate_pooling(changed, "fr-EMP-2019", settings, seed=7))


def test_pooling_is_disabled_by_default_and_keeps_original_probabilities():
    donors = build_donors(respondent_steps(), "fr-EMP-2019")
    targets = donors.select("is_weekday", "city_category", "csp", "n_cars").unique()
    settings = ProgrammePoolingParameters()
    assert not settings.enabled
    assignments = pool_programmes(donors, targets, settings)
    assert np.allclose(assignments["p_reference"], assignments["p_sampling"])


def test_reference_trip_counts_keep_respondents_with_identical_schedules_separate():
    reference = _weight_reference_observations(respondent_steps()).with_columns(
        n_persons=pl.col("p_reference")
    )
    distribution = build_trip_count_distribution(reference.lazy())
    assert distribution.filter(pl.col("trip_count_bin") == "2")["probability"][0] == 1.0


def test_fit_regularization_limits_adjustments_to_starting_weights():
    """A stronger penalty keeps trip counts closer to the starting mean of four."""
    trip_counts = np.array([[2.0], [4.0], [6.0]])
    target_weights = np.array([0.1, 0.1, 0.8])
    starting_weights = np.log(np.array([1 / 3, 1 / 3, 1 / 3]))
    flexible = _fit_programme_probabilities(trip_counts, target_weights, starting_weights, 0.001)
    restrained = _fit_programme_probabilities(trip_counts, target_weights, starting_weights, 100)
    assert abs(float(flexible @ trip_counts[:, 0]) - 5.4) < 0.01
    assert abs(float(restrained @ trip_counts[:, 0]) - 4.0) < 0.1
    assert np.isclose(flexible.sum(), 1)
    assert (flexible > 0).all()


def test_unsupported_held_out_segments_have_missing_predictions():
    """Missing programme support must not be interpreted as a zero-trip forecast."""
    # Each respondent is the only observed donor in their urban category.
    steps = respondent_steps().with_columns(city_category=pl.col("individual_id").cast(pl.String))
    scores = validate_pooling(steps, "fr-EMP-2019", ProgrammePoolingParameters(enabled=False), seed=42)
    assert scores.height > 0
    assert not scores["covered"].any()
    assert scores["prediction"].is_null().all()
    assert scores["absolute_error"].is_null().all()


def test_empty_donor_pool_stays_unsupported():
    donors = build_donors(respondent_steps(), "fr-EMP-2019")
    targets = donors.select("is_weekday", "city_category", "csp", "n_cars").unique()
    assignments = pool_programmes(donors.head(0), targets, ProgrammePoolingParameters(enabled=True))
    assert assignments.is_empty()
    assert pooling_diagnostics(assignments).is_empty()
