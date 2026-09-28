"""Compare fixed-seed EMP pooling runs with the same original survey reference."""

import mobility
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from mobility.surveys.programme_pooling import validate_pooling


def test_emp_pooling_preserves_reference_metrics(test_data, tmp_path):
    """Check the full pipeline; this small area is not a behavioral validation."""
    zones = mobility.TransportZones(
        test_data["transport_zones_local_admin_unit_id"], radius=test_data["transport_zones_radius"],
    )
    population = mobility.Population(zones, sample_size=100)
    survey = mobility.EMPMobilitySurvey()
    activities = [mobility.HomeActivity(), mobility.WorkActivity(), mobility.OtherActivity(population=population)]
    modes = [mobility.CarMode(zones), mobility.WalkMode(zones), mobility.BicycleMode(zones)]
    checks = {}
    for label, settings in [
        ("original", mobility.ProgrammePoolingParameters()),
        ("pooled", mobility.ProgrammePoolingParameters(enabled=True, pooling_strength=30)),
    ]:
        setup = mobility.PopulationGroupDayTrips(
            population=population, activities=activities, modes=modes, surveys=[survey],
            parameters=mobility.GroupDayTripsParameters(
                programme_pooling=settings,
                run=mobility.GroupDayTripsRunParameters(n_iterations=1, seed=42),
                mode_sequences=mobility.GroupDayTripsModeSequenceParameters(mode_sequence_search_parallel=False),
            ),
        )
        run = setup.run("weekday")
        results = setup.results("weekday")
        # Fit on training respondents only, then save errors for review.
        observations = run.survey_plan_assets.get_reference_plan_assets_by_survey()[0]["plan_steps"].get()
        held_out = validate_pooling(observations, "fr-EMP-2019", settings, seed=42)
        assert held_out.height > 0
        held_out.write_csv(tmp_path / f"held-out-{label}.csv")

        # Donor diagnostics describe opt-in pooling, not legacy repaired diaries.
        if settings.enabled:
            assignments = results.diagnostics.programme_pooling(assignments=True)
            assert assignments.height > 0
            results.diagnostics.programme_pooling().write_csv(tmp_path / f"coverage-{label}.csv")
        else:
            with pytest.raises(ValueError, match="Enable programme pooling"):
                results.diagnostics.programme_pooling(assignments=True)
        reference_asset = results.survey_reference_plan_steps
        calibration = run._get_expected_diagnostics_inputs().calibration_plan_steps
        assert calibration.population_weighted_plan_steps is reference_asset
        reference = reference_asset.get().collect().sort(
            ["home_zone_id", "individual_id", "day_id", "seq_step_index"]
        )

        # Run through the normal entry point and compare all original metrics.
        assert run.get()["plan_steps"].collect().height > 0
        tables = {}
        for metric in ["trip_count", "travel_time", "travel_distance"]:
            table = results.metrics.metric(
                metric, by_variable="mode", reference="external", reference_view="values", output="table",
            )
            table.write_csv(tmp_path / f"{metric}-{label}.csv")
            tables[metric] = table.filter(pl.col("series") == "reference").sort("mode")
        checks[label] = {
            "reference_hash": reference_asset.inputs_hash,
            "model_hash": run.inputs_hash,
            "reference_steps": reference,
            "metrics": tables,
        }

    original, pooled = checks["original"], checks["pooled"]
    assert original["reference_hash"] == pooled["reference_hash"]
    assert original["model_hash"] != pooled["model_hash"]
    # The reference table includes each original day's p_reference.
    assert_frame_equal(original["reference_steps"], pooled["reference_steps"], check_exact=True)
    for metric in original["metrics"]:
        assert_frame_equal(original["metrics"][metric], pooled["metrics"][metric])
