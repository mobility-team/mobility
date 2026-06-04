from mobility.trips.group_day_trips.core.run import Run


def test_remove_clears_final_outputs_and_keeps_shared_iteration_assets(tmp_path):
    """Check that remove() leaves reusable iteration cache files in place."""
    final_output_paths = {
        "plan_steps": tmp_path / "plan_steps.parquet",
        "opportunities": tmp_path / "opportunities.parquet",
    }
    for path in final_output_paths.values():
        path.write_text("final output", encoding="utf-8")

    shared_iteration_path = tmp_path / "iteration_state.parquet"
    shared_congestion_path = tmp_path / "vehicle_od_flows_car.parquet"
    shared_iteration_path.write_text("iteration state", encoding="utf-8")
    shared_congestion_path.write_text("congestion flows", encoding="utf-8")

    run = object.__new__(Run)
    run.cache_path = final_output_paths

    run.remove()

    assert all(path.exists() is False for path in final_output_paths.values())
    assert shared_iteration_path.exists()
    assert shared_congestion_path.exists()
