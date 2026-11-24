from types import SimpleNamespace

from mobility.trips.group_day_trips.iterations.iterations import Iterations


def test_resume_skips_missing_or_stale_checkpoints(tmp_path, monkeypatch):
    iterations = Iterations(run_inputs_hash="run", is_weekday=True, base_folder=tmp_path)
    monkeypatch.setattr(iterations, "_find_latest_completed_iteration", lambda: 5)

    def checkpoint(iteration):
        # Iteration 3 has stale files, iteration 2 is complete, and iteration 1
        # is missing. A later marker must not make any of them complete.
        asset = SimpleNamespace(is_update_needed=lambda: iteration != 2)
        return SimpleNamespace(checkpoint_assets=lambda: {"state": asset})

    monkeypatch.setattr(iterations, "iteration", checkpoint)
    assert iterations.get_resume_iteration(3) == 2
    assert iterations.get_resume_iteration(1) is None
