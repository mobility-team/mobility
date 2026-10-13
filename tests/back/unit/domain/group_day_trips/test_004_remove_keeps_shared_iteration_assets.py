from pathlib import Path
from types import SimpleNamespace


from mobility.trips.group_day_trips.core.run import Run


class _FileToRemove:
    """Small stand-in for one saved file owned by a run."""

    def __init__(self, path: Path) -> None:
        self.cache_path = path

    def remove(self) -> None:
        self.cache_path.unlink(missing_ok=True)


def test_remove_clears_run_checkpoints_and_keeps_shared_iteration_assets(tmp_path):
    """Check that remove preserves iteration outputs shared by other runs."""
    checkpoint_path = tmp_path / "run_checkpoint.parquet"
    transition_path = tmp_path / "shared_transition_events.parquet"
    shared_iteration_path = tmp_path / "shared_iteration_state.parquet"
    for path in (checkpoint_path, transition_path, shared_iteration_path):
        path.write_text("saved data", encoding="utf-8")

    iteration = SimpleNamespace(
        checkpoint_assets=lambda: {
            "current_plans": _FileToRemove(checkpoint_path),
        }
    )
    run = object.__new__(Run)
    run.iterations = SimpleNamespace(iteration=lambda number: iteration)
    run.iteration_state_assets = [
        SimpleNamespace(
            iteration=1,
            transition_events_asset=_FileToRemove(transition_path),
        )
    ]

    run.remove()

    assert not checkpoint_path.exists()
    assert transition_path.exists()
    assert shared_iteration_path.exists()
