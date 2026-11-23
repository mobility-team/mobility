import random

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.trips.group_day_trips.iterations.iteration_assets import IterationSeedsAsset


class SavedRandomState(InMemoryAsset):
    """Provide the random state that a completed iteration would save."""

    def __init__(self, state):
        super().__init__({"state": state})

    def create_and_get_asset(self):
        return None

    def get_rng_state(self):
        return self.state


def test_resumed_iteration_continues_the_random_sequence(tmp_path):
    initial_rng = random.Random(108)
    first = IterationSeedsAsset(
        previous_state=SavedRandomState(initial_rng.getstate()),
        iteration=1,
        base_folder=tmp_path,
    ).get()

    # Reload the saved seeds as a new process would, then continue from the
    # random state saved after the first iteration's two draws.
    reloaded = IterationSeedsAsset(
        previous_state=SavedRandomState(initial_rng.getstate()),
        iteration=1,
        base_folder=tmp_path,
    ).get_cached_asset()
    second = IterationSeedsAsset(
        previous_state=SavedRandomState(reloaded["rng_state_after_sampling"]),
        iteration=2,
        base_folder=tmp_path,
    ).get()

    uninterrupted = random.Random(108)
    assert [
        first["activity_sequences"],
        first["destination_sequences"],
        second["activity_sequences"],
        second["destination_sequences"],
    ] == [uninterrupted.getrandbits(64) for _ in range(4)]
    assert second["rng_state_after_sampling"] == uninterrupted.getstate()
