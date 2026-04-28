import polars as pl
import pytest

from mobility.trips.group_day_trips.transitions.transition_metrics import state_waterfall
from mobility.trips.group_day_trips.transitions.transition_schema import TRANSITION_EVENT_SCHEMA


def test_state_waterfall_requires_persisted_transition_events():
    transitions = pl.DataFrame(schema=TRANSITION_EVENT_SCHEMA).lazy()

    with pytest.raises(ValueError, match="requires persisted transition events"):
        state_waterfall(
            transitions=transitions,
            demand_groups=pl.DataFrame().lazy(),
            transport_zones=object(),
            quantity="distance",
            plot=False,
        )
