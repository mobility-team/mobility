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


class _SimpleTable:
    """Small table wrapper with the `drop` method used by transport zones."""

    def __init__(self, rows):
        self.rows = rows

    def drop(self, *args, **kwargs):
        """Return rows without geometry, matching the transport-zones API."""
        return self.rows


class _SimpleTransportZones:
    """Minimal transport-zones object for state-waterfall tests."""

    class _StudyArea:
        def get(self):
            """Return local-admin-unit labels."""
            return _SimpleTable(
                [
                    {
                        "local_admin_unit_id": "100",
                        "local_admin_unit_name": "Test town",
                    }
                ]
            )

    study_area = _StudyArea()

    def get(self):
        """Return transport-zone to local-admin-unit links."""
        return _SimpleTable(
            [
                {
                    "transport_zone_id": "10",
                    "local_admin_unit_id": "100",
                }
            ]
        )


def test_state_waterfall_state_pair_uses_activity_time_destination_and_mode_sequences():
    transitions = pl.DataFrame(
        [
            {
                "iteration": 1,
                "demand_group_id": 1,
                "activity_seq_id": 11,
                "time_seq_id": 21,
                "dest_seq_id": 31,
                "mode_seq_id": 41,
                "activity_seq_id_trans": 12,
                "time_seq_id_trans": 22,
                "dest_seq_id_trans": 32,
                "mode_seq_id_trans": 42,
                "n_persons_moved": 3.0,
                "utility_prev_from": 1.0,
                "utility_prev_to": 2.0,
                "utility_from": 1.5,
                "utility_to": 2.5,
                "tau_transition": 0.0,
                "q_transition": 1.0,
                "adjustment_factor": 1.0,
                "is_self_transition": False,
                "trip_count_from": 1.0,
                "activity_time_from": 10.0,
                "travel_time_from": 1.0,
                "distance_from": 5.0,
                "steps_from": "#1 | to: 10 | activity: work | mode: car | dist_km: 5.0 | time_h: 1.0",
                "trip_count_to": 1.0,
                "activity_time_to": 9.5,
                "travel_time_to": 1.2,
                "distance_to": 7.0,
                "steps_to": "#1 | to: 10 | activity: work | mode: car | dist_km: 7.0 | time_h: 1.2",
            }
        ],
        schema=TRANSITION_EVENT_SCHEMA,
    )
    demand_groups = pl.DataFrame(
        [
            {
                "demand_group_id": 1,
                "home_zone_id": 10,
                "csp": "worker",
                "n_cars": 1,
                "n_persons": 3.0,
            }
        ]
    )

    _, state_pairs = state_waterfall(
        transitions=transitions.lazy(),
        demand_groups=demand_groups.lazy(),
        transport_zones=_SimpleTransportZones(),
        quantity="distance",
        plot=False,
    )

    assert state_pairs["state_pair"].to_list() == ["dg1-a11-t21-d31-mo41 -> dg1-a12-t22-d32-mo42"]
