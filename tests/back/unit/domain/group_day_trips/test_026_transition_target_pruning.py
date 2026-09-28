from itertools import product
import math

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from mobility.trips.group_day_trips import BehaviorChangeScope
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater, PLAN_KEY_COLS


@pytest.mark.parametrize('scope', list(BehaviorChangeScope))
@pytest.mark.parametrize('delta,scale,gain', [(3., 1., 0.), (3., .25, 1.), (math.inf, 1., -2.), (3., 0., 0.)])
def test_target_pruning_matches_unpruned_cartesian_calculation(scope, delta, scale, gain):
    rows = []
    for i, (group, subgroup, activity, time, dest, mode) in enumerate(product(range(2), range(2), range(2), range(2), range(2), range(3))):
        rows.append((group, subgroup, activity, time, dest, mode, i, float((i * 7) % 23)))
    possible = pl.DataFrame(rows, schema=PLAN_KEY_COLS + ['plan_id', 'utility'], orient='row')
    current = possible.filter(pl.col('plan_id') % 3 != 1).with_columns(utility=pl.col('utility') - 1.)
    targets = possible
    if scope not in (BehaviorChangeScope.FULL_REPLANNING, BehaviorChangeScope.NO_TRANSITIONS):
        current = current.filter(pl.col('mode_seq_id') != 0)
        targets = targets.filter(pl.col('mode_seq_id') != 0)
    pairs = (current.lazy().select(PLAN_KEY_COLS + ['utility'])
        .rename({'utility': 'utility_prev_from'})
        .join(targets.lazy(), on=PLAN_KEY_COLS).rename({'plan_id': 'plan_id_from'})
        .join(targets.lazy(), on=['demand_group_id', 'demand_subgroup_id'], suffix='_trans')
        .rename({'plan_id': 'plan_id_trans'}))
    fixed = []
    if scope != BehaviorChangeScope.FULL_REPLANNING:
        fixed += ['activity_seq_id', 'time_seq_id']
    if scope in (BehaviorChangeScope.MODE_REPLANNING, BehaviorChangeScope.NO_TRANSITIONS):
        fixed += ['dest_seq_id']
    if scope == BehaviorChangeScope.NO_TRANSITIONS:
        fixed += ['mode_seq_id']
    for col in fixed:
        pairs = pairs.filter(pl.col(col) == pl.col(col + '_trans'))
    is_self = pl.col('plan_id_from') == pl.col('plan_id_trans')
    pairs = pairs.with_columns(maximum=pl.col('utility_trans').max().over(PLAN_KEY_COLS))
    window = delta / scale if scale > 0 else math.inf
    if math.isfinite(window):
        pairs = pairs.filter(is_self | (pl.col('utility_trans') >= pl.col('maximum') - window))
    expected = pairs.filter(is_self | (pl.col('utility_trans') >= pl.col('utility') + gain)).drop('maximum')
    updater = PlanUpdater()
    actual = updater.build_allowed_plan_transitions(current, possible.lazy(), scope,
        transition_utility_pruning_delta=delta, transition_logit_scale=scale, min_transition_utility_gain=gain)
    order = ['plan_id_from', 'plan_id_trans']
    assert_frame_equal(actual.collect().select(expected.collect_schema().names()).sort(order), expected.collect().sort(order))
    probability_order = PLAN_KEY_COLS + ['plan_id_trans']
    kwargs = dict(transition_revision_probability=.5, transition_logit_scale=scale)
    assert_frame_equal(
        updater.compute_transition_probabilities_from_utilities(actual, **kwargs).sort(probability_order),
        updater.compute_transition_probabilities_from_utilities(expected, **kwargs).sort(probability_order))
