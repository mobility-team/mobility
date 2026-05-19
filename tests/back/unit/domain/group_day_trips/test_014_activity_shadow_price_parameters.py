import pytest
import polars as pl

from mobility.activities.leisure.leisure import LeisureActivity
from mobility.activities.other import OtherActivity
from mobility.activities.shopping.shop import ShopActivity
from mobility.activities.studies.study import StudyActivity
from mobility.activities.work.work import WorkActivity


@pytest.mark.parametrize(
    "activity_cls",
    [
        WorkActivity,
        StudyActivity,
        ShopActivity,
        LeisureActivity,
        OtherActivity,
    ],
)
def test_non_home_activities_expose_destination_shadow_price_parameters(activity_cls):
    """Check that users can tune destination shadow prices from activity classes."""
    opportunities = pl.DataFrame({"to": [1], "n_opp": [1.0]}).to_pandas()

    activity = activity_cls(
        opportunities=opportunities,
        destination_soft_capacity_factor=1.5,
        destination_shadow_price_sensitivity_coefficient=0.7,
        destination_shadow_price_min_coefficient=-1.2,
        destination_sampling_overload_gamma=1.8,
        destination_sampling_min_attraction_factor=0.03,
    )

    parameters = activity.inputs["parameters"]

    assert parameters.destination_soft_capacity_factor == 1.5
    assert parameters.destination_shadow_price_sensitivity_coefficient == 0.7
    assert parameters.destination_shadow_price_min_coefficient == -1.2
    assert parameters.destination_sampling_overload_gamma == 1.8
    assert parameters.destination_sampling_min_attraction_factor == 0.03
