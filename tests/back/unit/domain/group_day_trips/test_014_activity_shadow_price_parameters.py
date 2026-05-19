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
        destination_shadow_price_sensitivity=2.0,
        destination_shadow_price_min=-8.0,
    )

    parameters = activity.inputs["parameters"]

    assert parameters.destination_soft_capacity_factor == 1.5
    assert parameters.destination_shadow_price_sensitivity == 2.0
    assert parameters.destination_shadow_price_min == -8.0
