from mobility.parsers.mobility_survey.france import EMPMobilitySurvey
from mobility.parsers.mobility_survey.france import ENTDMobilitySurvey
import pytest

@pytest.mark.dependency()
def test_005_mobility_surveys_can_be_prepared(test_data):
    
    ms_2019 = EMPMobilitySurvey()
    ms_2008 = ENTDMobilitySurvey()
    
    ms_2019 = ms_2019.get()
    ms_2008 = ms_2008.get()
    
    dfs_names = [
        "short_trips",
        "days_trip",
        "long_trips",
        "travels",
        "n_travels",
        "p_immobility",
        "p_car",
        "p_det_mode"
    ]
    
    assert all([df_name in list(ms_2019.keys()) for df_name in dfs_names])
    assert all([df_name in list(ms_2008.keys()) for df_name in dfs_names])

    assert ms_2019["short_trips"].shape[0] > 0
    assert ms_2008["short_trips"].shape[0] > 0