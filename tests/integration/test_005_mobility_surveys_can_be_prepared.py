from mobility.parsers import MobilitySurvey
import pytest

@pytest.mark.dependency()
def test_005_mobility_surveys_can_be_prepared(test_data):
    
    ms_2019 = MobilitySurvey(source="EMP-2019")
    ms_2008 = MobilitySurvey(source="ENTD-2008")
    
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
