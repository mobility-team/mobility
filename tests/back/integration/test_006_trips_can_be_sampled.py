import mobility
import pytest

# Uncomment the next lines if you want to test interactively, outside of pytest,
# but still need the setup phase and input data defined in conftest.py
# Don't forget to recomment or the tests will not pass !

# from conftest import get_test_data, do_mobility_setup
# do_mobility_setup(True, False, False)
# test_data = get_test_data()
"""
@pytest.mark.dependency(
    depends=["tests/test_006_trips_can_be_sampled.py::test_006_trips_can_be_sampled"],
    scope="session"
 )
def test_007_trips_can_be_localized(test_data):

    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"]
    )

    car = mobility.CarMode(transport_zones)

    work_dest_params = mobility.WorkDestinationChoiceModelParameters()

    work_dest_cm = mobility.WorkDestinationChoiceModel(
        transport_zones,
        modes=[car],
        parameters=work_dest_params,
        n_possible_destinations=1
    )

    work_mode_cm = mobility.TransportModeChoiceModel(work_dest_cm)

    population = mobility.Population(
        transport_zones=transport_zones,
        sample_size=test_data["population_sample_size"]
    )

    trips = mobility.Trips(population)

    trips_localized = mobility.LocalizedTrips(
        trips=trips,
        mode_cm_list=[work_mode_cm],
        dest_cm_list=[work_dest_cm],
        keep_survey_cols=True
    )
    trips_localized = trips_localized.get()

    assert trips_localized.shape[0] > 0"""