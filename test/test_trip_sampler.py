import mobility
from mobility.carbon_computation import carbon_computation

def test_trip_sampler():
    # Create trip sampler
    ts = mobility.TripSampler()
    # Get annual trips of an individual of csp=3, living in a city center with another person
    trips = ts.get_trips(
        csp="3",
        csp_household="3",
        urban_unit_category="C",
        n_pers="2",
        n_cars="0",
        n_years=1,
    )
    # Compute carbon emissions for each trip
    emissions = carbon_computation(trips, ademe_database="Base_Carbone_V22.0.csv")
