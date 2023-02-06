import mobility


def test_trip_sampler():
    ts = mobility.TripSampler()
    trips = ts.get_trips(
        csp="3",
        csp_household="3",
        urban_unit_category="C",
        n_pers="2",
        n_cars="0",
        n_years=1,
    )
