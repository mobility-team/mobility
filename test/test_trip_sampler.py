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
    ts2 = mobility.TripSampler()
    trips2 = ts2.get_trips(
        csp="1",
        csp_household="1",
        urban_unit_category="B",
        n_pers="1",
        n_years=1,
    )
