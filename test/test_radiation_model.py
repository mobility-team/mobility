from mobility.radiation_departments import *

def test_radiation_model():
    lst_departments = ["90"]

    # GET DATA
    (
        sources_territory,
        sinks_territory,
        costs_territory,
        coordinates,
        raw_flowDT,
    ) = get_data_for_model(lst_departments)

    # FIRST RUN
    (
        predicted_flux,
        empirical_flux,
        coordinates,
        plot_sources,
    ) = run_model_for_territory(
        sources_territory.copy(),
        sinks_territory,
        costs_territory,
        coordinates,
        raw_flowDT,
        alpha=0,
        beta=1,
    )

    # COMPARE INSEE AND MODEL DATA
    compare_insee_and_model(predicted_flux, empirical_flux, coordinates, plot_sources)
    ct = compare_thresholds(predicted_flux, empirical_flux, thresholds=[400, 20])

    assert ct[400] == 0.8819304955370785
    assert ct[20] == 0.60780983183857

    optimise_parameters(sources_territory, sinks_territory, costs_territory, coordinates, raw_flowDT, test=True)
