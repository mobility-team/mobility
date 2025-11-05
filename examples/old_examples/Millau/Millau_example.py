"""
This script uses the radiation model on the french city Millau
and its surroundings to compute home-work mobility
and then compare the results with the INSEE data.

The territory considered in the radiation model is the french departments of
Aveyron, Lozère, Hérault, Gard, Tarn.

However, it is possible to use it on any set of contiguous departments
by changing the list of departments.

Prerequisites :
    * a CSV with all x,y coordinates (the coordinates are in the Lambert-93 projection)
      of the communes (COMMUNES_COORDINATES_CSV),
    * a XLSX with home-work flows between communes of the territory
      (work_home_fluxes_xlsx)
    * a CSV with all internal distances of the communes - based on superficies
      (communes_surfaces_csv)
"""

from mobility.radiation_departments import *
import time


if __name__ == "__main__":
    start_time = time.time()

    # CHOOSE DEPARTMENTS
    lst_departments = ["12", "48", "34", "30", "81"]

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
    compare_thresholds(predicted_flux, empirical_flux)
    compute_similarity_index(predicted_flux, empirical_flux, threshold=20)

    # OPTIMISATION #1 : BEST COEF
    """ Use this code if you want to test correction coefficients.
    In the case of Millau, the model produces less flows than INSEE,
    so a coefficient of 1.3 produces better results in this case.
    In the case of La Rochelle, it's a 1.1 coefficient that's the best.

    for c in np.arange(0.8, 2, 0.1):
        print("--------")
        print(f"c= {c:.1f}")
        predicted_flux_times = predicted_flux * c
        compare_insee_and_model(predicted_flux_times, empirical_flux,
                                coordinates, plot_sources)
        compare_thresholds(predicted_flux_times, empirical_flux)"""

    # OPTIMISATION #2 : BEST α & β VALUES
    """ Use this code to find the best α,β pair for the radiation model.
    The code will test every valid pair (α+β<=1) with 0.1 increments,
    so the execution time may be long (~20 minutes)
    You can use the coef you found at the previous step

    Best pair found is [0.0, 1.0], i.e. the default values, for Millau
    Best pair found is [0.2, 0.8] for La Rochelle (department 17 only)

    best_parameters=optimise_parameters(sources_territory, sinks_territory,
                                        costs_territory, coordinates,
                                        raw_flowDT, coef=1.3)"""

    # EXECUTION TIME
    end_time = time.time()
    print("Duration: ", end_time - start_time)
