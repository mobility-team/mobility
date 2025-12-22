import polars as pl 

class StateInitializer:
    """Builds initial chain demand, averages, and capacities for the model.
    
    Provides helpers to (1) aggregate population groups and attach survey
    chain probabilities, (2) compute mean activity durations, (3) create
    the stay-home baseline state, (4) derive destination capacities
    (sinks), and (5) fetch current OD costs.
    """
    
    def get_chains(self, population, surveys, motives, modes, is_weekday):
        """Aggregate demand groups and attach survey chain probabilities.
    
        Produces per-group trip chains with durations and anchor flags by
        joining population groups (zone/city category/CSP/cars) with survey
        chain probabilities, filtering by weekday/weekend, and indexing
        motive sequences.
    
        Args:
            population: Population container providing transport zones and groups.
            surveys: Iterable of survey objects exposing `get_chains_probability`.
            motives: Iterable of motives; used to mark anchors.
            modes: 
            is_weekday (bool): Select weekday (True) or weekend (False) chains.
    
        Returns:
            tuple[pl.DataFrame, pl.DataFrame]:
                - chains: Columns include
                  ["demand_group_id","motive_seq_id","seq_step_index","motive",
                   "n_persons","duration","is_anchor"].
                - demand_groups: Aggregated groups with
                  ["demand_group_id","home_zone_id","csp","n_cars","n_persons"].
        """

        # Map local admin units to urban unit categories (C, B, I, R) to be able
        # to get population counts by urban unit category
        lau_to_city_cat = ( 
            pl.from_pandas(
                population.transport_zones.study_area.get()
                .drop("geometry", axis=1)
                [["local_admin_unit_id", "urban_unit_category"]]
                .rename({"urban_unit_category": "city_category"}, axis=1)
            )
            .with_columns(
                country=pl.col("local_admin_unit_id").str.slice(0, 2)
            )
        )
        
        countries = lau_to_city_cat["country"].unique().to_list()

        # Aggregate population groups by transport zone, city category, socio pro 
        # category and number of cars in the household
        demand_groups = (
            
            pl.scan_parquet(population.get()["population_groups"])
            .rename({
                "socio_pro_category": "csp",
                "transport_zone_id": "home_zone_id",
                "weight": "n_persons"
            })
            .join(lau_to_city_cat.lazy(), on=["local_admin_unit_id"])
            .group_by(["country", "home_zone_id", "city_category", "csp", "n_cars"])
            .agg(pl.col("n_persons").sum())
            
            .collect(engine="streaming")
            
        )
        
        # Get the chain probabilities from the mobility surveys
        surveys = [s for s in surveys if s.country in countries]
        
        p_chain = (
            pl.concat(
                [
                    (
                        survey
                        .get_chains_probability(motives, modes)
                        .with_columns(
                            country=pl.lit(survey.inputs["country"])
                        )
                    )
                    for survey in surveys
                ]
            )
        )
        
        # Cast string columns to enums for better perf
        def get_col_values(df1, df2, col):
            s = pl.concat([df1.select(col), df2.select(col)]).to_series()
            return s.unique().sort().to_list()
            
        city_category_values = get_col_values(demand_groups, p_chain, "city_category")
        csp_values = get_col_values(demand_groups, p_chain, "csp")
        n_cars_values = get_col_values(demand_groups, p_chain, "n_cars")
        motive_values = p_chain["motive"].unique().sort().to_list()
        mode_values = p_chain["mode"].unique().sort().to_list()
        
        p_chain = (
            p_chain
            .with_columns(
                country=pl.col("country").cast(pl.Enum(countries)),
                city_category=pl.col("city_category").cast(pl.Enum(city_category_values)),
                csp=pl.col("csp").cast(pl.Enum(csp_values)),
                n_cars=pl.col("n_cars").cast(pl.Enum(n_cars_values)),
                motive=pl.col("motive").cast(pl.Enum(motive_values)),
                mode=pl.col("mode").cast(pl.Enum(mode_values)),
            )
        )
        
        # Create demand groups
        # !!! Sorting before creating ids is VERY important for reproducibility
        demand_groups = (
            demand_groups
            .with_columns(
                country=pl.col("country").cast(pl.Enum(countries)),
                city_category=pl.col("city_category").cast(pl.Enum(city_category_values)),
                csp=pl.col("csp").cast(pl.Enum(csp_values)),
                n_cars=pl.col("n_cars").cast(pl.Enum(n_cars_values))
            )
            .sort(["home_zone_id", "country", "city_category", "csp", "n_cars"])
            .with_row_index("demand_group_id")
        )
        
        # Create an index for motive sequences to avoid moving giant strings around
        # !!! Sorting before creating ids is VERY important for reproducibility
        motive_seqs = ( 
            p_chain
            .select(["motive_seq", "seq_step_index", "motive"])
            .unique()
        )
        
        motive_seq_index = (
            motive_seqs.select("motive_seq")
            .unique()
            .sort("motive_seq")
            .with_row_index("motive_seq_id")
        )
        
        motive_seqs = (
            motive_seqs
            .join(motive_seq_index, on="motive_seq")
        )
        
        
        p_chain = (
            p_chain
            .join(
                motive_seqs.select(["motive_seq", "motive_seq_id", "seq_step_index"]),
                on=["motive_seq", "seq_step_index"]
            )
            .drop("motive_seq")
        )
        
        # Compute the amount of demand (= duration) per demand group and motive sequence
        anchors = {m.name: m.is_anchor for m in motives}
        
        chains = (

            demand_groups
            .join(p_chain, on=["country", "city_category", "csp", "n_cars"])
            .filter(pl.col("is_weekday") == is_weekday)
            .drop("is_weekday")
            .with_columns(
                n_persons=pl.col("n_persons")*pl.col("p_seq")
            )
            .with_columns(
                duration_per_pers=(
                    (
                        pl.col("duration_morning")
                        + pl.col("duration_midday")
                        + pl.col("duration_evening")
                    )
                )
            )
            
            
        )
        
        chains_by_motive = (
            
            chains
            
            .group_by(["demand_group_id", "motive_seq_id", "seq_step_index", "motive"])
            .agg(
                n_persons=pl.col("n_persons").sum(),
                duration=(pl.col("n_persons")*pl.col("duration_per_pers")).sum()
            )
            
            .sort(["demand_group_id", "motive_seq_id",  "seq_step_index"])
            .with_columns(
                is_anchor=pl.col("motive").replace_strict(anchors)
            )
        
        )
        
        # Drop unecessary columns from demand groups
        demand_groups = (
            demand_groups
            .drop(["country", "city_category"])
        )

        return chains_by_motive, chains, demand_groups
    
    
    def get_mean_activity_durations(self, chains, demand_groups):
        
        """Compute mean per-person durations for activities and home-night.

        Uses chain step durations weighted by group sizes to estimate:
        - mean activity duration per (CSP, motive) excluding final steps, and
        - mean residual home-night duration per CSP.
        Enforces a small positive floor (~2 min) for numerical stability.
        
        Args:
            chains (pl.DataFrame): Output from `get_chains`.
            demand_groups (pl.DataFrame): Group metadata with CSP and sizes.
        
        Returns:
            tuple[pl.DataFrame, pl.DataFrame]:
                - mean_motive_durations: ["csp","motive","mean_duration_per_pers"].
                - mean_home_night_durations: ["csp","mean_home_night_per_pers"].
        """
        
        two_minutes = 120.0/3600.0
        
        chains = ( 
            chains
            .join(demand_groups.select(["demand_group_id", "csp"]), on="demand_group_id")
            .with_columns(duration_per_pers=pl.col("duration")/pl.col("n_persons"))
        )
        
        mean_motive_durations = (
            chains
            .filter(pl.col("seq_step_index") != pl.col("seq_step_index").max().over(["demand_group_id", "motive_seq_id"]))
            .group_by(["csp", "motive"])
            .agg(
                mean_duration_per_pers=pl.max_horizontal([
                    (pl.col("duration_per_pers")*pl.col("n_persons")).sum()/pl.col("n_persons").sum(),
                    pl.lit(two_minutes)
                ])
            )
        )
        
        mean_home_night_durations = (
            chains
            .group_by(["demand_group_id", "csp", "motive_seq_id"])
            .agg(
                n_persons=pl.col("n_persons").first(),
                home_night_per_pers=24.0 - pl.col("duration_per_pers").sum()
            )
            .group_by("csp")
            .agg(
                 mean_home_night_per_pers=pl.max_horizontal([
                     (pl.col("home_night_per_pers")*pl.col("n_persons")).sum()/pl.col("n_persons").sum(),
                     pl.lit(two_minutes)
                  ])
            )
        )
        
        return mean_motive_durations, mean_home_night_durations
    
    
    def get_stay_home_state(self, demand_groups, home_night_dur, motives):
        
        """Create the baseline 'stay home all day' state.

        Builds an initial state (iteration 0) per demand group with utility
        derived from mean home-night duration and the configured coefficient.
        
        Args:
            demand_groups (pl.DataFrame): ["demand_group_id","csp","n_persons"].
            home_night_dur (pl.DataFrame): ["csp","mean_home_night_per_pers"].
            parameters: Model parameters (expects `stay_home_utility_coeff`).
        
        Returns:
            tuple[pl.DataFrame, pl.DataFrame]:
                - stay_home_state: Columns
                  ["demand_group_id","iteration","motive_seq_id","mode_seq_id",
                   "dest_seq_id","utility","n_persons"] with zeros for seq IDs.
                - current_states: A clone of `stay_home_state` for iteration start.
        """
        
        home_motive = [m for m in motives if m.name == "home"][0]
        
        stay_home_state = (
            
            demand_groups.select(["demand_group_id", "csp", "n_persons"])
            .with_columns(
                iteration=pl.lit(0, pl.UInt32()),
                motive_seq_id=pl.lit(0, pl.UInt32()),
                mode_seq_id=pl.lit(0, pl.UInt32()),
                dest_seq_id=pl.lit(0, pl.UInt32())
            )
            .join(home_night_dur, on="csp")
            .with_columns(
                utility=( 
                    home_motive.value_of_time_stay_home 
                    * pl.col("mean_home_night_per_pers")
                    * (pl.col("mean_home_night_per_pers")/0.1/pl.col("mean_home_night_per_pers")).log()
                )
            )
            
            .select(["demand_group_id", "iteration", "motive_seq_id", "mode_seq_id", "dest_seq_id", "utility", "n_persons"])
        )
        
        current_states = ( 
            stay_home_state
            .select(["demand_group_id", "iteration", "motive_seq_id", "mode_seq_id", "dest_seq_id", "utility", "n_persons"])
            .clone()
        )
        
        return stay_home_state, current_states
    

    def get_sinks(self, chains, motives, transport_zones):
        
        """Compute destination capacities (sinks) per motive and zone.

        Scales available opportunities by demand per motive and a
        motive-specific saturation coefficient to derive per-destination
        capacity and initial availability.
        
        Args:
            chains (pl.DataFrame): Chains with total duration per motive.
            motives: Iterable of motives exposing `get_opportunities(...)` and
                `sink_saturation_coeff`.
            transport_zones: Zone container passed to motives.
        
        Returns:
            pl.DataFrame: ["to","motive","sink_capacity","sink_available",
                           "k_saturation_utility"] with initial availability=capacity.
        """

        demand = ( 
            chains
            .filter(pl.col("motive_seq_id") != 0)
            .group_by(["motive"])
            .agg(pl.col("duration").sum())
        )
        
        motive_names = chains.schema["motive"].categories
        
        # Load and adjust sinks
        sinks = (
            
            pl.concat(
                [
                    (
                        motive
                        .get_opportunities(transport_zones)
                        .with_columns(
                            motive=pl.lit(motive.name),
                            sink_saturation_coeff=pl.lit(motive.sink_saturation_coeff)
                        )
                    )
                    for motive in motives if motive.has_opportunities is True
                ]
            )
            
            .filter(pl.col("n_opp") > 0.0)

            .with_columns(
                motive=pl.col("motive").cast(pl.Enum(motive_names)),
                to=pl.col("to").cast(pl.Int32)
            )
            .join(demand, on="motive")
            .with_columns(
                sink_capacity=( 
                    pl.col("n_opp")/pl.col("n_opp").sum().over("motive")
                    * pl.col("duration")*pl.col("sink_saturation_coeff")
                ),
                k_saturation_utility=pl.lit(1.0, dtype=pl.Float64())
            )
            .with_columns(
                sink_available=pl.col("sink_capacity")
            )
            .select(["to", "motive", "sink_capacity", "sink_available", "k_saturation_utility"])
        )

        return sinks
    
    
    def get_current_costs(self, costs, congestion):
        """Fetch current OD costs and cast endpoint IDs.
    
        Args:
            costs: TravelCostsAggregator-like provider with `.get(congestion=...)`.
            congestion (bool): Whether to use congested costs.
    
        Returns:
            pl.DataFrame: At least ["from","to","cost"], with Int32 endpoints.
        """

        current_costs = (
            costs.get(congestion=congestion)
            .with_columns([
                pl.col("from").cast(pl.Int32()),
                pl.col("to").cast(pl.Int32())
            ])
        )

        return current_costs