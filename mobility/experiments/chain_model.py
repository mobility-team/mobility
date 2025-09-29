import polars as pl
import numpy as np
import shutil
import os

shutil.rmtree("d:/data/mobility/chains")
shutil.rmtree("d:/data/mobility/flows")

os.mkdir("d:/data/mobility/chains")
os.mkdir("d:/data/mobility/flows")

n_samples = 15
random_switch_rate = 1.0/(1.0+4.0*np.arange(1, 16))
delta_cost_change = -0.0
alpha = 0.5

selection_lambda = {
    "work": 0.99986,
    "shopping": 0.99986,
    "other": 0.99986,
    "leisure": 0.99986,
    "home": 0.99986
}


base_costs = pl.read_parquet("d:/data/mobility/costs.parquet")

def offset_costs(costs, delta, prob):
    return (
        costs
        .with_columns([
            (pl.col("cost") + delta).alias("cost"),
            pl.lit(prob).alias("prob")
        ])
    )






sequences = pl.read_parquet("d:/data/mobility/sequences.parquet")




pop_groups = (
    pl.read_parquet("d:/data/mobility/pop_groups.parquet")
    .with_columns(
        city_category=pl.col("city_category").cast(pl.Enum(["C", "B", "R", "I"])),
        csp=pl.col("csp").cast(pl.Enum(["1", "2", "3", "4", "5", "6", "7", "8", "no_csp"])),
        n_cars=pl.col("n_cars").cast(pl.Enum(["0", "1", "2+"]))
    )
)


pop_sequences = (
    
    pop_groups
    .join(sequences, on=["city_category", "csp", "n_cars"])
    .filter(pl.col("is_weekday") == True)
    .drop("is_weekday")
    .with_columns(n_subseq=pl.col("weight")*pl.col("p_seq")) 

)




pop_sequences_agg = (
    
    pop_sequences
    .group_by(["transport_zone_id", "motive_subseq", "subseq_step_index", "motive"])
    .agg(
        n_subseq=pl.col("n_subseq").sum(),
        duration=(
            (
                pl.col("duration_morning")
                + pl.col("duration_midday")
                + pl.col("duration_evening")
            )
            * pl.col("n_subseq")
        ).sum()
    )
    .with_columns(
        duration_per_subseq=pl.col("duration")/pl.col("n_subseq")
    )
 
)

next_pop_sequences_agg = pop_sequences_agg.clone()

    

demand = ( 
    
    pop_sequences_agg
    .group_by(["motive"])
    .agg(pl.col("duration").sum())
    
)

# Load and adjust sinks
base_sinks = (
    pl.read_parquet("d:/data/mobility/sinks.parquet")
    .with_columns([
        pl.col("sink_volume").alias("work"),
        pl.col("sink_volume").alias("shopping"),
        pl.col("sink_volume").alias("leisure"),
        pl.col("sink_volume").alias("other")
    ])
    .select(["to", "work", "shopping", "leisure", "other"])
    .unpivot(index=["to"], variable_name="motive", value_name="n_opp")
    .with_columns(
        motive=pl.col("motive").cast(pl.Enum(["work", "shopping", "leisure", "other", "home"]))
    )
    .join(demand, on="motive")
    .with_columns(
        sink_duration=pl.col("n_opp")/pl.col("n_opp").sum().over("motive")*pl.col("duration")
    )
    .select(["to", "motive", "sink_duration"])
)


next_sinks = base_sinks.clone()








previous_seq_flows = None





for i in range(0, n_samples):
    
    print("------------------------------------------------------------------")
    print(f"Tracing chains for sample n°{i}...")
        
    # Update costs
    print("Computing costs to destinations for each motive and OD...")
    
    rsr = random_switch_rate[i]
    
    costs = pl.concat([
        offset_costs(base_costs, -2.0, 0.07),
        offset_costs(base_costs, -1.0, 0.24),
        offset_costs(base_costs, +0.0, 0.38),
        offset_costs(base_costs, +1.0, 0.24),
        offset_costs(base_costs, +2.0, 0.07)
    ])

    costs = (
        costs.lazy()
        .join(next_sinks.lazy(), on="to")
        .with_columns(n_opp=pl.col("sink_duration")*pl.col("prob"))
        .drop("prob")
        .with_columns(cost_bin=pl.col("cost").round().cast(pl.Int32()))
    )

    cost_bin_to_dest = (
        costs
        .with_columns(p_to=pl.col("sink_duration")/pl.col("sink_duration").sum().over(["from", "motive", "cost_bin"]))
        .select(["motive", "from", "cost_bin", "to", "p_to"])
        .collect()
    )

    costs_bin = (
        costs
        .group_by(["from", "motive", "cost_bin"])
        .agg(pl.col("sink_duration").sum())
        .sort(["from", "motive", "cost_bin"])
    )
    
    # Compute the probability of choosing a destination, given a trip motive, an 
    # origin and the costs to get to destinations
    print("Computing the probability of choosing a destination based on current location, potential destinations, and motive (with radiation models)...")
    
    p_ij = (
            
        # Apply the radiation model for each motive and origin
        costs_bin
        .with_columns(
            s_ij=pl.col("sink_duration").cum_sum().over(["from", "motive"]),
            selection_lambda=pl.col("motive").replace_strict(selection_lambda)
        )
        .with_columns(
            p_a = (1 - pl.col("selection_lambda")**(1+pl.col('s_ij'))) / (1+pl.col('s_ij')) / (1-pl.col("selection_lambda"))
        )
        .with_columns(
            p_a_lag=( 
                pl.col('p_a')
                .shift(fill_value=1.0)
                .over(["from", "motive"])
                .alias('p_a_lag')
            )
        )
        .with_columns(
            p_ij=pl.col('p_a_lag') - pl.col('p_a')
        )
        .with_columns(
            p_ij=pl.col('p_ij') / pl.col('p_ij').sum().over(["from", "motive"])
        )
        .filter(pl.col("p_ij") > 0.0)
        
        # Keep only the first 99 % of the distribution
        .sort("p_ij", descending=True)
        .with_columns(
            p_ij_cum=pl.col("p_ij").cum_sum().over(["from", "motive"]),
            p_count=pl.col("p_ij").cum_count().over(["from", "motive"])
        )
        .filter((pl.col("p_ij_cum") < 0.99) | (pl.col("p_count") == 1))
        .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["from", "motive"]))
        
        # Disaggregate bins -> destinations
        .join(cost_bin_to_dest.lazy(), on=["motive", "from", "cost_bin"])
        .with_columns(p_ij=pl.col("p_ij")*pl.col("p_to"))
        .group_by(["motive", "from", "to"])
        .agg(pl.col("p_ij").sum())
        
        # Keep only the first 99 % of the distribution
        # (or the destination that has a 100% probability, which can happen)
        .sort("p_ij", descending=True)
        .with_columns(
            p_ij_cum=pl.col("p_ij").cum_sum().over(["from", "motive"]),
            p_count=pl.col("p_ij").cum_count().over(["from", "motive"])
        )
        .filter((pl.col("p_ij_cum") < 0.99) | (pl.col("p_count") == 1))
        .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["from", "motive"]))
        
        .select(["motive", "from", "to", "p_ij"])
        
        .collect(engine="streaming")
    )
        
    next_step = ( 
        next_pop_sequences_agg
        .filter(pl.col("subseq_step_index") == 1)
        .with_columns(pl.col("transport_zone_id").alias("from"))
        .rename({"transport_zone_id": "home_zone_id"})
    )
    
    n_subseq = next_pop_sequences_agg["n_subseq"].sum()
    print(f"Number of subsequences in the system : {n_subseq}")
    
    
    subseq_step_index = 1
    chains = []
    
    while next_step.height > 0:
        
        print(f"Estimating flows sequence step n°{subseq_step_index}...")
        
        # Blend the probability to choose a destination based on current location
        # with the probability to choose this destination from home
        # When alpha = 0, people ignore where they live when they choose a destination (except for the first trip)
        # When alpha = 1, people ignore where they currently and only consider where they live when they choose a destination
        # When 0 < alpha < 1, people take the two into account
        print("Adjusting probabilities to account for home location...")

        trips = (
            
            next_step.lazy()
            .filter(pl.col("motive") != "home")
            .select(["home_zone_id", "motive_subseq", "motive", "from"])
            
            .join(
                p_ij.lazy(),
                on=["motive", "from"]
            )
            
            .join(
                p_ij.lazy()
                .select(["motive", "from", "to", "p_ij"])
                .rename({"from": "home_zone_id", "p_ij": "p_ij_home"}),
                on=["home_zone_id", "motive", "to"],
                how="left"
            )
            
            # Some low probability destinations have no probability because
            # of the 99 % cutoff applied when computing p_ij, so we set them to zero
            .with_columns(
                p_ij_home=pl.col("p_ij_home").fill_null(0.0)
            )
            
            .with_columns(
                p_ij=( 
                    pl.when(pl.col("home_zone_id") == pl.col("from"))
                    .then(pl.col("p_ij"))
                    .otherwise(pl.col("p_ij").pow(1-alpha)*pl.col("p_ij_home").pow(alpha))
                )
            )
            .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["motive_subseq", "motive", "home_zone_id", "from"]))
            
            # Keep only the first 99 % of the distribution
            .sort("p_ij", descending=True)
            .with_columns(
                p_ij_cum=pl.col("p_ij").cum_sum().over(["motive_subseq", "home_zone_id", "from", "motive"]),
                p_ij_count=pl.col("p_ij").cum_count().over(["motive_subseq", "home_zone_id", "from", "motive"])
            )
            .filter((pl.col("p_ij_cum") < 0.99) | (pl.col("p_ij_count") == 1))
            .with_columns(
                p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["motive_subseq", "home_zone_id", "from", "motive"])
            )
            
            .collect(engine="streaming")
            
        )
        
        
        # Use the exponential sort trick to sample destinations based on their probabilities
        # (because polars cannot do weighted sampling like pandas)
        # https://timvieira.github.io/blog/post/2019/09/16/algorithms-for-sampling-without-replacement/
        
        noise = -np.log(np.random.rand(trips.height))
        
        trips = (
            
            trips.lazy()
            .with_columns(pl.Series("noise", noise))
            .with_columns(
                sample_score=pl.col("noise")/pl.col("p_ij")
            )
            
            .sort(["sample_score"])
            .group_by(["home_zone_id", "motive_subseq", "motive", "from"])
            .head(1)
            
            .select(["home_zone_id", "motive_subseq", "motive", "from", "to", "p_ij"])
            .with_columns(subseq_step_index=pl.lit(subseq_step_index).cast(pl.UInt32))
            .with_columns(i=pl.lit(i).cast(pl.UInt32))
            
            .collect(engine="streaming")
            
        )
        
        # Add the back to home step
        trips_home = (
            next_step
            .filter(pl.col("motive") == "home")
            .with_columns(
                i=pl.lit(i).cast(pl.UInt32),
                p_ij=1.0,
                to=pl.col("home_zone_id")
            )
            .select(["home_zone_id", "motive_subseq", "motive", "from", "to", "p_ij", "subseq_step_index", "i"])
        )
        
        trips = pl.concat([trips, trips_home])
        
        chains.append(trips)
        
        
        # Create the next steps in the chains, using the latest locations as 
        # origins for the next trip
        subseq_step_index += 1
        
        next_step = ( 
            next_pop_sequences_agg
            .filter(pl.col("subseq_step_index") == subseq_step_index)
            .rename({"transport_zone_id": "home_zone_id"})
            .join(
                (
                    trips
                    .select(["home_zone_id", "motive_subseq", "to"])
                    .rename({"to": "from"})
                ),
                on=["home_zone_id", "motive_subseq"]
            )
        )
        
    
    chains = pl.concat(chains)
    chains.write_parquet(f"d:/data/mobility/chains/chains_{i}.parquet")

    # Overflows
    
    print("Computing the number of persons on each possible chain...")
    
    # Compute the probability of each sequence
    p_seq = (
        
        pl.scan_parquet("d:/data/mobility/chains/")
        .filter(pl.col("subseq_step_index") == 1)
        .with_columns(
            p_seq=pl.col("p_ij").log().sum().over(["home_zone_id", "motive_subseq"]).exp()
        )
        .with_columns(
            p_seq=pl.col('p_seq') / pl.col('p_seq').sum().over(["home_zone_id", "motive_subseq"])
        )
        .select(["home_zone_id", "motive_subseq", "i", "p_seq"])
        
    )
    
    seq_flows = (
        
        pl.scan_parquet("d:/data/mobility/chains/")
        .join(p_seq, on=["home_zone_id", "motive_subseq", "i"])
        
        # Compute the number of persons at each destination, for each motive
        .join(
            next_pop_sequences_agg.rename({"transport_zone_id": "home_zone_id"}).lazy(),
            on=["home_zone_id", "motive_subseq", "motive", "subseq_step_index"]
        )
        .with_columns(
            n_subseq=pl.col("n_subseq")*pl.col("p_seq"),
            duration=pl.col("duration")*pl.col("p_seq")
        )
        .select([
            'home_zone_id', 'motive_subseq', 'motive', 'from', 'to',
            'subseq_step_index', 'i', 'n_subseq', 'duration', "duration_per_subseq"
        ])
        
        .collect(engine="streaming")
        
    )
    
    
    if previous_seq_flows is not None:
        
        print("Combining with the flows from the previous steps...")
        
        seq_flows = pl.concat([seq_flows, previous_seq_flows])
        seq_flows = (
            seq_flows
            .group_by(["home_zone_id", "motive_subseq", "motive", "from", "to", "subseq_step_index", "i"])
            .agg(
                n_subseq=pl.col("n_subseq").sum(),
                duration=pl.col("duration").sum()
            )
            .with_columns(
                duration_per_subseq=pl.col("duration")/pl.col("n_subseq")
            )
        )
    
    
    n_subseq = seq_flows["n_subseq"].sum()
    print(f"Total number of subsequences in the system: {n_subseq}")
        
    
    seq_flows.write_parquet(f"d:/data/mobility/flows/flows_{i}.parquet")

    
    # Compute the share of persons in each OD flow that could not find an
    # opportunity because too many people chose the same destiation
    # p_overflow_motive = 1.0 - duration/available duration
    
    # A given chain is "overflowing" opportunities at destination if 
    # at soon as one of the destinations is "overflowing", so :
    # p_overflow = max(p_overflow_motive)
    print("Correcting flows for sink saturation...")
    
    seq_flows_overflow = (
        
        seq_flows
        
        .join(base_sinks, on=["motive", "to"], how="left")
        .with_columns(
            p_overflow=(
                pl.when(pl.col("motive") == "home")
                .then(0.0)
                .otherwise((1.0 - 1.0/(pl.col("duration").sum().over(["to", "motive"])/pl.col("sink_duration"))).clip(0.0, 1.0))
            )
        )
        .with_columns(
            p_overflow_max=pl.col("p_overflow").max().over(["home_zone_id", "motive_subseq", "i"])
        )
        .with_columns(
            overflow=pl.col("n_subseq")*pl.col("p_overflow_max")
        )
        .with_columns(
            n_subseq=pl.col("n_subseq") - pl.col("overflow")
        )
        
    )
    
    seq_flows_overflow.filter(pl.col("n_subseq") < 0.0)
    
    n_subseq = seq_flows_overflow["n_subseq"].sum()
    overflow = seq_flows_overflow["overflow"].sum()
    print(f"Total number of subsequences in the system: {n_subseq+overflow}")
    
    
    # Compute the number of persons switching destinations after comparing their 
    # utility to the average utility of persons living in the same place
    # (adding X € to the no switch decision to account for transition costs)
    print("Correcting flows for persons optimizing their cost...")
    
    p_seq_change = (
        
        seq_flows_overflow
    
        .join(base_costs, on=["from", "to"])
        .group_by(["home_zone_id", "motive_subseq", "i"])
        .agg(
            cost=pl.col("cost").sum(),
            n_subseq=pl.col("n_subseq").first()
        )
        .with_columns(
            average_cost=(
                (pl.col("cost")*pl.col("n_subseq"))
                .sum().over(["home_zone_id", "motive_subseq"])
                /
                pl.col("n_subseq")
                .sum().over(["home_zone_id", "motive_subseq"])
            )
        )
        .with_columns(
            delta_cost=pl.col("average_cost") - (pl.col("cost") + delta_cost_change)
        )
        .with_columns(
            p_seq_change=(
                pl.when(pl.col("delta_cost").abs() > 10.0)
                .then(pl.when(pl.col("delta_cost") > 0.0).then(0.0).otherwise(1.0))
                .otherwise(1.0/(1.0+pl.col("delta_cost").exp()))
            )
        )
    
        .select(["home_zone_id", "motive_subseq", "i", "p_seq_change"])
        
    )
    
    seq_flows_change = (
    
        seq_flows_overflow
        .join(p_seq_change, on=["home_zone_id", "motive_subseq", "i"])
        .with_columns(
            change=pl.col("n_subseq")*pl.col("p_seq_change")
        )
        .with_columns(
            n_subseq=pl.col("n_subseq") - pl.col("change")
        )
        
    )
    
    n_subseq = seq_flows_change["n_subseq"].sum()
    change = seq_flows_change["change"].sum()
    print(f"Total number of subsequences in the system: {n_subseq+change+overflow}")
    
    print("Correcting flows for persons changing randomly...")
        
    # Compute the share of persons switching destinations for random reasons
    seq_flows_rand_switch = (
        
        seq_flows_change
        .with_columns(
            random_switch=pl.col("n_subseq")*rsr
        )
        .with_columns(
            n_subseq=pl.col("n_subseq") - pl.col("random_switch"),
            delta_n_subseq=pl.col("overflow") + pl.col("change") + pl.col("random_switch")
        )
        .with_columns(
            duration=pl.col("n_subseq")*pl.col("duration_per_subseq"),
            delta_duration=pl.col("delta_n_subseq")*pl.col("duration_per_subseq")
        )
        
    )
    
    
    n_subseq = seq_flows_rand_switch["n_subseq"].sum()
    delta_n_subseq = seq_flows_rand_switch["delta_n_subseq"].sum()
    print(f"Total number of persons in the system: {n_subseq+delta_n_subseq}")
    
    
    previous_seq_flows = (
        seq_flows_rand_switch
        .select(
            [
                'home_zone_id', 'motive_subseq', 'motive', 'from', 'to',
                'subseq_step_index', 'i', 'n_subseq', "duration", "duration_per_subseq"
            ]
        )
    )
        
    # Modified opportunities
    print("Computing remaining opportunities counts at destination...")
    
    next_sinks = (
    
        seq_flows_rand_switch
        .group_by(["to", "motive"])
        .agg(pl.col("duration").sum())
        .join(base_sinks, on=["to", "motive"], how="full", coalesce=True)
        .with_columns(
            sink_duration=( 
                pl.col("sink_duration").fill_null(0.0).fill_nan(0.0)
                -
                pl.col("duration").fill_null(0.0).fill_nan(0.0)
            )
        )
        .select(["motive", "to", "sink_duration"])
        .filter(pl.col("sink_duration") > 0.0)
        
    )
    
    next_pop_sequences_agg = (
        seq_flows_rand_switch
        .group_by(["home_zone_id", "motive_subseq", "motive", "subseq_step_index"])
        .agg(
            n_subseq=pl.col("delta_n_subseq").sum(),
            duration=pl.col("delta_duration").sum()
        )
        .with_columns(
            duration_per_subseq=pl.col("duration")/pl.col("n_subseq")
        )
        .rename({"home_zone_id": "transport_zone_id"})
    )

    
    # stop


flows = []

for k in range(n_samples):
    
    flows.append(
        (
            pl.scan_parquet(f"d:/data/mobility/flows/flows_{k}.parquet")
            .group_by(["from", "to"])
            .agg(pl.col("n_subseq").sum())
            .with_columns(k=k)
            .collect(engine="streaming")
        )
    )
    
flows = (
    pl.concat(flows)
    .with_columns(w_lag=pl.col("n_subseq").shift().over(["from", "to"]))
    .with_columns(delta=pl.col("n_subseq")-pl.col("w_lag"))
    .with_columns(delta_rel=pl.col("delta")/pl.col("n_subseq"))
)



# p = flows.to_pandas()

f  = (
     flows
     .group_by(["k"])
     .agg(
         n_subseq=pl.col("n_subseq").sum(),
         delta=pl.col("delta").abs().sum()
    )
)

f.to_pandas().plot("k", "delta")

f = flows.to_pandas()


flows.filter(pl.col("from") == 471).filter(pl.col("to") == 479)
flows.filter(pl.col("from") == 479).filter(pl.col("to") == 479)
flows.filter(pl.col("from") == 309).filter(pl.col("to") == 113)
flows.filter(pl.col("from") == 115).filter(pl.col("to") == 309)
flows.filter(pl.col("from") == 276).filter(pl.col("to") == 321)
flows.filter(pl.col("from") == 355).filter(pl.col("to") == 470)
flows.filter(pl.col("from") == 461).filter(pl.col("to") == 515)
flows.filter(pl.col("n_subseq") < 0.0)

flows.filter(pl.col("k") == 14).sort(pl.col("delta_rel")).tail(20)

flows.filter(pl.col("n_subseq") > 10.0).filter(pl.col("k") == 12)["delta_rel"].hist()
flows.filter(pl.col("n_subseq") > 10.0).filter(pl.col("k") == 10)["n_subseq"].sum()/flows.filter(pl.col("k") == 14)["n_subseq"].sum()


f = (
     
     pl.scan_parquet(f"d:/data/mobility/flows/flows_14.parquet")
     .filter(pl.col("home_zone_id") == 485)
     .filter(pl.col("motive_subseq") == "work-shopping-home")
     .collect(engine="streaming")
     .to_pandas()
     
)