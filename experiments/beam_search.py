import polars as pl
import numpy as np
import pandas as pd

# Seed for reproducibility
np.random.seed(42)

# Define zones and activity types
n_zones = 100
zones = [i for i in range(n_zones)]

# Opportunities per zone for each activity type
motives = ["work", "shopping", "restaurant", "leisure"]
opportunities = pl.DataFrame({"zone": zones})
for m in motives:
    opportunities = opportunities.with_columns(pl.Series(m, np.random.randint(0, 100, size=n_zones)))

opportunities = opportunities.unpivot(
    index="zone",
    variable_name="motive",
    value_name="opps"
)

# Travel cost dataframe
cost_matrix = np.random.uniform(1, 20, size=(n_zones, n_zones))

costs = pl.DataFrame({
    "from_zone": np.repeat(zones, len(zones)),
    "to_zone": zones * len(zones),
    "cost": cost_matrix.flatten()
})

selection_lambda = {
    "work": 0.99986,
    "shopping": 0.99,
    "restaurant": 0.99984,
    "leisure": 0.99985
}

n_chains = 10000
chains = pd.DataFrame({
    "chain_id": np.arange(n_chains),
    "home_zone": np.random.randint(0, n_zones, size=n_chains)
})
chains["motive"] = [
    np.random.choice(
        motives,
        size=np.random.randint(1, len(motives)+1),
        replace=False
    ).tolist() for _ in chains.index
]
chains = chains.explode("motive").reset_index(drop=True)
chains["trip_id"] = chains.groupby("chain_id", as_index=False).cumcount()
chains = pl.from_pandas(chains)

chain_seqs = chains.group_by("chain_id").agg(pl.col("motive").str.join("-").alias("motive_seq"))

trip_id = 0

origins = (
    chains
    .filter(pl.col("trip_id") == trip_id)
    .select(["chain_id", "home_zone", "trip_id", "motive"])
    .rename({"home_zone": "from_zone"})
)

trips = []

print("start localizing chains")

while origins.height > 0:
    
    print(trip_id)
    
    # Compute the probabilities of selecting a destination given the generalized
    # cost to get to it and the number of opportunities at destination, 
    # following the radiation model with selection formula
    beam = (
        origins
        .join(costs, on="from_zone")
        .join(opportunities, left_on=["motive", "to_zone"], right_on=["motive", "zone"])
        .sort(["chain_id", "cost"])
        .with_columns(
            s_ij=pl.col("opps").cum_sum().over("chain_id"),
            selection_lambda=pl.col("motive").replace_strict(selection_lambda)
        )
        .with_columns(
            p_a = (1 - pl.col("selection_lambda")**(1+pl.col('s_ij'))) / (1+pl.col('s_ij')) / (1-pl.col("selection_lambda"))
        )
        .with_columns(
            p_a_lag=( 
                pl.col('p_a')
                .shift(fill_value=1.0)
                .over(["chain_id"])
                .alias('p_a_lag')
            )
        )
        .with_columns(
            p_ij=pl.col('p_a_lag') - pl.col('p_a')
        )
        .with_columns(
            p_ij=pl.col('p_ij') / pl.col('p_ij').sum().over('chain_id')
        )
        .filter(pl.col("p_ij") > 0.0)
    )
    
    # Use the exponential sort trick to sample destinations based on their probabilities
    # (because polars cannot do weighted sampling like pandas)
    # https://timvieira.github.io/blog/post/2019/09/16/algorithms-for-sampling-without-replacement/
    noise = -np.log(np.random.uniform(size=beam.height))
    
    beam = (
        beam
        .with_columns([
            pl.Series("noise", noise)
        ])
        .with_columns(
            sample_score=pl.col("noise")/pl.col("p_ij")
        )
        .sort(["sample_score"])
        .group_by("chain_id")
        .head(1)
        .with_columns(
            trip_id=pl.lit(trip_id)
        )
        .select(["chain_id", "trip_id", "from_zone", "to_zone"]) 
    )
    
    trips.append(beam)
    
    # Prepare the next origins (= the destinations of the trips)
    trip_id += 1

    origins = (
        chains
        .filter(pl.col("trip_id") == trip_id)
        .select(["chain_id", "trip_id", "motive"])
        .join(
            beam
            .select(["chain_id", "to_zone"])
            .rename({"to_zone": "from_zone"}),
            on=["chain_id"]
        )
    )


trips = ( 
    pl.concat(trips, how="align")
    .sort(["chain_id", "trip_id"])
    .join(
        chains,
        on=["chain_id", "trip_id"]
    )
    .join(costs, on=["from_zone", "to_zone"])
)

work_zone = ( 
    trips
    .filter(pl.col("motive") == "work")
    .select(["chain_id", "to_zone"])
    .rename({"to_zone": "work_zone"})
)

chain_probs = ( 
    trips
    .group_by(["home_zone", "chain_id"])
    .agg(pl.col("cost").sum())
    .join(chain_seqs, on="chain_id")
    .with_columns(
        p_chain=(
            pl.col("cost").neg().exp()
            /
            pl.col("cost").neg().exp().sum().over(["home_zone", "motive_seq"])
        ),
        p_seq=(
            pl.col("cost").count().over(["home_zone", "motive_seq"])
            /
            pl.col("cost").count().over(["home_zone"])
        )
    )
    .with_columns(
        p=pl.col("p_chain")*pl.col("p_seq")
    )
)



x = chain_probs.filter(pl.col("home_zone") == 0).to_pandas()

