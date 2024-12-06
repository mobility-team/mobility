import polars as pl

class WorkUtilities:
    
    def __init__(self, transport_zones, sinks, utility_by_country):
        
        transport_zones = pl.DataFrame(
            transport_zones[["transport_zone_id", "country"]].rename({"transport_zone_id": "to"}, axis=1)
        ).with_columns(
            pl.col("to").cast(pl.Int64)
        )
        
        utility_by_country = pl.DataFrame({
            "country": utility_by_country.keys(),
            "base_utility": utility_by_country.values()
        })
        
        sinks = pl.DataFrame(sinks.reset_index()).with_columns([
            pl.col("to").cast(pl.Int64)
        ])
        
        utilities = (
            sinks
            .join(transport_zones, on="to")
            .join(utility_by_country, on="country")
            .with_columns([pl.col("base_utility").alias("utility")])
            .select(["to", "base_utility", "utility"])
        )
        
        self.sinks = sinks        
        self.utilities = utilities
        
        
    def get(self, congestion: bool = False):
        if congestion is True:
            return self.utilities.select(["to", "utility"])
        else:
            return self.utilities.with_columns([pl.col("base_utility").alias("utility")]).select(["to", "utility"])
        
        
    def update(self, flows):
        
        sink_occupation = flows.group_by("to").agg(
            pl.col("flow_volume").sum().alias("sink_occupation")
        )

        sink_occupation = self.sinks.join(
            sink_occupation,
            on="to",
            how="left",
            coalesce=True
        ).with_columns(
            pl.col("sink_occupation").fill_null(0.0)
        )
        
        # sink_occupation = sink_occupation.with_columns([
        #     pl.when(
        #         pl.col("sink_occupation")/pl.col("sink_volume") > 1.0
        #     ).then(
        #         pl.col("sink_volume")/pl.col("sink_occupation")
        #     ).otherwise(
        #         1.0
        #     ).alias("k_utility")
        # ])   
                
        # sink_occupation = sink_occupation.with_columns([
        #     (1.0/(1.0 + 0.5*(pl.col("sink_occupation")/pl.col("sink_volume")).pow(8))).alias("k_utility")
        # ])  
        
        self.utilities = ( 
            self.utilities
            .join(sink_occupation.select(["to", "k_utility"]), on="to")
            .with_columns([(pl.col("base_utility") * pl.col("k_utility")).alias("utility")])
            .drop("k_utility")
        )
