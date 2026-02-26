import logging

import polars as pl

from scipy.stats import norm

from mobility.choice_models.add_index import add_index

class DestinationSequenceSampler:
    """Samples destination sequences for trip chains.
    
    Orchestrates: (1) utility assembly with uncertain costs, (2) radiation-based
    destination probabilities, and (3) spatialization of anchor and non-anchor
    steps into per-iteration destination sequences.
    """
    
    def run(
            self,
            motives,
            transport_zones,
            remaining_sinks,
            iteration,
            chains,
            demand_groups,
            costs,
            tmp_folders,
            parameters,
            seed
        ):
        
        """Compute destination sequences for one iteration.

        Builds utilities with cost uncertainty, derives destination probabilities,
        spatializes anchor motives, then sequentially samples non-anchor steps.
        Returns the per-step chains with a `dest_seq_id` and the iteration tag.
        
        Args:
            motives: Iterable of Motive objects.
            transport_zones: Transport zone container used by motives.
            remaining_sinks (pl.DataFrame): Current availability per (motive, to).
            iteration (int): Iteration index (>=1).
            chains (pl.DataFrame): Chain steps with
                ["demand_group_id","motive_seq_id","motive","is_anchor","seq_step_index"].
            demand_groups (pl.DataFrame): ["demand_group_id","home_zone_id"] (merged for origins).
            costs (pl.DataFrame): OD costs with ["from","to","cost"].
            tmp_folders (dict[str, pathlib.Path]): Must include "sequences-index".
            parameters: Model parameters (alpha, dest_prob_cutoff, cost_uncertainty_sd, …).
            seed (int): 64-bit seed for reproducible exponential races.
        
        Returns:
            pl.DataFrame: Spatialized chains with columns including
                ["demand_group_id","motive_seq_id","dest_seq_id","seq_step_index","from","to","iteration"].
        """
        
        utilities = self.get_utilities(
            motives,
            transport_zones,
            remaining_sinks,
            costs,
            parameters.cost_uncertainty_sd
        )
        
        dest_prob = self.get_destination_probability(
            utilities,
            motives,
            parameters.dest_prob_cutoff
        )

        chains = (
            chains
            .filter(pl.col("motive_seq_id") != 0)
            .join(demand_groups.select(["demand_group_id", "home_zone_id"]), on="demand_group_id")
            .select(["demand_group_id", "home_zone_id", "motive_seq_id", "motive", "is_anchor", "seq_step_index"])
        )
        
        chains = self.spatialize_anchor_motives(chains, dest_prob, seed)
        chains = self.spatialize_other_motives(chains, dest_prob, costs, parameters.alpha, seed)
        
        dest_sequences = ( 
            chains
            .group_by(["demand_group_id", "motive_seq_id"])
            .agg(
                to=pl.col("to").sort_by("seq_step_index").cast(pl.Utf8())
            )
            .with_columns(
                to=pl.col("to").list.join("-")
            )
            .sort(["demand_group_id", "motive_seq_id"])
        )
        
        dest_sequences = add_index(
            dest_sequences,
            col="to",
            index_col_name="dest_seq_id",
            tmp_folders=tmp_folders
        )
        
        chains = (
            chains
            .join(
                dest_sequences.select(["demand_group_id", "motive_seq_id", "dest_seq_id"]),
                on=["demand_group_id", "motive_seq_id"]
            )
            .drop(["home_zone_id", "motive"])
            .with_columns(iteration=pl.lit(iteration).cast(pl.UInt32))
        )
        
        return chains

        
    def get_utilities(self, motives, transport_zones, sinks, costs, cost_uncertainty_sd):
        
        """Assemble per-(from,to,motive) utility with cost uncertainty.
        
        Gathers motive utilities, joins sink availability, and expands costs with a
        small discrete Gaussian around 0 to model uncertainty. Buckets
        (cost - utility) into integer `cost_bin`s and returns:
        (1) aggregated availability by bin, and (2) bin→destination disaggregation.
        
        Args:
            motives: Iterable of Motive objects exposing `get_utilities(transport_zones)`.
            transport_zones: Zone container passed to motives.
            sinks (pl.DataFrame): ["to","motive","sink_available", …].
            costs (pl.DataFrame): ["from","to","cost"].
            cost_uncertainty_sd (float): Std-dev for the discrete Gaussian over deltas.
        
        Returns:
            tuple[pl.LazyFrame, pl.LazyFrame]:
                - costs_bin: ["from","motive","cost_bin","sink_available"].
                - cost_bin_to_dest: ["motive","from","cost_bin","to","p_to"].
        """
        
        utilities = [(m.name, m.get_utilities(transport_zones)) for m in motives]
        utilities = [u for u in utilities if u[1] is not None]
        utilities = [u[1].with_columns(motive=pl.lit(u[0])) for u in utilities]
        
        motive_values = sinks.schema["motive"].categories
        
        utilities = (
            
            pl.concat(utilities)
            .with_columns(
                motive=pl.col("motive").cast(pl.Enum(motive_values))
            )

        )
        
        def offset_costs(costs, delta, prob):
            return (
                costs
                .with_columns([
                    (pl.col("cost") + delta).alias("cost"),
                    pl.lit(prob).alias("prob")
                ])
            )
        
        x = [-2.0, -1.0, 0.0, 1.0, 2.0]
        p = norm.pdf(x, loc=0.0, scale=cost_uncertainty_sd)
        p /= p.sum()
        
        costs = pl.concat([offset_costs(costs, x[i], p[i]) for i in range(len(p))])

        costs = (
            costs.lazy()
            .join(
                ( 
                    sinks
                    .filter(pl.col("sink_available") > 0.0)
                    .lazy()
                ),
                on="to"
            )
            .join(utilities.lazy(), on=["motive", "to"], how="left")
            .with_columns(
                utility=pl.col("utility").fill_null(0.0),
                sink_available=pl.col("sink_available")*pl.col("prob")
            )
            .drop("prob")
            .with_columns(
                cost_bin=(pl.col("cost") - pl.col("utility")).floor()
            )
        )

        cost_bin_to_dest = (
            costs
            .with_columns(p_to=pl.col("sink_available")/pl.col("sink_available").sum().over(["from", "motive", "cost_bin"]))
            .select(["motive", "from", "cost_bin", "to", "p_to"])
        )

        costs_bin = (
            costs
            .group_by(["from", "motive", "cost_bin"])
            .agg(pl.col("sink_available").sum())
            .sort(["from", "motive", "cost_bin"])
        )
        
        return costs_bin, cost_bin_to_dest
    
   
    def get_destination_probability(self, utilities, motives, dest_prob_cutoff):
        
        """Compute P(destination | from, motive) via a radiation-style model.

          Applies a cumulative-opportunity radiation formulation per (from, motive),
          trims the tail to `dest_prob_cutoff`, and expands cost bins back to
          destinations using `p_to`.
        
          Args:
              utilities (tuple): Output of `get_utilities` → (costs_bin, cost_bin_to_dest).
              motives: Iterable of motives (uses `radiation_lambda` per motive).
              dest_prob_cutoff (float): Keep top cumulative probability mass (e.g., 0.99).
        
          Returns:
              pl.DataFrame: ["motive","from","to","p_ij"] normalized per (from, motive).
          """
        
        # Compute the probability of choosing a destination, given a trip motive, an 
        # origin and the costs to get to destinations
        logging.info("Computing the probability of choosing a destination based on current location, potential destinations, and motive (with radiation models)...")
        
        costs_bin = utilities[0]
        cost_bin_to_dest = utilities[1]

        motives_lambda = {motive.name: motive.radiation_lambda for motive in motives}
        
        prob = (
                
            # Apply the radiation model for each motive and origin
            costs_bin
            .with_columns(
                s_ij=pl.col("sink_available").cum_sum().over(["from", "motive"]),
                selection_lambda=pl.col("motive").replace_strict(motives_lambda)
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
            
            # Rounding to avoid floating point instability when sorting
            .with_columns(p_ij=pl.col("p_ij").round(9))
  
            # Keep only the first 99 % of the distribution
            .sort(["from", "motive", "p_ij", "cost_bin"], descending=[False, False, True, False])
            .with_columns(
                p_ij_cum=pl.col("p_ij").cum_sum().over(["from", "motive"]),
                p_count=pl.col("p_ij").cum_count().over(["from", "motive"])
            )
            .filter((pl.col("p_ij_cum") < dest_prob_cutoff) | (pl.col("p_count") == 1))
            .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["from", "motive"]))
            
            # Disaggregate bins -> destinations
            .join(cost_bin_to_dest, on=["motive", "from", "cost_bin"])
            .with_columns(p_ij=pl.col("p_ij")*pl.col("p_to"))
            .group_by(["motive", "from", "to"])
            .agg(pl.col("p_ij").sum())
            
            # Rounding to avoid floating point instability when sorting
            .with_columns(p_ij=pl.col("p_ij").round(9))
            
            # Keep only the first 99 % of the distribution
            # (or the destination that has a 100% probability, which can happen)
            .sort(["from", "motive", "p_ij", "to"], descending=[False, False, True, False])
            .with_columns(
                p_ij_cum=pl.col("p_ij").cum_sum().over(["from", "motive"]),
                p_count=pl.col("p_ij").cum_count().over(["from", "motive"])
            )
            .filter((pl.col("p_ij_cum") < dest_prob_cutoff) | (pl.col("p_count") == 1))
            .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["from", "motive"]))
            
            .select(["motive", "from", "to", "p_ij"])
            
            .collect(engine="streaming")
        )
        
        return prob  
        
        
    def spatialize_anchor_motives(self, chains, dest_prob, seed):
        """Samples destinations for anchor motives and fills `anchor_to`.
    
        Uses an exponential race (log(noise)/p_ij) per
        ["demand_group_id","motive_seq_id","motive"] to select one destination
        among candidates in `dest_prob`. 'home' anchors are fixed to
        `home_zone_id`, and `anchor_to` is backward-filled along the chain.
    
        Args:
            chains (pl.DataFrame): Chain steps with
                ["demand_group_id","home_zone_id","motive_seq_id","motive",
                 "is_anchor","seq_step_index"].
            dest_prob (pl.DataFrame): Destination probabilities with
                ["motive","from","to","p_ij"].
            seed (int): 64-bit RNG seed for reproducibility.
    
        Returns:
            pl.DataFrame: Same rows as `chains` with an added `anchor_to` column.
        """
        
        logging.info("Spatializing anchor motives...")
        
        spatialized_anchors = ( 
            
            chains
            .filter((pl.col("is_anchor")) & (pl.col("motive") != "home"))
            .select(["demand_group_id", "home_zone_id", "motive_seq_id", "motive"])
            .unique()
            
            .join(
                dest_prob,
                left_on=["home_zone_id", "motive"],
                right_on=["from", "motive"]
            )
            
            .with_columns(
                noise=( 
                    pl.struct(["demand_group_id", "motive_seq_id", "motive", "to"])
                    .hash(seed=seed)
                    .cast(pl.Float64) 
                    .truediv(pl.lit(18446744073709551616.0))
                    .log()
                    .neg()
                )
            )
            
            .with_columns(
                sample_score=( 
                    pl.col("noise")/pl.col("p_ij").clip(1e-18)  # Make sure p_ij is > 0
                    + pl.col("to").cast(pl.Float64)*1e-18           # Add a very small noise to beark ties
                )
            )
            
            .with_columns(
                min_score=( 
                    pl.col("sample_score").min()
                    .over(["demand_group_id", "motive_seq_id", "motive"])
                )
            )
            .filter(pl.col("sample_score") == pl.col("min_score"))
            
            .select(["demand_group_id", "motive_seq_id", "motive", "to"])
            
        )

        chains = (
            
            chains
            .join(
                spatialized_anchors.rename({"to": "anchor_to"}),
                on=["demand_group_id", "motive_seq_id", "motive"],
                how="left"
            )
            .with_columns(
                anchor_to=pl.when(
                    pl.col("motive") == "home"
                ).then(
                    pl.col("home_zone_id")
                ).otherwise(
                    pl.col("anchor_to")
                )
            )
            .sort(["demand_group_id", "motive_seq_id", "seq_step_index"])
            .with_columns(
                anchor_to=pl.col("anchor_to").backward_fill().over(["demand_group_id","motive_seq_id"])
            )
            
        ) 
                    
        return chains
    
    
    def spatialize_other_motives(self, chains, dest_prob, costs, alpha, seed):
        """Spatializes non-anchor motives sequentially between anchors.
    
        Iterates step by step from the home zone, sampling destinations
        based on `dest_prob` and a penalty toward the next anchor. At each
        iteration, the chosen `to` becomes the `from` for the following step,
        until all steps are spatialized.
    
        Args:
            chains (pl.DataFrame): Chains with `anchor_to` already set.
                Must include ["demand_group_id","home_zone_id","motive_seq_id",
                "motive","is_anchor","seq_step_index","anchor_to"].
            dest_prob (pl.DataFrame): Destination probabilities with
                ["motive","from","to","p_ij"].
            costs (pl.DataFrame): OD costs with ["from","to","cost"], used to
                discourage drifting away from anchors.
            alpha (float): Penalty coefficient applied to anchor distance.
            seed (int): 64-bit RNG seed for reproducibility.
    
        Returns:
            pl.DataFrame: Sampled chain steps with
                ["demand_group_id","home_zone_id","motive_seq_id","motive",
                 "anchor_to","from","to","seq_step_index"].
        """
        
        logging.info("Spatializing other motives...")
        
        chains_step = ( 
            chains
            .filter(pl.col("seq_step_index") == 1)
            .with_columns(pl.col("home_zone_id").alias("from"))
        )
        
        seq_step_index = 1
        spatialized_chains = []
        
        while chains_step.height > 0:
            
            logging.info(f"Spatializing step {seq_step_index}...")
            
            spatialized_step = ( 
                self.spatialize_trip_chains_step(seq_step_index, chains_step, dest_prob, costs, alpha, seed)
                .with_columns(
                    seq_step_index=pl.lit(seq_step_index).cast(pl.UInt32)
                )
            )
            
            spatialized_chains.append(spatialized_step)
            
            # Create the next steps in the chains, using the latest locations as 
            # origins for the next trip
            seq_step_index += 1
            
            chains_step = ( 
                chains
                .filter(pl.col("seq_step_index") == seq_step_index)
                .join(
                    (
                        spatialized_step
                        .select(["demand_group_id", "home_zone_id", "motive_seq_id", "to"])
                        .rename({"to": "from"})
                    ),
                    on=["demand_group_id", "home_zone_id", "motive_seq_id"]
                )
            )
               
        return pl.concat(spatialized_chains)
        
        
    def spatialize_trip_chains_step(self, seq_step_index, chains_step, dest_prob, costs, alpha, seed):
        """Samples destinations for one non-anchor step via exponential race.
        
        Adjusts probabilities with a penalty toward the anchor distance,
        then samples a single `to` per group using the log(noise)/p_ij trick.
        Anchor motives are passed through unchanged.
        
        Args:
            seq_step_index (int): Step index (>=1).
            chains_step (pl.DataFrame): Rows for this step, must include
                ["demand_group_id","home_zone_id","motive_seq_id","motive",
                 "is_anchor","anchor_to","from"].
            dest_prob (pl.DataFrame): Destination probabilities with
                ["motive","from","to","p_ij"].
            costs (pl.DataFrame): OD costs with ["from","to","cost"].
            alpha (float): Penalty coefficient for anchor distance.
            seed (int): 64-bit RNG seed for reproducibility.
        
        Returns:
            pl.DataFrame: Sampled rows with
                ["demand_group_id","home_zone_id","motive_seq_id","motive",
                 "anchor_to","from","to"].
        """
        
        # Tweak the destination probabilities so that the sampling takes into
        # account the cost of travel to the next anchor (so we avoid drifting
        # away too far).
        
        # Use the exponential sort trick to sample destinations based on their probabilities
        # (because polars cannot do weighted sampling like pandas)
        # https://timvieira.github.io/blog/post/2019/09/16/algorithms-for-sampling-without-replacement/
        
        steps = (
        
            chains_step
            .filter(pl.col("is_anchor").not_())
            
            .join(dest_prob, on=["from", "motive"])
            
            .join(
                costs,
                left_on=["to", "anchor_to"],
                right_on=["from", "to"]
            )
            
            .with_columns(
                p_ij=( 
                    (pl.col("p_ij").clip(1e-18).log()   # Make sure p_ij > 0
                     - alpha*pl.col("cost")).exp()
                ),
                noise=( 
                    pl.struct(["demand_group_id", "motive_seq_id", "to"])
                    .hash(seed=seed)
                    # .cast(pl.Float64) 
                    .truediv(pl.lit(18446744073709551616.0))
                    .log()
                    .neg()
                )
            )
            
            .with_columns(
                sample_score=( 
                    pl.col("noise")/pl.col("p_ij").clip(1e-18)
                    + pl.col("to").cast(pl.Float64)*1e-18
                )
            )
            
            .with_columns(
                min_score=pl.col("sample_score").min().over(["demand_group_id", "motive_seq_id"])
            )
            .filter(pl.col("sample_score") == pl.col("min_score"))
            
            .select(["demand_group_id", "home_zone_id", "motive_seq_id", "motive", "anchor_to", "from", "to"])
            
        )
        
        # Add the steps that end up at anchor destinations
        steps_anchor = (
            chains_step
            .filter(pl.col("is_anchor"))
            .with_columns(
                to=pl.col("anchor_to")
            )
            .select(["demand_group_id", "home_zone_id", "motive_seq_id", "motive", "anchor_to", "from", "to"])
        )
               
        steps = pl.concat([steps, steps_anchor])
        
        return steps
