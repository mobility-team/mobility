import optuna
import logging

import polars as pl
import plotly.express as px

from functools import partial
from typing import List

from mobility.parsers.mobility_survey import MobilitySurvey

class SurveyPreCalibrator:
    
    def run(
            self,
            chains: pl.DataFrame,
            demand_groups: pl.DataFrame,
            surveys: List[MobilitySurvey],
            motive_dur: pl.DataFrame,
            home_night_dur: pl.DataFrame,
            is_weekday: bool,
            utility_of_stay_home_time: float = 1.0,
            n_iterations: int = 300,
            loss_fun: str = "NLL",
            plot: bool = False
        ):
        
        logging.info("Precalibrating on survey data...")
        
        country_categories = chains["country"].dtype.categories
        csp_categories = chains["csp"].dtype.categories
        mode_categories = chains["mode"].dtype.categories
        motive_categories = chains["motive"].dtype.categories
        
        demand_groups = (
            demand_groups
            .group_by(["country", "city_category", "csp", "n_cars"])
            .agg(pl.col("n_persons").sum())
        )
        
        survey_immobility_probability = self.get_survey_immobility_probability(
            surveys,
            csp_categories,
            is_weekday
        )
        
        survey_states_steps = self.get_survey_states_steps(
            chains,
            demand_groups,
            survey_immobility_probability
        )
        
        survey_stay_home_states = self.get_survey_stay_home_states(
            demand_groups,
            survey_immobility_probability,
            mode_categories,
            motive_categories
        )
        
        survey_states = self.combine_survey_states(
            survey_states_steps,
            survey_stay_home_states
        )
        
        study = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(
                multivariate=True,
                group=True,
                seed=0
            )
        )
        
        study.optimize(
            self.get_objective(
                survey_states_steps,
                survey_stay_home_states,
                demand_groups,
                motive_dur,
                home_night_dur,
                motive_categories,
                mode_categories,
                country_categories,
                utility_of_stay_home_time,
                loss_fun
            ),
            n_trials=n_iterations
        )
        
        parameters = self.get_best_parameters_from_study(
            study,
            motive_categories,
            mode_categories,
            country_categories,
            utility_of_stay_home_time
        )
        
        model_states = self.run_model(
            parameters,
            survey_states_steps,
            survey_stay_home_states,
            motive_dur,
            home_night_dur,
            demand_groups
        )
        
        comparison, loss_value = self.compare_model_survey(
            survey_states,
            model_states,
            loss_fun
        )

        if plot:
            self.plot_model_vs_survey_data(comparison)
        
        
        logging.info(f"Loss value after precalibration: {loss_value}")
        
        return parameters

        

        
    def get_survey_immobility_probability(self, surveys, csp_categories, is_weekday):
        
        surveys_immobility = [
            ( 
                pl.DataFrame(s.get()["p_immobility"].reset_index())
                .with_columns(
                    country=pl.lit(s.inputs["country"], pl.String())
                )
            )
            for s in surveys
        ]
        
        surveys_immobility = ( 
            pl.concat(surveys_immobility)
            .with_columns(
                csp=pl.col("csp").cast(pl.Enum(csp_categories)),
                p_immobility=( 
                    pl.when(is_weekday)
                    .then(pl.col("immobility_weekday"))
                    .otherwise(pl.col("immobility_weekend"))
                )
            )
            .select(["csp", "p_immobility"])
        )
        
        return surveys_immobility
        
        
    def get_survey_states_steps(self, chains, demand_groups, survey_immobility_probability):
        
        
        survey_states_steps = (
        
            chains
            .group_by([
                "country", "city_category", "csp", "n_cars",
                "motive_seq_id", "mode_seq", "seq_step_index"
            ])
            .first()
            .select([
                "country", "city_category", "csp", "n_cars", "motive_seq_id",
                "mode_seq", "seq_step_index", "motive", "mode", "distance",
                "travel_time", "duration_per_pers", "p_seq"
            ])
            
            .join(
                demand_groups.group_by(["country", "city_category", "csp", "n_cars"]).agg(pl.col("n_persons").sum()),
                on=["country", "city_category", "csp", "n_cars"]
            )
            
            .join(
                survey_immobility_probability,
                on="csp"
            )
            
            .with_columns(
                n_persons=pl.col("n_persons")*pl.col("p_seq")*(1.0-pl.col("p_immobility"))
            )
        
        )
        
        return survey_states_steps
    
    def get_survey_stay_home_states(self, demand_groups, survey_immobility_probability, mode_categories, motive_categories):
        
        stay_home_states = (
            
            demand_groups
                    
            .with_columns(
                motive_seq_id=pl.lit(0, pl.UInt32()),
                mode_seq=pl.lit("0", pl.String()),
                mode=pl.lit(None, pl.Enum(mode_categories)),
                motive=pl.lit(None, pl.Enum(motive_categories)),
                distance=pl.lit(None, pl.Float64()),
                travel_time=pl.lit(None, pl.Float64()),
                seq_step_index=pl.lit(None, pl.Int64()),
                utility=pl.lit(0.0, pl.Float64()),
                stay_home_time=pl.lit(24.0, pl.Float64())
            )
            
            .join(survey_immobility_probability, on=["csp"])
            
            .with_columns(
                n_persons=pl.col("n_persons")*pl.col("p_immobility")
            )
        
        )
        
        return stay_home_states
    
    
    def get_objective(
            self,
            survey_states_steps,
            survey_stay_home_states,
            demand_groups,
            motive_dur,
            home_night_dur,
            motive_categories,
            mode_categories,
            country_categories,
            utility_of_stay_home_time,
            loss_fun
        ):
        
        return partial(
            self.objective,
            survey_states_steps=survey_states_steps,
            survey_stay_home_states=survey_stay_home_states,
            demand_groups=demand_groups,
            motive_dur=motive_dur,
            home_night_dur=home_night_dur,
            motive_categories=motive_categories,
            mode_categories=mode_categories,
            country_categories=country_categories,
            utility_of_stay_home_time=utility_of_stay_home_time,
            loss_fun=loss_fun
        )
    
            
    def objective(
            self,
            trial,
            survey_states_steps,
            survey_stay_home_states,
            demand_groups,
            motive_dur,
            home_night_dur,
            motive_categories,
            mode_categories,
            country_categories,
            utility_of_stay_home_time,
            loss_fun
        ):
        
        parameters = self.get_parameters(
            trial,
            motive_categories,
            mode_categories,
            country_categories,
            utility_of_stay_home_time
        )
        
        model_states = self.run_model(
            parameters,
            survey_states_steps,
            survey_stay_home_states,
            motive_dur,
            home_night_dur,
            demand_groups
        )
        
        survey_states = self.combine_survey_states(
            survey_states_steps,
            survey_stay_home_states
        )
        
        comparison, loss_value = self.compare_model_survey(
            survey_states,
            model_states,
            loss_fun
        ) 
        
        return loss_value
    
    
    def get_parameters(self, trial, motive_categories, mode_categories, country_categories, utility_of_stay_home_time):
        
        utility_of_activity_constant = self.create_optuna_polars_df(
            trial,
            "utility_of_activity_constant",
            "motive",
            motive_categories,
            min_value=utility_of_stay_home_time/100,
            max_value=utility_of_stay_home_time*1,
            log=True
        )
        
        utility_of_activity_time = self.create_optuna_polars_df(
            trial,
            "utility_of_activity_time",
            "motive",
            motive_categories,
            min_value=utility_of_stay_home_time/100,
            max_value=utility_of_stay_home_time*10,
            log=True
        )
        
        min_activity_time_constant = self.create_optuna_polars_df(
            trial,
            "min_activity_time_constant",
            "motive",
            motive_categories,
            min_value=1e-2,
            max_value=10.0,
            log=True
        )
        
        cost_of_mode_time = self.create_optuna_polars_df(
            trial,
            "cost_of_mode_time",
            "mode",
            mode_categories,
            min_value=utility_of_stay_home_time/100,
            max_value=utility_of_stay_home_time*1,
            log=True
        )
        
        cost_of_mode_constant = self.create_optuna_polars_df(
            trial,
            "cost_of_mode_constant",
            "mode",
            mode_categories,
            min_value=utility_of_stay_home_time/100,
            max_value=utility_of_stay_home_time*1,
            log=True
        )
        
        min_stay_home_time_constant = trial.suggest_float("min_stay_home_time_constant", 1e-1, 10.0, log=True)
        global_utility_coefficient = trial.suggest_float("global_utility_coefficient", 0.99, 1.01, log=True)
        
        return {
            "utility_of_activity_constant": utility_of_activity_constant,
            "utility_of_activity_time": utility_of_activity_time,
            "min_activity_time_constant": min_activity_time_constant,
            "cost_of_mode_time": cost_of_mode_time,
            "cost_of_mode_constant": cost_of_mode_constant,
            "min_stay_home_time_constant": min_stay_home_time_constant,
            "utility_of_stay_home_time": utility_of_stay_home_time,
            "global_utility_coefficient": global_utility_coefficient
        }
    
    
    def run_model(
            self,
            parameters,
            survey_states_steps,
            survey_stay_home_states,
            motive_dur,
            home_night_dur,
            demand_groups
        ):
        
        states_steps_utility = (
            
            survey_states_steps
            .join(motive_dur, on=["csp", "motive"], how="left")
            .join(home_night_dur, on=["csp"], how="left")
            .join(parameters["utility_of_activity_time"], on=["motive"], how="left")
            .join(parameters["utility_of_activity_constant"], on=["motive"], how="left")
            .join(parameters["min_activity_time_constant"], on=["motive"], how="left")
            .join(parameters["cost_of_mode_time"], on=["mode"], how="left")
            .join(parameters["cost_of_mode_constant"], on=["mode"], how="left")
        
            .with_columns(
                activity_utility=(
                    pl.col("utility_of_activity_constant")
                    + pl.col("utility_of_activity_time")*pl.col("mean_duration_per_pers")
                    * 
                    (
                        pl.col("duration_per_pers")
                        / (pl.col("min_activity_time_constant").neg().exp()*pl.col("mean_duration_per_pers"))
                    ).log().clip(0.0)
                ),
                travel_utility=(
                    - pl.col("travel_time")*pl.col("cost_of_mode_time")
                    - pl.col("cost_of_mode_constant")
                )
            )
            
            .with_columns(
                utility=pl.col("activity_utility") + pl.col("travel_utility")
            )
            
        )
        
        states_utility = (
        
            states_steps_utility
            .group_by(["country", "city_category", "csp", "n_cars", "motive_seq_id", "mode_seq"])
            .agg(
                utility=pl.col("utility").sum(),
                stay_home_time=24.0 - pl.col("duration_per_pers").sum()
            )
            
        )
        
        
        states_utility = pl.concat([
            states_utility,
            survey_stay_home_states.select([
                "country", "city_category", "csp", "n_cars",
                "motive_seq_id", "mode_seq", "utility", "stay_home_time"
            ])
        ])
        
        
        model_states = (
            
            states_utility
            
            .join(home_night_dur, on="csp")
            
            .with_columns(
                utility_of_stay_home_time=pl.lit(parameters["utility_of_stay_home_time"], pl.Float64()),
                min_activity_time_constant=pl.lit(parameters["min_stay_home_time_constant"], pl.Float64()),
                global_utility_coefficient=pl.lit(parameters["global_utility_coefficient"], pl.Float64())
            )
            
            .with_columns(
                stay_home_utility=(
                    pl.col("utility_of_stay_home_time")*pl.col("mean_home_night_per_pers")
                    * 
                    (
                        pl.col("stay_home_time")
                        / (pl.col("min_activity_time_constant").neg().exp()*pl.col("mean_home_night_per_pers"))
                    ).log().clip(0.0)
                )
            )
            
            .with_columns(
                utility=( 
                    (pl.col("utility") + pl.col("stay_home_utility"))
                    * pl.col("global_utility_coefficient")
                )
            )
            
            .with_columns(
                utility=pl.col("utility") - pl.col("utility").max().over(["country", "city_category", "csp", "n_cars"])
            )
            
            .with_columns(
                log_p=pl.col("utility") - pl.col("utility").exp().sum().over(["country", "city_category", "csp", "n_cars"]).log()
            )
            
            .with_columns(
                p=pl.col("utility").exp()/pl.col("utility").exp().sum().over(["country", "city_category", "csp", "n_cars"])
            )
            
            .join(
                ( 
                    demand_groups
                    .group_by(["country", "city_category", "csp", "n_cars"])
                    .agg(pl.col("n_persons").sum())
                ),
                on=["country", "city_category", "csp", "n_cars"]
            )
            
            .with_columns(
                n_persons_model=pl.col("n_persons")*pl.col("p")
            )
            
            .select([
                "country", "city_category", "csp", "n_cars",
                "motive_seq_id", "mode_seq", "log_p", "n_persons_model"
            ])
            
        )
        
        return model_states
    
    
    def combine_survey_states(self, survey_states_steps, survey_stay_home_states):
        
        survey_states = pl.concat([
            
            ( 
                survey_states_steps
                .filter(
                    pl.col("seq_step_index") == 1
                )
                .select([
                    "country", "city_category", "csp", "n_cars",
                    "motive_seq_id", "mode_seq", "n_persons"
                ])      
            ),
            
            survey_stay_home_states.select([
                "country", "city_category", "csp", "n_cars",
                "motive_seq_id", "mode_seq", "n_persons"
            ])
            
        ])
        
        return survey_states
    
    
        
    def combine_survey_states_steps(self, survey_states_steps, survey_stay_home_states):
        
        survey_states_steps = pl.concat([
            
            ( 
                survey_states_steps
                .select([
                    "country", "city_category", "csp", "n_cars",
                    "motive_seq_id", "mode_seq", "mode", "motive", "n_persons"
                ])      
            ),
            
            survey_stay_home_states.select([
                "country", "city_category", "csp", "n_cars",
                "motive_seq_id", "mode_seq", "mode", "motive", "n_persons"
            ])
            
        ])
        
        return survey_states_steps
    
    
    def create_optuna_polars_df(self, trial, parameter_name, variable_name, variable_values, min_value, max_value, log):
        
        df = ( 
            
            pl.DataFrame([
                {
                    variable_name: v,
                    f"{parameter_name}": trial.suggest_float(
                        f"{parameter_name}:{v}", min_value, max_value, log=log
                    )
                }
                for v in variable_values
            ])
            
            .with_columns(
                pl.col(variable_name).cast(pl.Enum(variable_values))
            )
            
        )
        
        return df
        
        
    def get_best_parameters_from_study(self, study, motive_categories, mode_categories, country_categories, utility_of_stay_home_time):
        
        best_params = pl.DataFrame({
            "param": list(study.best_params.keys()),
            "value": list(study.best_params.values())
        })
        
        utility_of_activity_constant = self.extract_parameter_from_df(
            best_params,
            "utility_of_activity_constant",
            "motive",
            motive_categories
        )
        
        utility_of_activity_time = self.extract_parameter_from_df(
            best_params,
            "utility_of_activity_time",
            "motive",
            motive_categories
        )
        
        min_activity_time_constant = self.extract_parameter_from_df(
            best_params,
            "min_activity_time_constant",
            "motive",
            motive_categories
        )
        
        cost_of_mode_time = self.extract_parameter_from_df(
            best_params,
            "cost_of_mode_time",
            "mode",
            mode_categories
        )
        
        cost_of_mode_constant = self.extract_parameter_from_df(
            best_params,
            "cost_of_mode_constant",
            "mode",
            mode_categories
        )
        
        
        min_stay_home_time_constant = study.best_params["min_stay_home_time_constant"]
        global_utility_coefficient = study.best_params["global_utility_coefficient"]
        
        return {
            "utility_of_activity_constant": utility_of_activity_constant,
            "utility_of_activity_time": utility_of_activity_time,
            "min_activity_time_constant": min_activity_time_constant,
            "cost_of_mode_time": cost_of_mode_time,
            "cost_of_mode_constant": cost_of_mode_constant,
            "min_stay_home_time_constant": min_stay_home_time_constant,
            "utility_of_stay_home_time": utility_of_stay_home_time,
            "global_utility_coefficient": global_utility_coefficient
        }
        
    def extract_parameter_from_df(self, parameters_df, param_name, variable_name, variable_values):
        
        parameter_df = (
            parameters_df
            .filter(pl.col("param").str.contains(param_name))
            .with_columns(
                pl.col("param").str.split(":").list.last().alias(variable_name)
            )
            .with_columns(
                pl.col(variable_name).cast(pl.Enum(variable_values))
            )
            .select([variable_name, "value"])
            .rename({"value": param_name})
        )
        
        return parameter_df
    
    def compare_model_survey(self, survey_states, model_states, loss_fun):
        
        comparison = (
            
            survey_states
            
            .join(
                model_states,
                on=[
                    "country", "city_category", "csp", "n_cars",
                    "motive_seq_id", "mode_seq"
                ]
            )
            
            .with_columns(
                delta=pl.col("n_persons_model") - pl.col("n_persons")
            )
             
        )
        
        if loss_fun == "SSI":
        
            loss_value = (
                
                comparison
                .with_columns(
                    ssi=( 
                        2*pl.min_horizontal([pl.col("n_persons_model"), pl.col("n_persons")])
                        / (pl.col("n_persons_model") + pl.col("n_persons"))
                    ),
                    n=pl.col("n_persons").len()
                )
                    
                .select(
                    -(pl.col("ssi")/pl.col("n")).sum()
                )
                .item()
                
            )
            
        elif loss_fun == "NLL":
            
            loss_value = ( 
                
                comparison
                
                .with_columns(
                    y_log_p=pl.col("n_persons")*pl.col("log_p")
                )
                
                .select(
                    -pl.col("y_log_p").sum()
                )
                .item()
                
            )
            
        elif loss_fun == "CE":
            
            loss_value = ( 
                
                comparison
                
                .filter(pl.col("n_persons").sum().over(["country", "city_category", "csp", "n_cars"]) > 100.0)
                
                .with_columns(
                    p_survey=pl.col("n_persons")/pl.col("n_persons").sum().over(["country", "city_category", "csp", "n_cars"])
                )
                
                .select(
                    -(pl.col("p_survey")*pl.col("log_p")).sum()
                )
                .item()
                
            )
                
            
        else:
            raise ValueError("loss_fun should be SSI or NLL.")
        
        return comparison, loss_value
    
    
    def plot_model_vs_survey_data(self, comparison):
                   
        fig = px.scatter(
            comparison,
            x="n_persons",
            y="n_persons_model",
            log_x=True,
            log_y=True,
            color="country"
        )
        fig.show("browser")
        
        return fig