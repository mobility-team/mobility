import pandas as pd
from collections import defaultdict
from mobility.in_memory_asset import InMemoryAsset

class MobilitySurveyAggregator(InMemoryAsset):
    
    def __init__(self, population, surveys):
        inputs = {
            "population": population,
            "surveys": surveys
        }
        super().__init__(inputs)
        
    def get(self):
        
        population = self.inputs["population"].get()
        countries = list(population["country"].unique())
        
        survey_data = {
            country: self.get_survey_data(country) 
            for country in countries
        }
        
        if len(countries) == 1:
            survey_data = ( 
                survey_data[countries[0]]
                
            )
            
            survey_data = {
                k: ( 
                    v
                    .assign(country=countries[0])
                    .set_index('country', append=True)
                )
                for k, v in survey_data.items()
            }
            
        else:
            
            combined = defaultdict(list)

            for country in countries:
                for key, df in survey_data[country].items():
                    if not df.empty:
                        combined[key].append(
                            df
                            .assign(country=country)
                            .set_index('country', append=True)
                        )
            
            survey_data = {k: pd.concat(v) for k, v in combined.items()}
                
        return survey_data
            

    def get_survey_data(self, country):
        
        surveys = self.inputs["surveys"]
            
        if country in surveys:
            survey = surveys[country].get()
        else:
            raise ValueError(f"No mobility survey was provided for country {country}")
        
        return survey
        