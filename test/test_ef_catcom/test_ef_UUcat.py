# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 17:43:23 2022

@author: a.girot
"""

import sys
sys.path.append("..")

from trip_sampler import *
from pathlib import Path
import os
import numpy as np
from get_survey_data import get_survey_data
import plotly_express as px
t=TripSampler()
data_folder_path = Path(os.path.dirname(__file__)).parent 
print(data_folder_path)

#%%

class Test_ef_UUcat():
    
 
    def __init__(self, test_type="parking spot"):
        
    # Load INSEE data
    
        data_folder_path = Path(os.path.dirname(__file__)).parent 
        
        
        if test_type=="parking spot":
            
            #get INSEE data containing the needed information for the test, and adjust the data to fit the tripSampler functions
            
            self.indiv = pd.read_parquet(data_folder_path/"test_insee/data/pop2018bis.parquet")
            
            self.indiv.columns="cantville","age","csp","own_parking_spot","weight_indiv","n_pers","n_cars","urban_unit_category","csp_ref"
            
            self.indiv.loc[self.indiv["n_cars"].astype(int) > 1,"n_cars"] = "2+"
            
            self.indiv["weight_indiv"]= self.indiv["weight_indiv"].astype(dtype=np.float64)
            
            
            
            
        elif test_type=="PT offer" : 
            
            self.indiv= pd.read_parquet(data_folder_path/"test_ef_catcom/data/sdes_2019_pop.parquet")
            
            self.indiv.columns="weight_indiv","distcom","n_cars","n_pers","urban_unit_category","csp_ref","csp"
            
            self.indiv["weight_indiv"]= self.indiv["weight_indiv"].astype(dtype=np.float64)
            
        
        else: 
            print ("this test is not available yet. Please choose between 'PT offer' or 'parking spot'")
            
            return ()
            
        
        # load emission factors ( by mode)
        
        self.mode_ef = pd.read_csv(data_folder_path/"test_ef_catcom/data/mode_ef.csv",sep=",",dtype=(str))
        
        self.mode_ef.columns=["mode_id","ef"]
        
    

    def get_co2_per_group(self,trip_group ,size):
        
    
        grp = trip_group.groupby(["mode_id","trip_type"])["distance"].sum()
        grp = grp.reset_index() 
        grp = grp.merge(self.mode_ef, on="mode_id")
        grp=grp.set_index("trip_type")
        
        
        weekday_co2 = grp.xs("short_trips_week_day")
        weekday_co2["total emissions (kg CO2)"]= weekday_co2["distance"]*(weekday_co2["ef"].astype(float))
        weekday_co2_tot=weekday_co2["total emissions (kg CO2)"].sum()/1000/size
    
        return (weekday_co2_tot)
    
    
    def get_results(self,size,test_type="parking spot") :
        
        output= []
        
        indiv= self.indiv
        
        if test_type=="parking spot":
            
            get_value = ["1","2"]
            indiv =indiv.set_index(["own_parking_spot"])
        
        if test_type == "PT offer":
            
            get_value=["1","2","3","4"]
            indiv =indiv.set_index(["distcom"])
            
            
            
        
        for j in (get_value):
            
            indiv_test = indiv.xs(j)
            
            
        
            for k in (["R","B","C","I"]):
                 
                indiv_group = indiv_test[indiv_test["urban_unit_category"]==k].copy()
                
                if indiv_group.shape[0]>size:
                    indiv_group = indiv_group.sample(size,weights="weight_indiv")
                    
                else: 
                    break
                    #size= indiv_group.shape[0]
                
                    
                
                """
                
                if j=="3" and k=="I":
                    indiv_group = indiv_group.sample(80)
                    
                if j=="4" and k=="I":
                    
                    indiv_group = indiv_group.sample(20,weights="weight_indiv")
                    
                else : 
                    i
                 """
                
                trip_group=[]
                
              
               
               
                for i in range (len(indiv_group)):
                    
                #get the trips of k persons in the same subgroup
                
                    csp=indiv_group.iloc[i]["csp"] 
                    n_cars = indiv_group.iloc[i]["n_cars"]
                    indiv_trips= t.get_trips(csp=csp, csp_household="5", urban_unit_category=k, n_pers="2", n_cars=n_cars,n_years=1)
                    
                    trip_group.append(indiv_trips)
                    
                trip_group= pd.concat(trip_group)
 
                co2_group = self.get_co2_per_group(trip_group,size)
                
                if test_type=="parking spot":
                    
                    
                    if j=="1":
                        test_value="yes"
                    if j=="2": 
                        test_value="no"
                    
                    indiv_tested={'urban_unit_category':[k],'own_parking_spot':[test_value], 'emissions : weekday trips (tCO2e/year)':[co2_group]}
                    
                    
                if test_type== "PT offer" :
                     
                    if j=="1":
                        test_value="-300 m"
                    if j=="2": 
                        test_value="300-599 m"
                    if j=="3":
                        test_value="600-999 m"
                    if j== "4": 
                        test_value ="999 + m"
                    
                    indiv_tested={'urban_unit_category':[k],'distance to closest PT stop':[test_value], 'emissions : weekday trips (tCO2e/year)':[co2_group]}
                    
                    
                indiv_tested= pd.DataFrame(indiv_tested)
                
                output.append(indiv_tested)
        
        output= pd.concat(output)   
        
     
                
        if test_type=="parking spot":
            
            fig=px.scatter(output, x=output["urban_unit_category"], y=output["emissions : weekday trips (tCO2e/year)"], color=output["own_parking_spot"])
            
        if test_type=="PT offer":
             
             fig=px.scatter(output, x=output["urban_unit_category"], y=output["emissions : weekday trips (tCO2e/year)"], color=output["distance to closest PT stop"]) #%%
        
        fig.show()
        

#%%
te=Test_ef_UUcat()     
uu=te.__init__( test_type="parking spot")
go=te.get_results(size=1000,test_type="parking spot")