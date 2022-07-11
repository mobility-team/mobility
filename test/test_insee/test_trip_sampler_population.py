#from mobility import trip_sampler as tp
import sys
sys.path.append("..")
 
from trip_sampler import TripSampler
import pandas as pd
from pathlib import Path
import os
import numpy as np
from get_survey_data import get_survey_data
#import plotly_express as px

#%%
t=TripSampler()

data_folder_path = Path(os.path.dirname(__file__))

#☻%%
def load_data(year=2019, source="INSEE"):
    
    if source =="SDES":
        
        indiv_insee = pd.read_parquet(data_folder_path/"data/sdes_2019_pop.parquet")
        indiv_insee= indiv_insee.drop(columns="distcom")
        indiv_insee.columns=[ "weight_indiv","n_cars","n_pers","urban_unit_category","csp_ref","csp"]

    if source == "INSEE": 
        
        if year >=2015 : 
        
            indiv_insee = pd.read_parquet(data_folder_path/"data/pop2018bis.parquet")
            print ("données INSEE de 2019")
        
        else : 
            indiv_insee = pd.read_parquet(data_folder_path/"data/pop2011bis.parquet")  
            print ("données INSEE de 2011")
        
        # Adapt INSEE data to fit with mobility trip sampler model
        
        indiv_insee= indiv_insee.drop(columns="GARL")
        
        indiv_insee.columns=["cantville","age","csp", "weight_indiv","n_pers","n_cars","urban_unit_category","csp_ref"]
        
        indiv_insee.loc[indiv_insee["n_cars"].astype(int) > 1,"n_cars"] = "2+"
        
        
        if year >=2015 : 
            
            indiv_insee["weight_indiv"]= indiv_insee["weight_indiv"].astype(dtype=np.float64)
        
    return (indiv_insee)

indiv_insee=load_data()

#%%

#get the trips
    
def get_indiv_trips(csp,urban_unit_category,n_cars=None, n_pers=None, csp_ref=None, year =2018):
    """
    

    Parameters
    ----------
    csp : TYPE
        DESCRIPTION.
    urban_unit_category : TYPE
        DESCRIPTION.
    n_cars : TYPE
        DESCRIPTION.

    Returns
    -------
    n_trips_per_weekday : TYPE
        DESCRIPTION.
    len_trips_weekday : TYPE
        DESCRIPTION.
    n_trips_per_weekend_day : TYPE
        DESCRIPTION.
    len_trips_weekend_day : TYPE
        DESCRIPTION.
    tot_len_travel : TYPE
        DESCRIPTION.

    """
    
    cs1 = csp
    uu = urban_unit_category
    
    if year <2015 : 
        tutu = t.__init__(source="ENTD-2008")
      
    if n_cars!=None : 
        
        all_trips = t.get_trips(csp=cs1, csp_household="4", urban_unit_category=uu, n_pers="2", n_cars=n_cars, n_years=1)
    elif n_pers!=None and csp_ref!= None:
        all_trips = t.get_trips(csp=cs1, csp_household=csp_ref, urban_unit_category=uu, n_pers=n_pers, n_cars=None, n_years=1)
    else: 
        return ("missing data")
    
    # all_trips = all_data[0]
    
    weekday_trips = all_trips.loc[all_trips["trip_type"]=="short_trips_week_day"].copy()
    weekend_trips = all_trips.loc[all_trips["trip_type"]=="short_trips_weekend"].copy()
    long_trips = all_trips.loc[(all_trips["trip_type"]=="long_trips")].copy()
    travels=long_trips
    
   
    
    n_weekdays = weekday_trips["n_travel"].values[0]
    if len(weekend_trips["n_travel"])>0:
        n_weekend_days = weekend_trips["n_travel"].values[0]
    else:
        n_weekend_days = 0
    # n_travel_days = 
    
    n_trips_per_weekday = weekday_trips.shape[0]/n_weekdays
    len_trips_weekday = weekday_trips["distance"].sum()/weekday_trips.shape[0]
    
    if n_weekend_days!= 0:
        n_trips_per_weekend_day = weekend_trips.shape[0]/n_weekend_days
        len_trips_weekend_day = weekend_trips["distance"].sum()/weekend_trips.shape[0]
        
    else :
        n_trips_per_weekend_day = 0
        len_trips_weekend_day = 0
        
    
    tot_len_travel = travels["distance"].sum()
            
    # n_weekdays = all_data[1].shape[0]
    # n_weekend_days = all_data[2].shape[0]
    # n_travel_days = all_data[3]+all_data[4]
    
    # n_trips_per_weekday = weekday_trips.shape[0]/n_weekdays
    # len_trips_weekday = weekday_trips.sum()[4]/weekday_trips.shape[0]
    
    
    
    # if n_weekend_days!= 0:
    #     n_trips_per_weekend_day = weekend_trips.shape[0]/n_weekend_days
    #     len_trips_weekend_day = weekend_trips.sum()[4]/weekend_trips.shape[0]
        
    # else :
    #     n_trips_per_weekend_day = 0
    #     len_trips_weekend_day = 0
        
        
    # tot_len_travel = travels.sum()[3]
        
    
    return (n_trips_per_weekday,len_trips_weekday, n_trips_per_weekend_day,len_trips_weekend_day, tot_len_travel)
  
    
#n_trips_per_weekday,len_trips_weekday, n_trips_per_weekend_day,len_trips_weekend_day, tot_len_travel = get_indiv_trips(csp="7", urban_unit_category="B", n_cars = None, n_pers="2",csp_ref="6")


#%%

def sampled_indiv_data(n, indiv_data, mode= "n_cars", year= 2018):
    
    sample = indiv_data.sample(n, weights="weight_indiv")
    
    output=[]
    
    if year <2015 : 
        
        print ("using ENTD 2008 values to compute the trips")
      
        tutu = t.__init__(source="ENTD-2008")
        
    
    for i in range(len(sample)):
        csp=sample.iloc[i]["csp"]
        
        urban_unit_category = sample.iloc[i]["urban_unit_category"]
        
        if mode == "n_cars":
        
            n_cars=sample.iloc[i]["n_cars"]
            
            total = get_indiv_trips(csp=csp, urban_unit_category=urban_unit_category, n_cars=n_cars, year=year)
        elif mode== "p_car" : 
            n_pers = sample.iloc[i]["n_pers"]
            csp_ref = sample.iloc[i]["csp_ref"]
            
            if csp_ref=="no_csp":
                
                n_cars=sample.iloc[i]["n_cars"] 
                total = get_indiv_trips(csp=csp, urban_unit_category=urban_unit_category, n_cars=n_cars, year=year)
                
            else:   
                
                total = get_indiv_trips(csp=csp, urban_unit_category=urban_unit_category, csp_ref=csp_ref, n_pers=n_pers, year=year)
        
        n_trips_per_weekday=total[0]
        len_trips_weekday=total[1]
        n_trips_per_weekend_day=total[2]
        len_trips_weekend_day=total[3]
        total_len_travel=total[4]
       
        indiv={'urban_unit_category':[urban_unit_category],'CSP':[csp],'trips/day: weekday':[n_trips_per_weekday], 'dist/trips : weekday':[len_trips_weekday],
                'trips/day : weekend':[n_trips_per_weekend_day],'dist/trips : weekend':[len_trips_weekend_day],'travel_dist/y':[total_len_travel]}
        indiv= pd.DataFrame(indiv)
        
        output.append(indiv)
        
    output= pd.concat(output)
            
    return (output)
    
test= sampled_indiv_data(3, indiv_insee, mode="n_cars")

#%%

def get_tables(n, indiv_insee, by, mode, year = 2018): 
    
    disc=[]
    
    by=by
    
    if by=="CSP":
         
         #--Create subtables to ensure every CSP is represented in the trips
        indiv_insee = indiv_insee.set_index("csp")
        

        #ii1=indiv_insee.xs("1")
        #ii2=indiv_insee.xs("2")
        ii3=indiv_insee.xs("3")
        ii4=indiv_insee.xs("4")
        ii5=indiv_insee.xs("5")
        ii6=indiv_insee.xs("6")
        ii7=indiv_insee.xs("7")
        ii8=indiv_insee.xs("8")
           
        #ii1.reset_index(level="csp", inplace=True)
        #ii2.reset_index(level="csp", inplace=True)
        ii3.reset_index(level="csp", inplace=True)
        ii4.reset_index(level="csp", inplace=True)
        ii5.reset_index(level="csp", inplace=True)
        ii6.reset_index(level="csp", inplace=True)
        ii7.reset_index(level="csp", inplace=True)
        ii8.reset_index(level="csp", inplace=True)
    
        disc=[ii3,ii4,ii5,ii6,ii7,ii8]
        
    if by=="city_category":
        
     
        #--Create subtables to ensure every city_category is represented in the trips
        indiv_insee = indiv_insee.set_index("urban_unit_category")
        
        iiR=indiv_insee.xs("R")
        iiB=indiv_insee.xs("B")
        iiC=indiv_insee.xs("C")
        iiI=indiv_insee.xs("I")
        
        iiR.reset_index(level="urban_unit_category", inplace=True)
        iiB.reset_index(level="urban_unit_category", inplace=True)
        iiC.reset_index(level="urban_unit_category", inplace=True)
        iiI.reset_index(level="urban_unit_category", inplace=True)
        
        disc=[iiR,iiB,iiC,iiI]
        #disc=[iiB,iiC]
  
   
    result=pd.DataFrame(index=['trips/day: weekday','dist/trips : weekday','trips/day : weekend','dist/trips : weekend','travel_dist/y'])
                      
    for i in disc :
      
        cat = sampled_indiv_data(n,i,mode=mode, year=year)
        cat = cat.drop(columns = ["urban_unit_category","CSP"])
        r=cat.sum()/n
        r=r.to_frame()
        result=result.merge(r, left_index=True, right_index=True)
        
    if by== "CSP" :
        
       result.columns = ["csp 3","csp 4","csp 5","csp 6","csp 7","csp 8" ]
       
       
       if year >=2015 :
           print( " 2018 INSEE and 2018 EMP were used")
           result.to_csv(data_folder_path/"output/compute_csp_18.csv", sep=";")
           
       else :  
          print( " 2011 INSEE data and 2009 ENTD were used")
          result.to_csv(data_folder_path/"output/compute_csp_09.csv", sep=";")
    
    elif by== "city_category": 
      
      result.columns = ["R","B","C","I"]  
      #result.columns = ["B","C"]  
      if year >=2015 :
          
          result.to_csv(data_folder_path/"output/compute_city_category_18.csv", sep=";")
      else :
          print( " 2011 INSEE data and 2009 ENTD were used")
          result.to_csv(data_folder_path/"output/compute_city_category_09.csv", sep=";")
        
    
    return ()
        
        
        
test= get_tables(990,indiv_insee, by="CSP", mode="p_car")


#%%

number= indiv_insee.groupby("csp").count()

