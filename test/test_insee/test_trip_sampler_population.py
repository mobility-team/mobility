
import sys
sys.path.append("..")
 
from trip_sampler import TripSampler #Import the trip sampler from the .py file,
                                    #as the code has been modified compared to the currently Trip Sampler packaged version
                                    #(columns "trip_type" and "n_travel" are added to the output of the Trip Sampler function "get trips", for the need of the following functions)
import pandas as pd
from pathlib import Path
import os
import numpy as np


t=TripSampler()

data_folder_path = Path(os.path.dirname(__file__))

#%%

def load_data(year=2019, source="INSEE"):
    """
    

    Parameters
    ----------
    year : integer, optional
        the year parameters wich data to use if the INSEE source is chosen. The default is 2019. 
        if the year is inferior to 2015, the 2011 INSEE population data will be taken.  Else, the 2018 file is loaded
        
    source : string, optional
         the source  parameters wich data to use between the population of the SDES and the population of INSEE

    Returns
    -------
    indiv : Dataframe 
       indiv is  a dataframe of individuals
        
    """
    
    if source =="SDES":
        
        indiv = pd.read_parquet(data_folder_path/"data/sdes_2019_pop.parquet")
        indiv= indiv.drop(columns="distcom")
        indiv.columns=[ "weight_indiv","n_pers","urban_unit_category","csp","csp_ref","n_cars"]
        indiv.loc[indiv["csp"]=="no_csp", "csp"]="8"
        
    if source == "INSEE": 
        
        if year >=2015 : 
        
            indiv = pd.read_parquet(data_folder_path/"data/pop2018bis.parquet")
            print ("données INSEE de 2019")
        
        else : 
            indiv = pd.read_parquet(data_folder_path/"data/pop2011bis.parquet")  
            print ("données INSEE de 2011")
        
        # Adapt INSEE data to fit with mobility trip sampler model
        
        indiv= indiv.drop(columns="GARL")
        
        indiv.columns=["cantville","age","csp", "weight_indiv","n_pers","n_cars","urban_unit_category","csp_ref"]
        
        indiv.loc[indiv["n_cars"].astype(int) > 1,"n_cars"] = "2+"
        
        
        if year >=2015 : 
            
            indiv["weight_indiv"]= indiv["weight_indiv"].astype(dtype=np.float64)
        
    return (indiv)

indiv=load_data(source="SDES")



def get_indiv_trips(csp,urban_unit_category,n_cars=None, n_pers=None, csp_ref=None, year =2018):
    """  
    
    
    Parameters
    ----------
    csp :String
    Socio professional category of the individual
    
    urban_unit_category : String
    category of the city in which lives the individual (R,B,C,I INSEE classification)
    
    n_cars : String
        the number of cars of the household the individual belongs to. 

    Returns
    -------
    n_trips_per_weekday : float
        average number of trips per weekday, for the individual
        
    len_trips_weekday : float
        average length in kilometers, per weekday trip,  for the individual
        
    n_trips_per_weekend_day : float
        average number of trips per weekend day, for the individual
        
    len_trips_weekend_day : float
        average length in kilometers, per weekend trip,  for the individual
        
    tot_len_travel : float
        total lenght made while traveling, in kilometers, per year, for the individual 

    """
    
    cs1 = csp
    uu = urban_unit_category
    
    if year <2015 : 
        t.__init__(source="ENTD-2008")
      
    if n_cars!=None : 
        
        all_trips = t.get_trips(csp=cs1, csp_household="4", urban_unit_category=uu, n_pers="2", n_cars=n_cars, n_years=1)
        
    elif n_pers!=None and csp_ref!= None and n_cars==None:
        
        all_trips = t.get_trips(csp=cs1, csp_household=csp_ref, urban_unit_category=uu, n_pers=n_pers, n_cars=None, n_years=1)
    else: 
        return ("missing data")
      
    weekday_trips = all_trips.loc[all_trips["trip_type"]=="short_trips_week_day"].copy()
    
    weekend_trips = all_trips.loc[all_trips["trip_type"]=="short_trips_weekend"].copy()
    
    long_trips = all_trips.loc[(all_trips["trip_type"]=="long_trips")].copy()
    
    travels=long_trips
      
    n_weekdays = weekday_trips["n_travel"].values[0]
    
    if len(weekend_trips["n_travel"])>0:
        
        n_weekend_days = weekend_trips["n_travel"].values[0]
        
    else:
        
        n_weekend_days = 0
    
    n_trips_per_weekday = weekday_trips.shape[0]/n_weekdays
    
    len_trips_weekday = weekday_trips["distance"].sum()/weekday_trips.shape[0]
    
    if n_weekend_days!= 0:
        
        n_trips_per_weekend_day = weekend_trips.shape[0]/n_weekend_days
        
        len_trips_weekend_day = weekend_trips["distance"].sum()/weekend_trips.shape[0]
        
    else :
        
        n_trips_per_weekend_day = 0
        
        len_trips_weekend_day = 0
        
    tot_len_travel=travels["distance"].sum()
  
    
    return (n_trips_per_weekday,len_trips_weekday, n_trips_per_weekend_day,len_trips_weekend_day, tot_len_travel)
  
    

def sampled_indiv_data(n, indiv_data, mode= "n_cars", year= 2018):
    """


    Parameters
    ----------
    n : integer
      Number of individuals to compute trips

    indiv_data : dataframe
      Dataframe of individuals, in which n individuals will be sampled
        
    mode : string, optional
      Parameters the way of computing the trips of the individuals. The default is "n_cars".
        
    year : int, optional
           the year parameters wich SDES data to use to compute trips (ENTD 2009 or 2018 EMP)  
           if the year is inferior to 2015, the ENTD 2009 results will be taken. Else, the 2018 EMP results will be taken. The default is 2019. 

    Returns
    -------
    output : returns a dataframe, containing the different values returned by get_indiv_trips for every sampled individuals

    """
    
    
    sample = indiv_data.sample(n, weights="weight_indiv")
    
    output=[]
    
    if year <2015 : 
        
        print ("using ENTD 2008 values to compute the trips")
      
        t.__init__(source="ENTD-2008")
        
    
    for i in range(len(sample)):
        csp=sample.iloc[i]["csp"]
        
        urban_unit_category = sample.iloc[i]["urban_unit_category"]
        
        if mode == "n_cars": 
            
            # the raw data from the individuals dataframe is used
        
            n_cars=sample.iloc[i]["n_cars"]
            
            total = get_indiv_trips(csp=csp, urban_unit_category=urban_unit_category, n_cars=n_cars, year=year)
            
        elif mode== "p_car" : 
            
            # a number of possessed cars is computed, thanks to the trip sampler. New information about the individual are needed 
            
            n_pers = sample.iloc[i]["n_pers"]
            
            csp_ref = sample.iloc[i]["csp_ref"]
            
            if csp_ref=="no_csp":
                
                # the Trip Sampler raises error if a number of cars is beign computed with a csp ref being equal to no_csp.
                
                #to avoid raising this error, the n_car method is used
                  
                n_cars=sample.iloc[i]["n_cars"] 
                
                total = get_indiv_trips(csp=csp, urban_unit_category=urban_unit_category, n_cars=n_cars, year=year)
                
            else:   
                
                #if there is no risk to raises an error, a number of possessed cars is computed
                total = get_indiv_trips(csp=csp, urban_unit_category=urban_unit_category, csp_ref=csp_ref, n_pers=n_pers, year=year)
                
        # for every individual the output values of get indiv trips are stored into a Dataframe (1,7)    
        
        n_trips_per_weekday=total[0]
        
        len_trips_weekday=total[1]
        
        n_trips_per_weekend_day=total[2]
        
        len_trips_weekend_day=total[3]
        
        total_len_travel=total[4]
       
        indiv={'urban_unit_category':[urban_unit_category],'CSP':[csp],'trips/day: weekday':[n_trips_per_weekday], 'dist/trips : weekday':[len_trips_weekday],
                'trips/day : weekend':[n_trips_per_weekend_day],'dist/trips : weekend':[len_trips_weekend_day],'travel_dist/y':[total_len_travel]}
        
        
        indiv= pd.DataFrame(indiv)
        
        # the dataframes are stored into a list
        
        output.append(indiv)
        
    #the list of dataframe is concatenated, to form a dataframe with n rows.
        
    output= pd.concat(output)
            
    return (output)
    


def get_tables(n, indiv, by, mode, year = 2018): 
    """
    

    Parameters
    ----------
    n : integer
        number of individuals to sample per target group
    
    indiv : Dataframe
        the dataframe of individuals, wether from the INSEE or the SDES
        
    by : string
        the argument to form the target groups. It can wether be "CSP" or "city_category" 
        
    mode : string
         Parameters the mode to use to compute the output values of the sampled_indiv_data function 
         
    year : TYPE, optional
       Parameters the SDES trip data to use to compute the output values of the sampled_indiv_data function 

    Returns
    -------
    Csv files, containing the average output results for every target group

    """
    
    disc=[]
    
    by=by
    
    if by=="CSP":
        
         #--Create subtables containing a target group of individuals. The target groups are sorted by CSP
        indiv = indiv.set_index("csp")
        
        ii1=indiv.xs("1")
        ii2=indiv.xs("2")
        ii3=indiv.xs("3")
        ii4=indiv.xs("4")
        ii5=indiv.xs("5")
        ii6=indiv.xs("6")
        ii7=indiv.xs("7")
        ii8=indiv.xs("8")
        
           
        ii1.reset_index(level="csp", inplace=True)
        ii2.reset_index(level="csp", inplace=True)
        ii3.reset_index(level="csp", inplace=True)
        ii4.reset_index(level="csp", inplace=True)
        ii5.reset_index(level="csp", inplace=True)
        ii6.reset_index(level="csp", inplace=True)
        ii7.reset_index(level="csp", inplace=True)
        ii8.reset_index(level="csp", inplace=True)
        
    
        #disc=[ii1,ii2,ii3,ii4,ii5,ii6,ii7,ii8] #parameter the loop that will form the output dataframe
        disc=[ii3,ii4,ii5,ii6,ii7,ii8]
        
    if by=="city_category":
        
        #--Create subtables containing a target population of individuals. The target groups are sorted by city category 
        
        indiv = indiv.set_index("urban_unit_category")
        
        iiR=indiv.xs("R")
        iiB=indiv.xs("B")
        iiC=indiv.xs("C")
        iiI=indiv.xs("I")
        
        iiR.reset_index(level="urban_unit_category", inplace=True)
        iiB.reset_index(level="urban_unit_category", inplace=True)
        iiC.reset_index(level="urban_unit_category", inplace=True)
        iiI.reset_index(level="urban_unit_category", inplace=True)
        
        disc=[iiR,iiB,iiC,iiI] #parameter the loop that will  form the output dataframe
       
  
    #format the output dataframe
    
    result=pd.DataFrame(index=['trips/day: weekday','dist/trips : weekday','trips/day : weekend','dist/trips : weekend','travel_dist/y']) #the output values are set as index
    
    #add an output column for every target group 
                
    for i in disc :
      
        cat = sampled_indiv_data(n,i,mode=mode, year=year)
        
        cat = cat.drop(columns = ["urban_unit_category","CSP"]) #drop columns containing str values 
        
        r=cat.sum()/n   # for every target group, compute the average values of the variables returned by get_indiv_trips
        
                        # the sum operation transform r to a Series object, with five column and one line 
                        
                                    
        r=r.to_frame()  #  the r object is transposed, and is registered as a dataframe object. the columns of r  are also set as index 
        
                        
        #the type and format of r eases the merge with the exisiting output dataframe 
        
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
     
      if year >=2015 :
          
          result.to_csv(data_folder_path/"output/compute_city_category_18.csv", sep=";")
      else :
          print( "2011 INSEE data and 2009 ENTD were used")
          result.to_csv(data_folder_path/"output/compute_city_category_09.csv", sep=";")
        
    
    return ()
        
#%%
        
test= get_tables(384,indiv, by="CSP", mode="p_car")


