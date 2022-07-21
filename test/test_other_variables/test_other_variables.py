# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 17:43:23 2022

@author: a.girot
"""

import sys
sys.path.append("..")

from trip_sampler import TripSampler
from pathlib import Path
import os
import numpy as np
import pandas as pd 
from get_survey_data import get_survey_data
import plotly_express as px
t=TripSampler()
data_folder_path = Path(os.path.dirname(__file__)).parent 
print(data_folder_path)


#%%

class Test_other_variables():
    
 
    def __init__(self, test_type="parking spot"):
        """
        

        Parameters
        ----------
        test_type : STRING, optional
        
            Parameters the variable to study, and therefore determines the database to load. The default is "parking spot".
            
            the test "parking spot"requires a variable only available in INSEE database 
            
            the test "PT offer" requires a variable only available in the EMP/ ENTD database 

        Returns
        -------
        None.

        """
        
   #--- get INSEE data containing the needed information for the test, and adjust the data to fit the tripsampler functions
    
    
        data_folder_path = Path(os.path.dirname(__file__)).parent 
        
        self.test_type = test_type
        
        if self.test_type=="parking spot":
            
            #this test requires to load the variable 'own_parking_spot', reference as 'GARL' in the INSEE database
            #this variable describes if the household got a private parking spot (own garage, box or parking space)
            
            self.indiv = pd.read_parquet(data_folder_path/"test_insee/data/pop2018bis.parquet")
            
            self.indiv.columns="cantville","age","csp","own_parking_spot","weight_indiv","n_pers","n_cars","urban_unit_category","csp_ref"
            
            self.indiv.loc[self.indiv["n_cars"].astype(int) > 1,"n_cars"] = "2+"
            
            self.indiv["weight_indiv"]= self.indiv["weight_indiv"].astype(dtype=np.float64) 
            
           
            
        elif self.test_type=="PT offer" :
            
            #this test requires to load the variable 'distcom', reference as 'BLOGDIST' in the SDES/EMP database
            #this variable describes, for every household, the distance to the closest Public Transportation stop
            #WARNING : the distance is declared by the household, and can only take 4 different values . It is therefore not a precise value
            
            self.indiv= pd.read_parquet(data_folder_path/"test_ef_catcom/data/sdes_2019_pop.parquet")
            
            self.indiv.columns="weight_indiv","n_pers","urban_unit_category","distcom","csp","csp_ref","n_cars"
            
            self.indiv["weight_indiv"]= self.indiv["weight_indiv"].astype(dtype=np.float64)
    
            total_weight=self.indiv["weight_indiv"].sum()

            self.indiv= self.indiv[(self.indiv["distcom"].astype(float)<6)].copy()
            
            self.indiv= self.indiv[(self.indiv["distcom"].astype(float)!=0)].copy()
            
            final_weight=self.indiv["weight_indiv"].sum()
            
            self.indiv["weight_indiv"]=self.indiv["weight_indiv"]*final_weight/total_weight  #reweight people as we removed individuals
            
           
            
        
        else: 
            print("this test is not available yet. Please choose between 'PT offer' or 'parking spot'")
            
            return
            
            
    #--- load emission factors ( by mode)
        
        self.mode_ef = pd.read_csv(data_folder_path/"test_ef_catcom/data/mode_ef.csv",sep=",",dtype=(str))
        
        self.mode_ef.columns=["mode_id","ef"]
        
        
        return 
        
    
    
    
    def all_trips(self,ref_size=None) :
        """
        Parameters
        ----------
        size : INTEGER 
        
            After loading a population database,the number of individuals to sample per target group* is determined by size** 
            
                
         WARNING :Per default, the sampling size of each target group is determined according the Cochran formula, wich maximum is  384 ***
         
         384 is the results of the Cochran infinite formula considering the parameters set as follows 
         
              - maximum variability S equal to 50% ( p =0.5) 
              - z value critical value of desired confidence= 1,96 (95% confidence level) 
              - ±5% precision 
              
        The limit size for considering a sample population infinite is arbitrarly set to 2000 
        
         If the population is finite, the sample size is lowered acording the Cochran formula for finit populations
           
        *(made for each value of city category, and the variable given by test_type ie from 8 to 16 subgroups),
            
         
        *** Source : https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwikzpbD3Yn5AhUFNxoKHQO7AgoQFnoECDgQAQ&url=https%3A%2F%2Fwww.researchgate.net%2Fprofile%2FSubhash-Basu-3%2Fpost%2FCorrect_sample_size_formula_for_cross_sectional_studies_with_3_methods_of_measurement%2Fattachment%2F5ec585148c9064000159bc0e%2FAS%253A893351335317504%25401590002964049%2Fdownload%2F07_chapter%2B2.pdf&usg=AOvVaw3Rj4tMcbp-m-OohiE4fs46
         
            
        Returns
        -------
        None.

        """ 
   #--- get the trips of persons in every target group, 'size' individuals per target group
   
        if ref_size !=None :
            
            self.ref_size = ref_size
            
        else : 
            
            self.ref_size= 384
            
           
        indiv= self.indiv
        
        self.trip_group=[] 
        
        # Parameter the values for loops that will create the target groups 
        
        if self.test_type=="parking spot":
            
            alpha = "own_parking_spot"
            
            get_value = ["1","2"]
            
            indiv = indiv.set_index(['own_parking_spot'])
            
            
         
        if self.test_type == "PT offer":
            
            alpha= "distcom"
            
            indiv =indiv.set_index(["distcom"])
            
            get_value = ["1","2","3","4"]
            
                
        # Create the subgroups
        
        for j in (get_value):
            
            indiv_test = indiv.xs(j)
        
            for k in (["R","B","C","I"]):
                 
                indiv_group = indiv_test[indiv_test["urban_unit_category"]==k].copy() # the target group of individuals is created
                
                print (k, j, indiv_group.shape[0])
               
                if ref_size==None : #the Cochran method is applied
                
                    if indiv_group.shape[0]>2000:  
                        
                        size = self.ref_size     
                        
                        indiv_group = indiv_group.sample(size,weights="weight_indiv") # if the target group contains enough individuals,use  Cochran value computed for infinite size populations
                        
                       
                        
                    elif indiv_group.shape[0]<2000 and indiv_group.shape[0]>150 :  # else, lower the number of individuals to sample according the Cochran formula*** 
                        
                        size = np.round(self.ref_size/(1+(self.ref_size-1)/indiv_group.shape[0])).astype(int)
                        
                        indiv_group = indiv_group.sample(size,weights="weight_indiv")
                       
                        
        
                    else : 
                        
                        #The minimum size  of target group is arbitrarly set to 200. Under this value, the result won't be computed
                       
                       continue
                   
                if ref_size!=None : # the value set by the user is considered
                
                
                    if indiv_group.shape[0]> self.ref_size: 
                        
                        size = self.ref_size     
                        
                        indiv_group = indiv_group.sample(size,weights="weight_indiv") # if the target group contains enough individuals,use the user value
                        
                        
                    else: 
                        # if the target group size is inferior to the user value, the results for this target group won't be computed
                        continue
                               
                     

                for i in range (len(indiv_group)):
                
                    csp=indiv_group.iloc[i]["csp"]
                    
                    n_cars = indiv_group.iloc[i]["n_cars"]
                    
                    indiv_trips= t.get_trips(csp=csp, csp_household="5", urban_unit_category=k, n_pers="2", n_cars=n_cars,n_years=1)
                    
                    
                    #the variables csp_household and n_pers are set, as they only serve to compute n_cars in the Trip Sampler.
                    
                    #As the n_cars is already available in the data of individuals, we bypass the step of computing n_cars in the Trip Sampler 
                    
                    #In other words csp,urban_unit_category and n_cars, are the only variable that matters in the get trips launched here
                    
                    
                    
                    if self.test_type == "parking spot": 
                        
                        if j=="1":
                            
                            indiv_trips["own_parking_spot"]="yes"
                            
                        else :
                           indiv_trips["own_parking_spot"]="no"
                            
                    
                    elif self.test_type == "PT offer": 
                        
                        if j=="1":
                           indiv_trips["distcom"]="-300 m"
                            
                        if j=="2": 
                           indiv_trips["distcom"]="300-599 m"
                            
                        if j=="3":
                           indiv_trips["distcom"]="600-999 m"
                            
                        if j== "4": 
                           indiv_trips["distcom"]="999 + m"
                         
                        
                    indiv_trips["city_category"]=k
                    
                    indiv_trips["sample_size"]=size
                    
                    self.trip_group.append(indiv_trips)
                    
        self.trip_group= pd.concat(self.trip_group)
       

    def trace_figures(self):
        
          
     #--- Compute total emissions for weekday trips
     
       
        if self.test_type== "parking spot": 
            
            test_value = ["yes","no"]
            test_column = "own_parking_spot"
            output_column="own parking spot"
                    
                    
        if self.test_type== "PT offer" :
                                 
            test_value = ["-300 m","300-599 m","600-999 m","999 + m"]
            test_column = "distcom"
            output_column="distance to closest PT stop"
            
            
         
        
        indiv_tested={'urban unit category':[],output_column:[], 'emissions : weekday trips (tCO2e/year)':[]}

        indiv_tested= pd.DataFrame(indiv_tested)
       
        for j in test_value :
            
            subgroup1= self.trip_group[self.trip_group[test_column] == j].copy()
           
            
            for k in ["R","B","C","I"]: 
                  
                subgroup = subgroup1 [subgroup1["city_category"] == k].copy()
                
                if subgroup.shape[0]!=0 : 
                
                    size = subgroup.iloc[0]["sample_size"]
                    
                    grp =subgroup.groupby(["mode_id","trip_type"])["distance"].sum()
                    
                    grp = grp.reset_index() 
                    
                    grp = grp.merge(self.mode_ef, on="mode_id")
                      
                    weekday_co2 = grp[grp["trip_type"]=="short_trips_week_day"].copy()
                    
                    weekday_co2["total emissions (kg CO2)"]= weekday_co2["distance"]*(weekday_co2["ef"].astype(float))
                    
                    co2_group = weekday_co2["total emissions (kg CO2)"].sum()/size/1000
            
                    indiv_tested.loc[len(indiv_tested.index)]=[k,j,co2_group]
                   
 
               
        if self.test_type=="parking spot":
            
            fig=px.scatter(indiv_tested, x=indiv_tested['urban unit category'], y=indiv_tested["emissions : weekday trips (tCO2e/year)"], color=indiv_tested[output_column],
                           title="Weekday trips yearly emissions, given parking spot")
            
        if self.test_type=="PT offer":
             
            fig=px.scatter(indiv_tested, x=indiv_tested['urban unit category'], y=indiv_tested["emissions : weekday trips (tCO2e/year)"], color=indiv_tested[output_column],
                            title="Weekday trips yearly emissions, given distance to PT closest stop")
        
        
        fig.show()
            
            
        
     #--- Compute modal shares (use share and distance share), and total distance per modes
        
    
        color_mode= {"Marche":"powderblue", "Deux roues":"lightsteelblue","Automobile":"sandybrown",
                         "Transport en commun":"limegreen","Avion":"rebeccapurple", "Bateau":"violet"}
        
       
        # Prepare the data to plot understandable figures
        
        self.trip_group=self.trip_group[self.trip_group["trip_type"]=="short_trips_week_day"].copy()   #only keep local mobility in a weekday
        
        self.trip_group["mode2"] = self.trip_group["mode_id"].str.slice(0,1) #group the modes per category 
        self.trip_group.loc[self.trip_group["mode2"]=="1", "mode2"]="Marche"
        self.trip_group.loc[self.trip_group["mode2"]=="2", "mode2"]="Deux roues" 
        self.trip_group.loc[self.trip_group["mode2"]=="3", "mode2"]="Automobile"
        self.trip_group.loc[self.trip_group["mode2"].isin(["4","5","6"]), "mode2"]="Transport en commun"
        self.trip_group.loc[self.trip_group["mode2"]=="7", "mode2"]="Avion"
        self.trip_group.loc[self.trip_group["mode2"]=="8", "mode2"]="Bateau"
        
        self.trip_group=self.trip_group[(self.trip_group["mode2"]!="9")].copy() #remove incomplete data (no existing mode) 
        

        if self.test_type=="parking spot": 
            
            # Calculating modal share in distance --> sum the distances
            
            modist1 = self.trip_group.groupby(["city_category","mode2","own_parking_spot"])["distance"].sum()
            
            modist2 = self.trip_group.groupby(["city_category","own_parking_spot"])["distance"].sum()
            
            
            # Calculating total distances per mode--> get back the information of the size of every target group (could be different from ref size given Cochran's formula)
            
            modist3 = self.trip_group.groupby(["city_category","own_parking_spot","sample_size"])["trip_type"].count() 
            
            modist3= modist3.reset_index(level="sample_size")
            
            modist3=modist3.drop(columns="trip_type")
            
    
            # Calculating modal share in use --> count trips by mode 
            
            modist4 = self.trip_group.groupby(["city_category","mode2","own_parking_spot"])["distance"].count()
            
            modist5 = self.trip_group.groupby(["city_category","own_parking_spot"])["distance"].count()
              
                
          
        elif self.test_type=="PT offer": 
                
            # Same steps as for parking spot
               
            modist1 = self.trip_group.groupby(["city_category","mode2","distcom"])["distance"].sum()
        
            modist2 = self.trip_group.groupby(["city_category","distcom"])["distance"].sum() 
            
            #
            modist3 = self.trip_group.groupby(["city_category","distcom","sample_size"])["trip_type"].count()
            
            modist3= modist3.reset_index(level="sample_size")
            
            modist3=modist3.drop(columns="trip_type")
            
            
            modist4 = self.trip_group.groupby(["city_category","mode2","distcom"])["distance"].count()
            
            modist5 = self.trip_group.groupby(["city_category","distcom"])["distance"].count()
            
            
        #format the dataframes to allow merging            
       
        modist2=pd.DataFrame(modist2)

        modist1 = modist1.reset_index("mode2")
        
        modist5=pd.DataFrame(modist5)
        
        modist4 = modist4.reset_index("mode2")
        
             
        # Merge the different subtables to finally compute the results
        
        dist_share =modist1.merge(modist2, left_index=True, right_index=True)
        
        use_share=modist5.merge(modist4, left_index=True, right_index=True)
        
        tot_dist=dist_share.merge(modist3, left_index=True, right_index=True)
        
        
        # create new columns in the Dataframes, containing the results 
        
        dist_share["modal share (km)"]=round((100*dist_share["distance_x"].astype(float)/dist_share["distance_y"]), 1) #modal share, in distance
        
        
        tot_dist["total distance per mode (km/year/person)"]=round((tot_dist["distance_x"]/tot_dist["sample_size"])) #total distance per mode (average value, depending on the sample size of every target group)
        
        
        use_share["modal share (use)"]=round((100*use_share["distance_y"].astype(float)/use_share["distance_x"]), 1) #modal share, in use
        
        
        
        
       # format the output Dataframes
        
        tot_dist = tot_dist.drop(columns=["distance_x","distance_y"])
        
        dist_share=dist_share.drop(columns=["distance_x","distance_y"])
     
        dist_share = dist_share.reset_index()
        
        use_share = use_share.reset_index()
        
        tot_dist = tot_dist.reset_index()
        
        
       # Plot results for every target group 
       
        for i in (["R","B","C","I"]):  
            # return three graphs for each city category
            
            use_share_i = use_share[use_share["city_category"]==i].copy()
            
            dist_share_i = dist_share[dist_share["city_category"]==i].copy()
            
            tot_dist_i = tot_dist[tot_dist["city_category"]==i].copy()
       
            
            if self.test_type=="PT offer":
                  
                fig_tot_dist = px.histogram(tot_dist_i ,x="distcom", y="total distance per mode (km/year/person)", color="mode2",color_discrete_map=color_mode, title=  " total distance per mode, given distance to PT , city category "+ i, 
                                   text_auto=True )
                
                fig_use_share = px.histogram(use_share_i,x="distcom", y="modal share (use)", color="mode2",color_discrete_map=color_mode, title= "Use modal share  given distance to PT , city category "+ i, 
                               text_auto=True )
                
                fig_dist_share = px.histogram(dist_share_i,x="distcom", y="modal share (km)", color="mode2",color_discrete_map=color_mode, title= "Distance modal share  given distance to PT , city category "+ i, 
                               text_auto=True )
                
                
               
                    
                          
            if self.test_type=="parking spot":
                
                fig_tot_dist = px.bar(tot_dist_i ,x="own_parking_spot", y="total distance per mode (km/year/person)", color="mode2",color_discrete_map=color_mode, title=  " total distance per mode, given owning parking spot , city category "+ i, 
                                   text_auto=True )
                
                
                fig_use_share = px.bar(use_share_i,x="own_parking_spot", y="modal share (use)", color="mode2",color_discrete_map=color_mode, title= "Use modal share  given owning parking spot, city category "+ i, 
                               text_auto=True )
                
                fig_dist_share = px.bar(dist_share_i,x="own_parking_spot", y="modal share (km)", color="mode2",color_discrete_map=color_mode, title= "Distance modal share  given owning parking spot , city category "+ i, 
                               text_auto=True )
                
                
            fig_tot_dist.show()
            
            fig_use_share.show()
            
            fig_dist_share.show()
                
                
             
        



   
te=Test_other_variables(test_type = "parking spot")   

te.all_trips()

te.trace_figures()







               

      
