# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 15:27:31 2022

@author: a.girot
"""

import sys
sys.path.append("..")
from trip_sampler import *
 #%%
t=TripSampler()
import pandas as pd
from pathlib import Path
import os
import matplotlib.pyplot as plt
import numpy as np
import plotly_express as px

#---Get the data 


data_folder_path = Path(os.path.dirname(__file__))



test_type = "PT offer"


allt=[]
size=250

mode= "yrbr"

if test_type == "PT offer":
    
    indiv_data=pd.read_parquet(data_folder_path/"data/sdes_2019_pop.parquet")
    indiv_data= indiv_data[(indiv_data["distcom"].astype(float)<5)].copy()
    indiv_data= indiv_data[(indiv_data["distcom"].astype(float)!=0)].copy()
    
    get_value=["1","2","3","4"]
    indiv_data =indiv_data.set_index(["distcom"])
   
   
     
    
if test_type == "parking spot": 
    indiv_data=pd.read_parquet(data_folder_path/"data/pop2018bis.parquet")
    indiv_data=indiv_data.sample (1)
    indiv_data= indiv_data.drop(columns=("CANTVILLE","AGEREV"))
    indiv_data.columns=("csp","weight_indiv","n_pers","n_cars","csp_ref")
    get_value=["1","2"]
    #indiv_data=indiv_data.set_index(["GARL"])
  
for j in (get_value):
   
    indiv_test = indiv_data.xs(j)
   
    for k in (["R","B","C","I"]):
         
        testo = indiv_test[indiv_test["STATUTCOM_UU_RES"]==k].copy()
        
        if testo.shape[0]>size: 
            testo=testo.sample(size, weights="pond_indC")    
        
        
            for i in range(len(testo)):
                get_triped= t.get_trips(csp=testo.iloc[i]["cs1"], csp_household=testo.iloc[i]["csp_ref"], urban_unit_category=k, n_pers=testo.iloc[i]["NPERS"], n_cars=testo.iloc[i]["n_cars"], n_years=1)  
                get_triped["distcom"]=j
                get_triped["city_category"]=k
                allt.append(get_triped) 


df=pd.concat(allt)                                
df=df[df["trip_type"]=="short_trips_week_day"].copy()   
#--- Prepare the data to plot understandable figures

color_mode= {"Marche":"powderblue", "Deux roues":"lightsteelblue","Automobile":"sandybrown",
             "Taxi et ramassage scolaire/employeur":"gold" ,"TC léger (bus, tram, navette fluviale)":"yellowgreen",
             "TC lourd (métro, TGV, TER)":"limegreen","Avion":"rebeccapurple", "Bateau":"violet"}

df["mode2"] = df["mode_id"].str.slice(0,1) 
df.loc[df["mode_id"].isin(["5.56","5.57","5.58"]), "mode2"]="6"

df=df[(df["mode2"]!="9")].copy() #remove incomplete data
df.loc[df["mode2"]=="1", "mode2"]="Marche"
df.loc[df["mode2"]=="2", "mode2"]="Deux roues" 
df.loc[df["mode2"]=="3", "mode2"]="Automobile"
df.loc[df["mode2"]=="4", "mode2"]="Taxi et ramassage scolaire/employeur"
df.loc[df["mode2"]=="5", "mode2"]="TC léger (bus, tram, navette fluviale)"
df.loc[df["mode2"]=="6", "mode2"]="TC lourd (métro, TGV, TER)"
df.loc[df["mode2"]=="7", "mode2"]="Avion"
df.loc[df["mode2"]=="8", "mode2"]="Bateau"

df=df[df["distcom"].astype(float)<8].copy() #remove incomplete data
df.loc[df["distcom"]=="1", "distcom"]=" - 300 m"
df.loc[df["distcom"]=="2", "distcom"]="300-599 m"
df.loc[df["distcom"]=="3", "distcom"]="600-999 m"
df.loc[df["distcom"]=="4", "distcom"]="999 + m"

 #Remove uncomplete data
      
    
def modal_share(data,test, mode ,share="D"):    

 
    #calcul des parts modales, en distances, selon la motorisation des ménages ou la desserte TC
    if test=="ncar": 
        
        if share== "D":
            
            modist1 = data.groupby(["city_category","mode2","n_cars"])["dist"].sum()
            
            modist2 = data.groupby(["city_category","n_cars"])["dist"].sum()
        
        if share== "T":
            
            modist1 = data.groupby(["city_category","mode2","n_cars"])["dist"].count()
            
            modist2 = data.groupby(["city_category","n_cars"])["dist"].count()
            
    elif test=="PT_offer": 
        
        if share== "D":
       
            modist1 = data.groupby(["city_category","mode2","distcom"])["distance"].sum()
        
            modist2 = data.groupby(["city_category","distcom"])["distance"].sum()   
            
        if share== "T":
            
            modist1 = data.groupby(["city_category","mode2","distcom"])["distance"].count()
            
            modist2 = data.groupby(["city_category","distcom"])["distance"].count()
            
    
    
    modist2=pd.DataFrame(modist2)

    modist1 = modist1.reset_index("mode2")
    
    modist=modist1.merge(modist2, left_index=True, right_index=True)
    
    if mode=="tot_dist":
        modist["total distance per mode (km/year/person)"]=modist["distance_x"]/size
        
    else :
        modist["modal share " + share]=modist["distance_x"]/modist["distance_y"]
        
    modist= modist.reset_index()

 
    for i in (["R","B","C","I"]):         
        stat2=modist[modist["city_category"]==i].copy()
        
        if test=="PT_offer":
            
            if mode=="tot_dist":
                fig=px.histogram(stat2,x="distcom", y="total distance per mode (km/year/person)", color="mode2",color_discrete_map=color_mode, title=  " total distance per mode, given distance to PT , city category "+ i, 
                               text_auto=True )
            else: 
            
                fig=px.histogram(stat2,x="distcom", y="modal share " + share, color="mode2",color_discrete_map=color_mode, title=  share +" Modal share  given distance to PT , city category "+ i, 
                           text_auto=True )
           
                
                      
        if test=="ncar":
            stat2["n_cars"]=stat2["n_cars"].astype(str)
            fig=px.bar(stat2,x="n_cars", y="modal share " + share, color="mode2", color_discrete_map = color_mode, title= share +" Modal share given number of cars owned , city category "+ i
                      )
                           
        fig.show()

    return ()

    
y=modal_share(data=df,test="PT_offer",mode= "rtrg", share="T")
