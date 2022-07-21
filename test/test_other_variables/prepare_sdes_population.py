# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 18:04:39 2022

@author: a.girot
"""

import pandas as pd
from pathlib import Path
import os
import numpy as np

#%%

data_folder_path = Path(os.path.dirname(__file__)).parent.parent
get_data_path = data_folder_path/"mobility/data/surveys/EMP-2019"

#---Load different  SDES tables to build the output dataframe, that will contain all the needed variables

men = pd.read_csv(get_data_path/"tcm_men_public_V2.csv",encoding="latin-1", sep=";",dtype=str, usecols=["ident_men","CS24PR","NPERS","STATUTCOM_UU_RES"] )

distcom = pd.read_csv(get_data_path/"q_menage_public_V2.csv",encoding="latin-1", sep=";",dtype=str, usecols=["IDENT_MEN","JNBVEH","BLOGDIST"])

indiv= pd.read_csv(get_data_path/"tcm_ind_kish_public_V2.csv",encoding="latin-1", sep=";",dtype=str, usecols=["ident_men","ident_ind","CS24"])

pondki= pd.read_csv(get_data_path/"k_individu_public_V2.csv",encoding="latin-1", sep=";",dtype=str, usecols=["IDENT_IND","pond_indC"])


#---Merge the dataframe

 #change the name of some variables to ease the merge operation
 
pondki.columns=("ident_ind","pond_indC")

distcom.columns=("ident_men","JNBVEH","distcom")

grougrou=distcom.merge(men, on="ident_men")

 # merge
 
indiv=indiv.merge(pondki, on="ident_ind")

indiv=indiv.merge(men, on="ident_men")

indiv=indiv.merge(distcom, on="ident_men")


#--- Format the output dataframe

indiv["cs1"]=indiv["CS24"].str.slice(0,1)

indiv.loc[indiv["cs1"].isna(), "cs1"]="no_csp"
indiv.loc[indiv["cs1"]=='0', "cs1"] = "no_csp"

indiv["csp_ref"]=indiv["CS24PR"].str.slice(0,1)
indiv.loc[indiv["csp_ref"].isna(), "csp_ref"]="no_csp"
indiv.loc[indiv["csp_ref"]=='0', "csp_ref"] = "no_csp"

indiv.loc[indiv["NPERS"].astype(float)>2, "NPERS"]="3+"

indiv["n_cars"] = "0"
indiv.loc[indiv["JNBVEH"].astype(int) == 1, "n_cars"] = "1"
indiv.loc[indiv["JNBVEH"].astype(int) > 1, "n_cars"] = "2+"



indiv.loc[indiv["STATUTCOM_UU_RES"]=="H", "STATUTCOM_UU_RES"]= "R"



indiv=indiv.drop(columns=["CS24PR","CS24","ident_men","ident_ind","JNBVEH"])

testino = indiv.groupby(by=["distcom","STATUTCOM_UU_RES"])["cs1"].count()
testino= pd.DataFrame(testino)
dfu=testino.plot( y="cs1", kind="bar")

indiv.to_parquet(data_folder_path/"test/test_ef_catcom/data/sdes_2019_pop.parquet")

