# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 18:04:39 2022

@author: a.girot
"""

import pandas as pd
from pathlib import Path
import os
import numpy as np

#import plotly_express as px
data_folder_path = Path(os.path.dirname(__file__)).parent.parent
print(data_folder_path )

#%%


data_folder_path = Path(os.path.dirname(__file__)).parent.parent
get_data_path = data_folder_path/"mobility/data/surveys/EMP-2019"



men = pd.read_csv(get_data_path/"tcm_men_public_V2.csv",encoding="latin-1", sep=";",dtype=str, usecols=["ident_men","CS24PR","NPERS","STATUTCOM_UU_RES"] )

distcom = pd.read_csv(get_data_path/"q_menage_public_V2.csv",encoding="latin-1", sep=";",dtype=str, usecols=["IDENT_MEN","BLOGDIST","JNBVEH"])

indiv= pd.read_csv(get_data_path/"tcm_ind_kish_public_V2.csv",encoding="latin-1", sep=";",dtype=str, usecols=["ident_men","ident_ind","CS24"])

pondki= pd.read_csv(get_data_path/"k_individu_public_V2.csv",encoding="latin-1", sep=";",dtype=str, usecols=["IDENT_IND","pond_indC"])

pondki.columns=("ident_ind","pond_indC")

indiv=indiv.merge(pondki, on="ident_ind")

distcom.columns=("ident_men","distcom","n_cars")

indiv=indiv.merge(distcom, on="ident_men")

indiv=indiv.merge(men, on="ident_men")


indiv["csp_ref"]=indiv["CS24PR"].str.slice(0,1)
indiv["cs1"]=indiv["CS24"].str.slice(0,1)

indiv.loc[indiv["cs1"].isna(), "cs1"]="no_csp"
indiv.loc[indiv["cs1"]=='0', "cs1"] = "no_csp"

indiv.loc[indiv["csp_ref"].isna(), "csp_ref"]="no_csp"
indiv.loc[indiv["csp_ref"]=='0', "csp_ref"] = "no_csp"

indiv=indiv.drop(columns=["CS24PR","CS24","ident_men","ident_ind"])

indiv.loc[indiv["NPERS"].astype(float)>2, "NPERS"]="3+"

indiv.loc[indiv["n_cars"].astype(float)>1, "n_cars"]= "2+"

indiv.loc[indiv["STATUTCOM_UU_RES"]=="H", "STATUTCOM_UU_RES"]= "R"


indiv.to_parquet(data_folder_path/"test/test_ef_catcom/data/sdes_2019_pop.parquet")

