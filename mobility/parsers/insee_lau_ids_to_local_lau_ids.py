import pandas as pd
from unidecode import unidecode

insee_cities = pd.read_csv("D:/dev/mobility_oss/mobility/data/insee/insee_RP2021_mobpro_border_cities.csv", header=None, names=["insee_id", "insee_name"])
insee_cities = insee_cities[insee_cities["insee_id"].str[0:2] == "SU"]
insee_cities["insee_name_clean"] = insee_cities["insee_name"].str.lower()
insee_cities["insee_name_clean"] = insee_cities["insee_name_clean"].str.replace("-", " ")
insee_cities['insee_name_clean_wo_paren'] = insee_cities['insee_name_clean'].str.replace(r'\s*\(.*?\)', '', regex=True)

bfs_cities = pd.read_excel("D:/dev/mobility_oss/mobility/data/bfs/je-f-21.03.01.xlsx", skiprows=9, usecols=[0, 1], header=None, names=["bfs_id", "bfs_name"], nrows=2172)
bfs_cities["bfs_name_clean"] = bfs_cities["bfs_name"].apply(lambda x: unidecode(x))
bfs_cities["bfs_name_clean"] = bfs_cities["bfs_name_clean"].str.lower()
bfs_cities["bfs_name_clean"] = bfs_cities["bfs_name_clean"].str.replace("-", " ")
bfs_cities['bfs_name_clean_wo_paren'] = bfs_cities['bfs_name_clean'].str.replace(r'\s*\(.*?\)', '', regex=True)

insee_bfs = pd.merge(insee_cities, bfs_cities[["bfs_id", "bfs_name_clean"]], left_on="insee_name_clean", right_on="bfs_name_clean", how="left")
insee_bfs = pd.merge(insee_bfs, bfs_cities[["bfs_id", "bfs_name_clean_wo_paren"]], left_on="insee_name_clean_wo_paren", right_on="bfs_name_clean_wo_paren", how="left", suffixes=["_1", "_2"])

insee_bfs["bfs_id"] = insee_bfs["bfs_id_1"].where(~insee_bfs["bfs_id_1"].isnull(), insee_bfs["bfs_id_2"])

insee_bfs[insee_bfs["bfs_id_2"].isnull()]
insee_bfs

# Around 500 swiss cities (~25%) cannot be matched by name because they were merged
# so we would have to use the BFS cities mutations dataset to match them with their current city

insee_bfs = insee_bfs[["insee_id", "bfs_id"]].groupby("insee_id", as_index=False).first()
insee_bfs = insee_bfs[~insee_bfs["bfs_id"].isnull()]

insee_bfs["bfs_id"] = insee_bfs["bfs_id"].astype(int)

insee_bfs.to_excel("D:/insee_bfs_mapping.xlsx")
