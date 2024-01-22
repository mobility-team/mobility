from mobility.radiation_departments import run_model_for_territory
from mobility.radiation_FR_CH import get_franco_swiss_data_for_model

terr = ["25", "39", "NE", "VD"]
# terr  =["25","39"]

FR_SUBSET_CODES = ["25318", "25380", "25254", "25320", "25413", "25405", "25307",
                   "25361", "25494", "25442", "25348", "25308", "25486",
                   "25514", "25142", "25459", "25295", "25525", "25362", "25565",
                   "25121", "25263", "25619", "25534", "25096", "25451", "25464",
                   "25252", "25131", "25179", "25483", "25501",  # CCLHMD
                   "25462", "25157"]  # Pontarlier, La Cluse

CH_SUBSET_CODES = ["5764", "5765", "5744", "5872"]  # , "5938", #Vallorbe, Vaulion, Ballaigues, Le Chenit, not Yverdon
# "6458"] #not Neuchâtel

SUBSET_CODES = ["87" + fr for fr in FR_SUBSET_CODES] + ["85" + ch for ch in CH_SUBSET_CODES]

(
    sources_territory,
    sinks_territory,
    costs_territory,
    coordinates,
    raw_flowDT,
) = get_franco_swiss_data_for_model(terr)

run_model_for_territory(sources_territory, sinks_territory, costs_territory, coordinates, raw_flowDT, subset=SUBSET_CODES)
