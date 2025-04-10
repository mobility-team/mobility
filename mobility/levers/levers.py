from dataclasses import dataclass, field
from geojson import GeoJSON
import geopandas

@dataclass(kw_only=True)
class Lever:
    name: str
    short_name: str
    


@dataclass
class SetOfLevers:
    set_of_levers: list[Lever]
    
    def short_name(self):
        short_name = ""
        for lever in self.set_of_levers:
            print(short_name)
            short_name += "_" + lever.short_name
        short_name += "_"
        return short_name

@dataclass
class AddTrainRoute(Lever):
    #layout: GeoJSON # typo à définir 
    frequency: float #tph


@dataclass
class CHNS(Lever):
    name: str = "Car à haut niveau de service"
    short_name: str = "CHNS"
    #gtfs_existing_route # quel format ?
    id_existing_route: str = ""
    frequency: float = 4
    trunks_with_bus_corridor_creation: list[str] = field(default_factory=list)# liste d'identifiants OSM ?
    

@dataclass
class ZFE(Lever):
    name: str = "Zone à faibles émissions"
    short_name: str = "ZFE"
    #area: GeoJSON
    percentage_vehicles_affected: float = 0.1
    

@dataclass
class ZTL(Lever):
    name: str = "Zone à trafic limité"
    short_name: str = "ZTL"
    #area: GeoJSON

@dataclass(kw_only=True)
class low_speed_area(Lever):
    name: str = "Zone 30"
    short_name: str = "Z30"
    area: GeoJSON

@dataclass
class cycle_network(Lever):
    area: GeoJSON # set of areas where all the ways will be transformed to be cycle-friendly
    

@dataclass
class increase_fuel_price(Lever):
    increase_price_per_km: float = 0.2 # price in €
    
@dataclass
class cycle_subside(Lever):
    subside_type: str = "distance" # per distance or per worker
    subside_per_km: float = 0.10
    max_subside_per_day: float = 4
    subside_per_worker: float = 2 


rer_js=AddTrainRoute(name="RER Salève-Jura", short_name="RER", frequency=6)

chns_annecy_1 = CHNS("Car Annecy-Genève v1 sans réduction capacité", "CHNSv1", "ch-r.11.272", 4)
chns_annecy_2 = CHNS("Car Annecy-Genève v2 avec réduction capacité", "CHNSv2", "ch-r.11.272", 4, []) #add OSM ids

ztl_ambitieuse = ZTL()

zone_30_geo = geopandas.read_file("https://gist.githubusercontent.com/Mind-the-Cap/0c5444fb199c202abfa4a6f6ca4b2db4/raw/51dd1669fc04aa4fd30804ae18bd3961e1a36b11/zone30-ambitieux.geojson")
print(zone_30_geo)
zone_30 = low_speed_area(area=zone_30_geo)
print(zone_30)

reseau_velo = cycle_network(area="")

prix_carburant = increase_fuel_price()

sub_velo = cycle_network()


sol = SetOfLevers([rer_js, chns_annecy_1, chns_annecy_2, zone_30, reseau_velo, prix_carburant, sub_velo])
print(sol)
print(sol.short_name())
