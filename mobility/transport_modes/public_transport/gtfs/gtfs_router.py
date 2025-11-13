import gtfs_kit
import os
import pathlib
import json
import logging
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from importlib import resources
from mobility.file_asset import FileAsset
from mobility.transport_zones import TransportZones
from mobility.r_utils.r_script import RScript

from mobility.parsers.download_file import download_file
from mobility.parsers.gtfs_stops import GTFSStops

from .gtfs_data import GTFSData

class GTFSRouter(FileAsset):
    """
    Creates a GTFS router for the given transport zones and saves it in .rds format.
    Currently works for France and Switzerland.
    
    Uses GTFSStops to get a list of the stops within the transport zones, the downloads the GTFS (GTFSData class),
    checks that expected agencies are present (if they were provided by the user in PublicTransportRoutingParameters)
    and creates the GTFS router using the R script prepare_gtfs_router.R
    
    For each GTFS source, this script will only keep stops with the region, add missing route types (by default bus),
    make IDs unique and remove erroneous calendar dates. 
    It will then align all GTFS sources on a common start date and merge all GTFS into one.
    It adds missing transfers between stops using a crow-fly formula, ans with a limit of 200m.
    It finds the Tuesday with the most services running within the montth with the most services on average.
    Finally, this global Tuesday-only GTFS is saved.
    """
    
    def __init__(self, transport_zones: TransportZones, additional_gtfs_files: list = None, expected_agencies: list = None):
        
        inputs = {
            "transport_zones": transport_zones,
            "additional_gtfs_files": additional_gtfs_files,
            "download_date": os.environ["MOBILITY_GTFS_DOWNLOAD_DATE"],
            "expected_agencies": expected_agencies
        }
        
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "gtfs_router.rds"

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self):
        return self.cache_path
    
    def create_and_get_asset(self):
        
        logging.info("Downloading GTFS files for stops within the transport zones...")
        
        transport_zones = self.inputs["transport_zones"]
        expected_agencies = self.inputs["expected_agencies"]
        
        stops = self.get_stops(transport_zones)

        gtfs_files = self.get_gtfs_files(stops)
        
        if self.inputs["additional_gtfs_files"] is not None:
            gtfs_files.extend(self.inputs["additional_gtfs_files"])
            
        if expected_agencies is not None:
            self.check_expected_agencies(gtfs_files, expected_agencies)
        
        self.prepare_gtfs_router(transport_zones, gtfs_files)

        return self.cache_path
    
    def check_expected_agencies(self, gtfs_files, expected_agencies):
        print(gtfs_files)
        for gtfs_url in gtfs_files:
            print("\nGTFS\n")
            gtfs=GTFSData(gtfs_url)
            agencies = gtfs.get_agencies_names(gtfs_url)
            print(agencies)
            print(type(agencies))
            for expected_agency in expected_agencies:
                print(f'Looking for {expected_agency} in {gtfs.name}')
                if expected_agency.lower() in agencies.lower():
                    logging.info(f"{expected_agency} found in {gtfs.name}")
                    expected_agencies.remove(expected_agency)
        print(expected_agencies)
        if expected_agencies == []:
            logging.info("All expected agencies were found")
            return True
        else:
            logging.info("Some agencies were not found in GTFS files.")
            print(expected_agencies)
            raise IndexError('Missing agencies')
            
        
    def get_stops(self, transport_zones):
        
        transport_zones = transport_zones.get()
        
        admin_prefixes = ["fr", "ch"]
        admin_prefixes = [prefix for prefix in admin_prefixes if transport_zones["local_admin_unit_id"].str.contains(prefix).any()]
        
        stops = GTFSStops(admin_prefixes, self.inputs["download_date"])
        stops = stops.get(bbox=tuple(transport_zones.total_bounds))
        
        return stops
    
    
    def prepare_gtfs_router(self, transport_zones, gtfs_files):
        
        gtfs_files = ",".join(gtfs_files)
        
        script = RScript(resources.files('mobility.transport_modes.public_transport.gtfs').joinpath('prepare_gtfs_router.R'))
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                gtfs_files,
                str(resources.files('mobility.data').joinpath('gtfs/gtfs_route_types.csv')),
                str(self.cache_path)
            ]
        )
            
        return None
    
    
    def get_gtfs_files(self, stops):
        
        gtfs_urls = self.get_gtfs_urls(stops)
        gtfs_files = [GTFSData(gtfs_url).get() for gtfs_url in gtfs_urls]
        gtfs_files = [str(f[0]) for f in gtfs_files if f[1] == True]
            
        return gtfs_files
            
            
            
    def get_gtfs_urls(self, stops):
        
        gtfs_urls = []
        
        # Add resource urls that are already known (for Switzerland for example)
        gtfs_urls.extend(stops["resource_url"].dropna().unique().tolist())
        
        # Add transport.data.gouv.fr resource urls by matching their datagouv_id in the global metadata file
        datagouv_dataset_urls = stops["dataset_url"].dropna().unique()
        datagouv_dataset_ids = [pathlib.Path(url).name for url in datagouv_dataset_urls]
        
        url = "https://transport.data.gouv.fr/api/datasets"
        path = ( 
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs"/
            (self.inputs["download_date"] + "_gtfs_metadata.json")
        )
        download_file(url, path)
            
        with open(path, "r", encoding="UTF-8") as f:
            metadata = json.load(f)
            
        for dataset_metadata in metadata:
            if dataset_metadata["datagouv_id"] in datagouv_dataset_ids:
                gtfs_resources = [r for r in dataset_metadata["resources"] if "format" in r.keys()]
                gtfs_resources = [r for r in gtfs_resources if r["format"] == "GTFS"]
                for r in gtfs_resources:
                    gtfs_urls.append(r["original_url"])
                
        return gtfs_urls
    
    def audit_gtfs(self):
        transport_zones = self.transport_zones
        stops = self.get_stops(transport_zones)
        gtfs_files = self.get_gtfs_files(stops)
        
        # Retrieve max services date from file saved with R script
        output_dir = "D:\\test-09" # TBC
        date_file_path = os.path.join(output_dir, "max_services_date.txt")
        try:
            with open(date_file_path, 'r') as f:
                max_services_date_str = f.readline().strip()
                max_services_date = int(max_services_date_str)
                print(f"Max services date is : {max_services_date}")
        except FileNotFoundError:
            print(f"Error : File not found in {date_file_path}")
        except ValueError:
            print("Error : The variable is not a valid integer.")
        except Exception as e:
            print(f"Error in retrieving max services date : {e}")

        for gtfs_url in gtfs_files:
            print("\nGTFS\n")
            print(gtfs_url)
            gtfs=GTFSData(gtfs_url)
            agencies = gtfs.get_agencies_names(gtfs_url)
            #print(agencies)
            #print(type(agencies))
            if "TPG" in agencies:
                print("TPG found in ", gtfs_url)
                #k = gtfs_kit.read_feed(gtfs_url, "m")
                #k = gtfs_kit.miscellany.restrict_to_agencies(k, ["000881"])
                #trips = k.get_trips()
                #print(trips)
                #dates = k.get_dates()
                #print(dates)
                #routes = k.get_routes()
                #print(routes)
                #shapes = k.get_shapes(as_gdf=True)
                #print(shapes)
                #shapes.plot()
                #script = RScript(resources.files('mobility.transport_modes.public_transport').joinpath('routing_tests.R'))
                #script.run(
                #    args=[
                #        "None"
                #        ]
                #)
            if "SNCF" in agencies:
                print("SNCF found in ", gtfs_url)
            
            try:
                feed = gtfs_kit.read_feed(gtfs_url, dist_units='m')
            except Exception as e:
                print(f"Error in loading GTFS : {e}")

            # 1. Load the reference dataframes (make copies to avoid modifing the feed)
            # If shapes.txt et shape_id exist, load them :
            try:
                shapes_df = feed.shapes.copy()
                trips_df = feed.trips[['trip_id', 'route_id', 'shape_id']].copy()
            # Otherwise :
            except:
                trips_df = feed.trips[['trip_id', 'route_id']].copy()

            routes_df = feed.routes[['route_id', 'route_short_name', 'route_long_name']].copy()
            stops_df = feed.stops[['stop_id','stop_name','stop_lat', 'stop_lon']].copy()
            stop_times_df = feed.stop_times.copy()


            # 2. Filter active trips for the target date
            #active_trips contains all the trips for the target date
            active_trips = feed.get_trips(date=max_services_date)
            if active_trips.empty:
                print(f"No active trip for date {max_services_date}.")
            else:
                nb_trips = active_trips['trip_id'].count()
                print(f"{nb_trips}' trips found for date {max_services_date}")


            # 3A. If shape_id exists : group active trips by shape_id and count the number of trips for each shape_id
            if 'shape_id' in active_trips.columns:
                print("Shapes file present, counting trips")
                # Group active trips by shape_id and count the number of trips for each shape_id
                trips_counts = active_trips.groupby(['shape_id','route_id']).size().reset_index(name='trip_count')
                
                trips_total = trips_counts['trip_count'].sum()
                print(trips_total)
                # Remove trips with empty shape_id if they are still present
                trips_counts = trips_counts[trips_counts['shape_id'].notna()]
                
                # Enrich shapes_df with the number of trips per shape_id
                shapes_counts = shapes_df.merge(
                    trips_counts, 
                    on='shape_id', 
                    how='left' 
                    )
                
                # Replace NaN values of trip_count by 0
                shapes_counts['trip_count'] = shapes_counts['trip_count'].fillna(0)
                shapes_counts = shapes_counts[shapes_counts['trip_count'] > 0.0]
                
                # Enrich shapes with route_name
                shapes_enriched_final = shapes_counts.merge(
                    routes_df, 
                    on='route_id', 
                    how='left'
                    )
                nb_shapes = shapes_enriched_final['shape_id'].nunique()
                print("Shapes enriched with route names and trip counts")
                print(f"The network has {nb_shapes} different shapes with trips on {max_services_date}")

            # 3B. If shape_id doesn't exist : reconstruct the shapes
            else:
                print("Column 'shape_id' is missing in trips.txt, reconstructing shapes")
                # Join active_trips and stop_times_df to have the stop sequence for each trip 
                trips_stop_sequences = pd.merge(
                    active_trips[['trip_id','route_id']],
                    stop_times_df[['trip_id','stop_id','stop_sequence']],
                    on='trip_id',
                    how='left'
                )
                # Sort by trip_id and stop_sequence 
                trips_stop_sequences = trips_stop_sequences.sort_values(by=['trip_id','stop_sequence'])

                # Group by trip_id and concat stops_id in one unique string to rebuild a pseudo_shape_id
                trips_with_pseudo_shape_id = trips_stop_sequences.groupby(['trip_id']).agg(
                    pseudo_shape_id=('stop_id', lambda x: '-'.join(x.astype(str)))
                ).reset_index()

                # Add the pseudo_shape_id to trips_with_stop_sequence
                trips_stop_sequences = pd.merge(
                    trips_stop_sequences, 
                    trips_with_pseudo_shape_id, 
                    on='trip_id'
                    )
                    
                # Suppress all the non unique values to recreate a shapes df
                pseudo_shapes = trips_stop_sequences[[
                    'pseudo_shape_id', 
                    'stop_id', 
                    'stop_sequence',
                    'route_id'
                ]].drop_duplicates(subset=['pseudo_shape_id', 'stop_sequence'])

                # Get the lat and long from stops_df and add stop_coords to pseudo_shapes
                stops_coords = stops_df[['stop_id', 'stop_lat', 'stop_lon']].copy()
                stops_coords = stops_coords.rename(columns={'stop_lat': 'shape_pt_lat','stop_lon': 'shape_pt_lon'})
                pseudo_shapes = pd.merge(
                    pseudo_shapes, 
                    stops_coords, 
                    on='stop_id'
                )
                
                # Group active trips by shape_id and count the number of trips for each pseudo_shape_id
                trips_counts = trips_with_pseudo_shape_id.groupby(['pseudo_shape_id']).size().reset_index(name='trip_count')

                # Join the counting information 
                shapes_counts = pd.merge(
                    pseudo_shapes, 
                    trips_counts, 
                    on='pseudo_shape_id'
                )

                # Enrich shapes with route_name
                shapes_enriched_final = shapes_counts.merge(
                    routes_df, 
                    on='route_id', 
                    how='left'
                    )
                
                # Finalize the structure (similar to à shapes.txt)
                # Rename stop_sequence into shape_pt_sequence
                shapes_enriched_final = shapes_enriched_final.rename(
                    columns={'stop_sequence': 'shape_pt_sequence',
                            'pseudo_shape_id': 'shape_id'}
                )
                # Sort by shape_id and shape_pt_sequence
                shapes_enriched_final = shapes_enriched_final.sort_values(['shape_id', 'shape_pt_sequence'])
                nb_shapes = shapes_enriched_final['shape_id'].nunique()
                print("Shapes enriched with route names and trip counts")
                print(f"The network has {nb_shapes} different shapes with trips on {max_services_date}")

            # 4. Mapping the stops
            # Create map 
            fig_stops = px.scatter_map(
                stops_df, # Limité pour l'exemple
                lat="stop_lat",
                lon="stop_lon",
                hover_name="stop_name", # Nom affiché au survol
                color_discrete_sequence=["blue"],
                height=600
            )
            # Update map with OSM style
            fig_stops.update_layout(mapbox_style="open-street-map")
            print("Map of the stops")
            fig_stops.show() 

            # 5. Mapping the shapes with one color for each route
            # Create map
            fig_shapes = px.line_map(
                shapes_enriched_final,
                lat="shape_pt_lat",
                lon="shape_pt_lon",
                color="route_id",  
                line_group="shape_id",
                hover_name="route_short_name",
                height=600,
            )
            # Update map to only show hover_name in hover bubble
            fig_shapes.update_traces(
                # The name of the road is between <b> tags
                # <extra></extra> deletes the default lines in hover_data
                hovertemplate='<b>Ligne %{hovertext}</b><extra></extra>' 
            )
            # Update map with OSM style & no legend
            fig_shapes.update_layout(
                map_style="open-street-map",
                showlegend=False
            )
            print("Map of the shapes with one color for each route")
            fig_shapes.show()

            # 6. Mapping the shapes with the linewidth depending on the number of trips
            # Initial parameters
            min_weight = 1
            max_weight = 10
            max_trips = shapes_enriched_final['trip_count'].max()
            center_lat = shapes_enriched_final['shape_pt_lat'].mean()
            center_lon = shapes_enriched_final['shape_pt_lon'].mean()
            # Create map with initial parameters
            fig_shapes_count = go.Figure()
            fig_shapes_count.update_layout(
                map_style='open-street-map', 
                map_center_lat = center_lat,
                map_center_lon = center_lon,
                map_zoom = 9,
                margin={"r":0,"t":0,"l":0,"b":0},
                showlegend=False
            )
            # Trace each line with variable width
            for shape_id, group_df in shapes_enriched_final.groupby('shape_id'):
                # Each group_df represents a full line (with several shapes)
                # Get the statistics for each trip
                trip_count = group_df['trip_count'].iloc[0]
                route_name = group_df['route_short_name'].iloc[0]                
                # Calculate line width
                # Width = Min + (Max - Min) * (n_trips / max_trips)
                line_weight = min_weight + (max_weight- min_weight) * (trip_count / max_trips)                
                # Add the new line to the figure
                fig_shapes_count.add_trace(go.Scattermap(
                    mode="lines",
                    # Shapes of the line
                    lon=group_df['shape_pt_lon'],
                    lat=group_df['shape_pt_lat'],
                    # Style of the line
                    line=dict(
                        width=line_weight, 
                        color='#00529C'   
                    ),
                    # Hover bubble
                    name=f"Ligne {route_name}",
                    hovertemplate=f"Ligne {route_name}<br>Passages: {int(trip_count)}<extra></extra>"
                ))
            # Display map
            print("Map of the shapes with line width varying based on the number of trips")
            fig_shapes_count.show()


        #print(gtfs_files)

        