import gtfs_kit
import os
import pathlib
import json
import logging
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
        logging.info(gtfs_files)
        for gtfs_url in gtfs_files:
            logging.info("GTFS")
            gtfs=GTFSData(gtfs_url)
            agencies = gtfs.get_agencies_names(gtfs_url)
            logging.info(agencies)
            logging.info(type(agencies))
            for expected_agency in expected_agencies:
                logging.info(f'Looking for {expected_agency} in {gtfs.name}')
                if expected_agency.lower() in agencies.lower():
                    logging.info(f"{expected_agency} found in {gtfs.name}")
                    expected_agencies.remove(expected_agency)
        logging.info(expected_agencies)
        if expected_agencies == []:
            logging.info("All expected agencies were found")
            return True
        else:
            logging.info("Some agencies were not found in GTFS files.")
            logging.info(expected_agencies)
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
        """
        Used to audit and verify GTFS files.
        For each GTFS in the defined Transport Zones, the function :
        - finds the date with the max number of services
        - if file shapes.txt is absent, recreates shapes based on stop sequences
        - computes number of trips on each shape for the max services date
        - identifies active stops on the max services date
        - exports stops and shapes enriched with trip count and route name as a GeoPackage file
        """
                
        transport_zones = self.inputs["transport_zones"]
        stops = self.get_stops(transport_zones)
        gtfs_files = self.get_gtfs_files(stops)

        for i, gtfs_url in enumerate(gtfs_files, start=1):
            logging.info("GTFS")
            logging.info(gtfs_url)
            gtfs=GTFSData(gtfs_url)
            agencies = gtfs.get_agencies_names(gtfs_url)

            try:
                feed = gtfs_kit.read_feed(gtfs_url, dist_units='m')
            except Exception as e:
                logging.info(f"Error in loading GTFS : {e}")

            # 1. Load the reference dataframes (make copies to avoid modifing the feed)
            # If shapes.txt et shape_id exist, load them :
            try:
                shapes_df = feed.shapes.copy()
                trips_df = feed.trips[['trip_id', 'route_id', 'shape_id']].copy()
            # Otherwise :
            except:
                shapes_df = None
                trips_df = feed.trips[['trip_id', 'route_id']].copy()

            routes_df = feed.routes[['route_id', 'route_short_name', 'route_long_name']].copy()
            stops_df = feed.stops[['stop_id','stop_name','stop_lat', 'stop_lon']].copy()
            stop_times_df = feed.stop_times.copy()

            dates = feed.get_dates()
            max_services_date = feed.compute_busiest_date(dates)
            logging.info(f"Max services date is {max_services_date}")

            # 2. Filter active trips for the busiest date
            #active_trips contains all the trips for the busiest date
            active_trips = feed.get_trips(date=max_services_date)
            if active_trips.empty:
                logging.info(f"No active trips found for {max_services_date}.")
            else :
                logging.info(f"{len(active_trips)} trips found for {max_services_date}")

                # 3. Get the active shapes corresponding to the target date, count the trips for each shape and add route name
                # 3A. If shapes_df is not null : group active trips by shape_id and count the number of trips for each shape_id
                if shapes_df is not None and not shapes_df.empty:
                    logging.info("File shapes.txt is present in GTFS feed, counting trips...")
                    
                    # Group active trips by shape_id and count the number of trips for each shape_id
                    trips_counts = active_trips.groupby(['shape_id']).size().reset_index(name='trip_count')
                    
                    # This part is added to manage the case where the same shapes may have different route_ids 
                    # even though they are on the same route
                    # Retrieve a unique route_id corresponding to each shape_id
                    shapes_routes = active_trips.groupby(['shape_id'])['route_id'].first().reset_index()
                    
                    # Add route_id to trips_counts
                    trips_counts = trips_counts.merge(
                        shapes_routes,
                        on='shape_id',
                        how='left'
                    )

                    # Remove trips with empty shape_id if they are still present
                    trips_counts = trips_counts[trips_counts['shape_id'].notna()]

                    # Create active_shapes_df
                    active_shapes_df = shapes_df[shapes_df['shape_id'].isin(active_trips['shape_id'])]

                # 3B. If shape_id doesn't exist : reconstruct the shapes
                else:
                    logging.info("File shapes.txt is missing in GTFS feed, reconstructing shapes...")
                    
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
                    trips_with_pseudo_shape_id = trips_stop_sequences.groupby(['trip_id','route_id']).agg(
                        pseudo_shape_id=('stop_id', lambda x: '-'.join(x.astype(str)))
                    ).reset_index()

                    # Add the pseudo_shape_id to trips_with_stop_sequence
                    trips_stop_sequences = pd.merge(
                        trips_stop_sequences, 
                        trips_with_pseudo_shape_id[['trip_id','pseudo_shape_id']], 
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
                    
                    # Finalize the structure (similar to shapes.txt)
                    # Rename stop_sequence into shape_pt_sequence
                    active_shapes_df = pseudo_shapes.rename(
                        columns={'stop_sequence': 'shape_pt_sequence',
                                'pseudo_shape_id': 'shape_id'}
                    )
                    # Sort by shape_id and shape_pt_sequence
                    active_shapes_df = active_shapes_df.sort_values(['shape_id', 'shape_pt_sequence'])
                    
                    # Group active trips by shape_id and count the number of trips for each pseudo_shape_id
                    logging.info("Counting trips...")
                    trips_counts = trips_with_pseudo_shape_id.groupby(['pseudo_shape_id']).size().reset_index(name='trip_count')
                    
                    # This part is added to manage the case where the same pseudo_shapes may have different route_ids 
                    # even though they are on the same route
                    # Retrieve a unique route_id corresponding to each shape_id
                    shapes_routes = trips_with_pseudo_shape_id.groupby(['pseudo_shape_id'])['route_id'].first().reset_index()

                    # Add route_id to trips_counts
                    trips_counts = trips_counts.merge(
                        shapes_routes,
                        on='pseudo_shape_id',
                        how='left'
                    )

                    trips_counts = trips_counts.rename(columns={'pseudo_shape_id': 'shape_id'})

                # Add route names to trips_counts
                trips_counts = trips_counts.merge(
                    routes_df,
                    on='route_id',
                    how='left'
                )

                # 4. Get the active stops corresponding to the target date
                # Keep only stop times for active trips
                active_stop_times = stop_times_df[stop_times_df['trip_id'].isin(active_trips['trip_id'])]

                # Extract the corresponding active stop_ids
                active_stop_ids = active_stop_times['stop_id'].unique()

                # Filter stops_df to only keep active stops
                active_stops_df = stops_df[stops_df['stop_id'].isin(active_stop_ids)]

                # 5. Enrich active_shapes_df and export gpkg
                # Creating GeoDataFrames for shapes and stops
                active_shapes_gdf = gtfs_kit.shapes.geometrize_shapes(active_shapes_df)
                active_stops_gdf = gtfs_kit.stops.geometrize_stops(active_stops_df)

                # Enrich shapes with trip_counts, route_id and route_names
                logging.info('Enriching shapes with trip counts and route names...')
                active_shapes_gdf = active_shapes_gdf.merge(
                    trips_counts, 
                    on='shape_id', 
                    how='left' 
                    )

                # Print some data about number of trips
                nb_shapes = active_shapes_gdf['shape_id'].nunique()
                trips_total = active_shapes_gdf['trip_count'].sum()
                logging.info(f"The network has {nb_shapes} different shapes with a total of {trips_total} trips on {max_services_date}")

                # Replace NaN values of trip_count by 0
                active_shapes_gdf['trip_count'] = active_shapes_gdf['trip_count'].fillna(0)
                #active_shapes_gdf = active_shapes_gdf[active_shapes_gdf['trip_count'] > 0.0]
                    
                # Export shapes and stops to gpkg
                output_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs" / "gpkg" / f"gtfs_{i}.gpkg"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                active_shapes_gdf.to_file(output_path,driver="GPKG",layer="shapes")
                active_stops_gdf.to_file(output_path,driver="GPKG",layer="stops")

                # Print some info
                logging.info(f"GTFS stops and shapes exported as GeoPackage in file {output_path}")


            
        
