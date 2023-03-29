import pandas as pd
from pyproj import Proj, transform
from mobility.parsers import communes_data

def donnees_coordonnees_communes(input_path):
    ''' 
    This function convert coordinates Longitude-Latitude in Lambert-93 coordinates.
    The Lambert-93 projection is a map projection designed for metropolitan France.
    It is based on a reference ellipsoid called GRS80
    '''
    def transorm_Lat_Lon_to_xy(lat, long):
        outProj = Proj(init='epsg:2154')  # Projection for XY coordinates
        inProj = Proj(init='epsg:4326')  # Projection for Lat/Lon coordinates
        return transform(inProj, outProj, lat, long)

    # Reading data from CSV file using Pandas
    df = pd.read_csv(input_path, usecols=['nom_commune_postal', 'code_commune_INSEE', 'latitude', 'longitude'])

    # Creation of a new DataFrame to store the results of the transformation
    data = pd.DataFrame(columns=['nom_commune_postal', 'code_commune_INSEE', 'x', 'y'])

    # Copy the columns 'nom_commune_postal' and 'code_commune_INSEE' from the input DataFrame to the output DataFrame    data['nom_commune_postal'] = df['nom_commune_postal']
    data['code_commune_INSEE'] = df['code_commune_INSEE']

    # Apply the Lat/Lon -> XY transformation function to each row of the input DataFrame
    # Results are stored in the 'x' and 'y' columns of the output DataFrame                   
    data['x'],data['y']=df.apply(lambda row: pd.Series(transorm_Lat_Lon_to_xy(row['latitude'], row['longitude'])), axis=1, result_type='expand')                
                                                                                 

if __name__ == "__main__":

    input_path="mobility\data\insee\donnees_communes\donneesCommunes.csv"
    check_files = input_path.exists()
    if not (check_files):
        communes_data()  # Download the commune Data
    df=donnees_coordonnees_communes(input_path)
    # Save the updated data about the communities
    df.to_csv("examples\Millau\donnees_coordonnees_communes_updated",index=False)