import os
import pathlib
import logging
import zipfile
import pandas as pd
import pyarrow.parquet as pq
import geopandas as gpd

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file
from mobility.parsers.local_admin_units import LocalAdminUnits


class ShopsTurnoverDistribution(FileAsset):
    """
    This class processes and retrieves the spatial distribution of shop turnovers
    based on INSEE and BFS data for France and Switzerland.
    """

    def __init__(self):
        inputs = {}

        # Define cache paths for storing preprocessed data
        cache_path = {
            "shops_turnover": pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "shops_turnover.parquet"
        }

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        """
        Retrieves cached data if it exists.
        """
        logging.info(f"Using cached shops' turnover data from: {self.cache_path['shops_turnover']}")
        return pd.read_parquet(self.cache_path["shops_turnover"])

    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Creates and retrieves the shops' turnover spatial distribution.
        """
        shops_turnover_ratio = self.prepare_shops_turnover_ratio()
        shops_turnover_fr = self.prepare_french_shops_turnover_distribution(shops_turnover_ratio)
        shops_turnover_ch = self.prepare_swiss_shops_turnover_distribution(shops_turnover_ratio)

        # Combine datasets and save
        shops_turnover = pd.concat([shops_turnover_fr, shops_turnover_ch])
        shops_turnover = shops_turnover.dropna(subset=["local_admin_unit_id"])
        shops_turnover.to_parquet(self.cache_path["shops_turnover"])
        return shops_turnover

    def prepare_shops_turnover_ratio(self) -> pd.DataFrame:
        """
        Prepares turnover ratios based on business type and company category.
        """
        insee_data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        url_facilities_turnover = "https://www.insee.fr/fr/statistiques/fichier/7651349/DD_esane21ep_caracteristiques_taille.xlsx"
        facilities_turnover_path = insee_data_folder / "dd-esane22ep-caracteristiques-taille.xlsx"
        download_file(url_facilities_turnover, facilities_turnover_path)

        # Read turnover ratios from Excel file
        facilities_turnover_ratio = pd.read_excel(
            facilities_turnover_path, skiprows=11, usecols=[1, 2, 3, 4, 6, 9]
        )
        facilities_turnover_ratio.columns = [
            'naf_id', 'activity', 'size', 'n_companies', 'n_employees', 'turnover'
        ]

        # Convert monetary values and compute ratios
        facilities_turnover_ratio["turnover"] = pd.to_numeric(
            facilities_turnover_ratio["turnover"], errors="coerce"
        ) * 1e6

        facilities_turnover_ratio["n_employees"] = pd.to_numeric(
            facilities_turnover_ratio["n_employees"], errors="coerce"
        )
        facilities_turnover_ratio["n_companies"] = pd.to_numeric(
            facilities_turnover_ratio["n_companies"], errors="coerce"
        )

        facilities_turnover_ratio["turnover_by_employee"] = (
            facilities_turnover_ratio["turnover"] / facilities_turnover_ratio["n_employees"]
        )
        facilities_turnover_ratio["turnover_by_equipment"] = (
            facilities_turnover_ratio["turnover"] / facilities_turnover_ratio["n_companies"]
        )

        # Map company size categories
        size_mapping = {
            "Ensemble des catégories d'entreprise": "all",
            "Microentreprises (MICRO)": "micro",
            "Petites et moyennes entreprises (PME), hors Microentreprises": "pme",
            "Entreprises de taille intermédiaire (ETI) ou Grandes entreprises (GE)": "eti_ge"
        }
        facilities_turnover_ratio["size"] = facilities_turnover_ratio["size"].map(size_mapping)

        # Associate NAF codes with INSEE equipment
        insee_to_naf = self.prepare_insee_to_naf()
        insee_to_naf["naf_id"] = insee_to_naf["code_naf"].str.replace(".", "", regex=False).str[:3]
        facilities_turnover_ratio = pd.merge(insee_to_naf, facilities_turnover_ratio, on="naf_id", how="left")

        # Group by business type and compute average ratios
        facilities_turnover_ratio_grouped = facilities_turnover_ratio.groupby(
            ["code_equipement", "libelle_equipement", "size", "naf_id"]
        )[["turnover_by_employee", "turnover_by_equipment"]].mean().reset_index()

        # Filter for relevant shop-related categories
        shops_turnover_ratio = facilities_turnover_ratio_grouped[
            facilities_turnover_ratio_grouped["code_equipement"].str.startswith("B")
        ]
        shops_turnover_ratio = shops_turnover_ratio.dropna(subset=["turnover_by_employee", "turnover_by_equipment"])
        shops_turnover_ratio = shops_turnover_ratio[
            (shops_turnover_ratio["code_equipement"].str.startswith("B")) &
            (shops_turnover_ratio["size"] == "all")
        ]
        return shops_turnover_ratio

    def prepare_french_shops_turnover_distribution(self, shops_turnover_ratio) -> pd.DataFrame:
        """
        Prepares turnover data for shops in France.
        """

        insee_data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"

        # Download shop location data
        url = "https://www.insee.fr/fr/statistiques/fichier/8217525/BPE24.parquet"
        parquet_path = insee_data_folder / "BPE24.parquet"
        download_file(url, parquet_path)


        french_shops = pq.read_table(
            parquet_path
            )
        french_shops = french_shops.to_pandas()
        french_shops = french_shops.dropna(subset=["LONGITUDE"])
        french_shops = french_shops[french_shops["TYPEQU"].str.startswith("B")]


        french_shops_turnover = pd.merge(
            french_shops,
            shops_turnover_ratio,
            left_on="TYPEQU",
            right_on="code_equipement",
            how = "left"
            )

        french_shops_turnover = french_shops_turnover[[
            "DEPCOM", "naf_id", "LONGITUDE", "LATITUDE", "turnover_by_equipment"
            ]]
        french_shops_turnover.columns = ["local_admin_unit_id", "naf_id", "lon", "lat", "turnover"]
        french_shops_turnover["local_admin_unit_id"] = "fr-" + french_shops_turnover["local_admin_unit_id"]

        os.unlink(parquet_path)

        return french_shops_turnover

    def prepare_swiss_shops_turnover_distribution(self, shops_turnover_ratio) -> pd.DataFrame:
        """
        Prepares turnover data for shops in Switzerland.
        """

        bfs_data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs"

        # Download Swiss employment data
        url_statent = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/32258837/master"
        statent_zip_path = bfs_data_folder / "ag-b-00.03-22-STATENT2022.zip"
        download_file(url_statent, statent_zip_path)

        # Extract the archive
        with zipfile.ZipFile(statent_zip_path, "r") as zip_ref:
            zip_ref.extractall(bfs_data_folder)

        swiss_employees_colnames = pd.read_csv(
            bfs_data_folder / "ag-b-00.03-22-STATENT2022" / "STATENT_2022.csv",
            sep=";",
            index_col=0,
            nrows=0
        ).columns.tolist()
        selected_columns = [swiss_employees_colnames[i] for i in [1, 2, 3] + list(range(226, 311))]


        statent_path= bfs_data_folder / "ag-b-00.03-22-STATENT2022" / "STATENT_2022.csv"

        # Lire uniquement les colonnes sélectionnées
        swiss_employees = pd.read_csv(
            statent_path,
            sep=";",
            usecols=selected_columns
        )

        swiss_employees = swiss_employees.melt(id_vars=['E_KOORD', 'N_KOORD', 'RELI'])

        swiss_employees = swiss_employees[swiss_employees["value"] != 0]
        swiss_employees["NOGA"]  = swiss_employees["variable"].str.extract(r'B08(\d+)VZA')

        noga_to_naf = self.prepare_noga_to_naf()

        swiss_employees = pd.merge(swiss_employees, noga_to_naf, on = "NOGA")
        swiss_employees["naf_id"] = swiss_employees["NAF"].str.replace(".", "", regex=False).str[:3]

        insee_to_naf = self.prepare_insee_to_naf()
        insee_to_naf["naf_id"] = insee_to_naf["code_naf"].str.replace(".", "", regex=False).str[:3]
        swiss_employees = pd.merge(swiss_employees, insee_to_naf, on="naf_id", how = "left")
        swiss_employees = swiss_employees.dropna(subset=["code_equipement"])

        swiss_shop_employees = swiss_employees[swiss_employees["code_equipement"].str.startswith("B")]
        swiss_shops_turnover = pd.merge(
            swiss_shop_employees,
            shops_turnover_ratio,
            on=["code_equipement", "naf_id"],
            how = "left"
            )

        swiss_shops_turnover = swiss_shops_turnover.groupby(["E_KOORD", "N_KOORD", "RELI", "Description NOGA", "naf_id"]).agg({
            "value": "mean",
            "turnover_by_employee": "mean",
            "turnover_by_equipment": "mean"
        }).reset_index()

        swiss_shops_turnover["turnover"] = swiss_shops_turnover["turnover_by_employee"] * swiss_shops_turnover["value"]

        # Adjust point to the center of the grid
        grid_resolution = 100
        swiss_shops_turnover["E_KOORD_center"] = swiss_shops_turnover["E_KOORD"] + grid_resolution/2
        swiss_shops_turnover["N_KOORD_center"] = swiss_shops_turnover["N_KOORD"] + grid_resolution/2

        # transform in GeoDataFrame
        swiss_shops_turnover = gpd.GeoDataFrame(swiss_shops_turnover,
                               geometry=gpd.points_from_xy(
                                   swiss_shops_turnover["E_KOORD_center"],
                                   swiss_shops_turnover["N_KOORD_center"]),
                               crs="EPSG:2056"
                               )

        swiss_shops_turnover = swiss_shops_turnover.to_crs(epsg=3035)

        local_admin_units = LocalAdminUnits().get()
        swiss_shops_turnover = gpd.sjoin(swiss_shops_turnover, local_admin_units, how="left", predicate="within")
        swiss_shops_turnover = swiss_shops_turnover.dropna(subset=["local_admin_unit_id"])

        swiss_shops_turnover = swiss_shops_turnover.to_crs(epsg=4326)

        swiss_shops_turnover["lon"] = swiss_shops_turnover.geometry.x
        swiss_shops_turnover["lat"] = swiss_shops_turnover.geometry.y
        swiss_shops_turnover = pd.DataFrame(swiss_shops_turnover.drop(columns='geometry'))

        swiss_shops_turnover = swiss_shops_turnover[["local_admin_unit_id", "naf_id", "lon" , "lat", "turnover"]]

        os.unlink(statent_zip_path)
        os.unlink(statent_path)

        return swiss_shops_turnover


    def prepare_insee_to_naf(self):

        insee_to_naf = [
            # A1 - SERVICES PUBLICS
            ["A101", "Police", "84.24Z", "Activités d'ordre public et de sécurité"],
            ["A104", "Gendarmerie", "84.24Z", "Activités d'ordre public et de sécurité"],
            ["A105", "Cour d’appel (CA)", "84.23Z", "Justice"],
            ["A108", "Conseil de prud’hommes (CPH)", "84.23Z", "Justice"],
            ["A109", "Tribunal de commerce (TCO)", "84.23Z", "Justice"],
            ["A120", "DRFIP (Direction régionale des finances publiques)", "84.11Z", "Administration publique générale"],
            ["A121", "DDFIP (Direction départementale des finances publiques)", "84.11Z", "Administration publique générale"],
            ["A122", "Réseau de proximité Pôle emploi", "84.30Z", "Sécurité sociale obligatoire"],  # ou 78.10Z (placement)
            ["A124", "Maison de justice et du droit", "84.23Z", "Justice"],
            ["A125", "Antenne de justice", "84.23Z", "Justice"],
            ["A126", "Conseil départemental d’accès au droit (CDAD)", "84.23Z", "Justice"],
            ["A128", "Implantations France Services (IFS)", "84.11Z", "Administration publique générale"],
            ["A129", "Mairie", "84.11Z", "Administration publique générale"],
            ["A130", "Bureau d’aide juridictionnelle (BAJ)", "84.23Z", "Justice"],
            ["A131", "Tribunal judiciaire (TJ)", "84.23Z", "Justice"],
            ["A132", "Tribunal de proximité (TPRX)", "84.23Z", "Justice"],

            # A2 - SERVICES GÉNÉRAUX
            ["A203", "Banque, Caisse d’Épargne", "64.19Z", "Autres intermédiations monétaires"],
            ["A205", "Services funéraires", "96.03Z", "Services funéraires"],
            ["A206", "Bureau de poste", "53.10Z", "Activités de poste dans le cadre d'une obligation de service universel"],
            ["A207", "Relais poste", "53.20Z", "Autres activités de poste et de courrier"],
            ["A208", "Agence postale", "53.10Z", "Activités de poste dans le cadre d'une obligation de service universel"],

            # A3 - SERVICES AUTOMOBILES
            ["A301", "Réparation automobile et de matériel agricole", "45.20Z", "Entretien et réparation de véhicules automobiles"],
            ["A302", "Contrôle technique automobile", "71.20B", "Analyses, essais et inspections techniques"],
            ["A303", "Location d'automobiles et d'utilitaires légers", "77.11A", "Location de voitures et de véhicules automobiles légers"],
            ["A304", "École de conduite", "85.53Z", "Activités des écoles de conduite"],

            # A4 - ARTISANAT DU BÂTIMENT
            ["A401", "Maçon", "43.99C", "Travaux de maçonnerie générale et gros œuvre de bâtiment"],
            ["A402", "Plâtrier, peintre", "43.34Z", "Travaux de peinture et vitrerie"],
            ["A403", "Menuisier, charpentier, serrurier", "43.32B", "Menuiserie métallique et serrurerie"],  # NB : scindable selon la spécialité
            ["A404", "Plombier, couvreur, chauffagiste", "43.22A", "Travaux d'installation d'eau et de gaz en tous locaux"],
            ["A405", "Électricien", "43.21A", "Travaux d'installation électrique"],
            ["A406", "Entreprise générale du bâtiment", "41.20A", "Construction de bâtiments résidentiels et non résidentiels"],

            # A5 - AUTRES SERVICES À LA POPULATION
            ["A501", "Coiffure", "96.02A", "Coiffure"],
            ["A502", "Vétérinaire", "75.00Z", "Activités vétérinaires"],
            ["A503", "Agence de travail temporaire", "78.20Z", "Activités des agences de travail temporaire"],
            ["A504", "Restaurant, restauration rapide", "56.10C", "Restauration de type rapide"],  # ou 56.10A si restaurant traditionnel
            ["A505", "Agence immobilière", "68.31Z", "Agences immobilières"],
            ["A506", "Pressing, laverie automatique", "96.01B", "Blanchisserie-teinturerie de détail"],
            ["A507", "Institut de beauté, onglerie", "96.02B", "Soins de beauté"],

            # AR codes regroupés
            ["AR01", "Police, gendarmerie", "84.24Z", "Activités d'ordre public et de sécurité"],
            ["AR02", "Centre de finances publiques", "84.11Z", "Administration publique générale"],
            ["AR03", "Bureau de poste, relais poste, agence postale", "53.10Z", "Activités de poste dans le cadre d'une obligation de service universel"],

            # B1 - GRANDES SURFACES
            ["B101", "Hypermarché", "47.11F", "Hypermarchés"],
            ["B102", "Supermarché", "47.11D", "Supermarchés"],
            ["B103", "Grande surface de bricolage", "47.52A", "Commerce de détail de quincaillerie, peintures et verres"],
            ["B104", "Hypermarché et grand magasin", "47.19A", "Grands magasins"],
            ["B105", "Magasin multi-commerces", "47.11E", "Magasins multi-commerces"],

            # B2 - COMMERCES ALIMENTAIRES
            ["B201", "Supérette", "47.11B", "Commerce d'alimentation générale (surface < 400 m²)"],
            ["B202", "Épicerie", "47.11B", "Commerce d'alimentation générale"],
            ["B203", "Boulangerie, pâtisserie", "10.71", "Fabrication de pain et de pâtisserie fraîche"],
            ["B204", "Boucherie, charcuterie", "47.22Z", "Commerce de détail de viandes et produits à base de viande"],
            ["B205", "Produits surgelés", "47.29Z", "Autres commerces de détail alimentaires en magasin spécialisé"],
            ["B206", "Poissonnerie", "47.23Z", "Commerce de détail de poissons, crustacés et mollusques"],
            ["B207", "Fromagerie", "47.29Z", "Commerce de détail de produits laitiers"],
            ["B208", "Magasin de produits bio", "47.29Z", "Commerce de détail de produits biologiques en magasin spécialisé"],
            ["B209", "Magasin de vins et spiritueux", "47.25Z", "Commerce de détail de produits biologiques en magasin spécialisé"],
            ["B210", "Primeur", "47.21Z", "Commerce de détail de fruits et légumes en magasin spécialisé"],

            # B3 - COMMERCES SPÉCIALISÉS NON ALIMENTAIRES
            ["B301", "Librairie, papeterie, journaux", "47.61Z", "Commerce de détail de livres en magasin spécialisé"],  # ou scinder 47.62Z
            ["B302", "Magasin de vêtements", "47.71Z", "Commerce de détail d'habillement en magasin spécialisé"],
            ["B303", "Magasin d'équipements du foyer", "47.59B", "Commerce de détail d'autres équipements du foyer"],
            ["B304", "Magasin de chaussures", "47.72A", "Commerce de détail de la chaussure"],
            ["B305", "Magasin d'électroménager et de mat. audio-vidéo", "47.54Z", "Commerce de détail d'appareils électroménagers"],
            ["B306", "Magasin de meubles", "47.59A", "Commerce de détail de meubles"],
            ["B307", "Magasin d'articles de sports et de loisirs", "47.64Z", "Commerce de détail d'articles de sport en magasin spécialisé"],
            ["B308", "Magasin de revêtements murs et sols", "47.52B", "Commerce de détail de quincaillerie, peintures et verres"],
            ["B309", "Droguerie, quincaillerie, bricolage (<400 m²)", "47.52B", "Commerce de détail de quincaillerie, peintures et verres"],
            ["B310", "Parfumerie, cosmétique", "47.75Z", "Commerce de détail de parfumerie et de produits de beauté"],
            ["B311", "Horlogerie, bijouterie", "47.77Z", "Commerce de détail d'articles d'horlogerie et de bijouterie"],
            ["B312", "Fleuriste, jardinerie, animalerie", "47.76Z", "Commerce de détail de fleurs, plantes, graines, animaux de compagnie"],
            ["B313", "Magasin d'optique", "47.78A", "Commerce de détail d'optique"],
            ["B315", "Magasin de matériel médical et orthopédique", "47.74Z", "Commerce de détail d'articles médicaux et orthopédiques"],
            ["B316", "Station-service", "47.30Z", "Commerce de détail de carburants en magasin spécialisé"],
            ["B317", "Commerce de tissus et mercerie", "47.51Z", "Commerce de détail de textiles en magasin spécialisé"],
            ["B318", "Commerce de jeux et jouets", "47.65Z", "Commerce de détail de jeux et jouets en magasin spécialisé"],
            ["B319", "Maroquinerie et articles de voyage", "47.72B", "Commerce de détail de maroquinerie et d'articles de voyage"],
            ["B320", "Commerce de combustible domestique", "47.78C", "Autres commerces de détail spécialisés divers"],
            ["B321", "Magasin d'électroménager et matériel audio, vidéo, informatique", "47.54Z", "Commerce de détail d'appareils électroménagers"],
            ["B322", "Commerce de matériel de télécommunications", "47.42Z", "Commerce de détail de matériels de télécommunication"],
            ["B323", "Commerce de biens d’occasion", "47.79Z", "Commerce de détail de biens d'occasion en magasin"],
            ["B324", "Librairie", "47.61Z", "Commerce de détail de livres en magasin spécialisé"],
            ["B325", "Papeterie et presse", "47.62Z", "Commerce de détail de journaux et papeterie en magasin spécialisé"],

            # BR01, BR02
            ["BR01", "Épicerie, supérette", "47.11B", "Commerce d'alimentation générale"],
            ["BR02", "Droguerie, quincaillerie, bricolage", "47.52B", "Commerce de détail de quincaillerie, peintures et verres"],

            # C - ENSEIGNEMENT
            ["C101", "École maternelle", "85.10Z", "Enseignement pré-primaire"],
            ["C102", "École maternelle RPI dispersé", "85.10Z", "Enseignement pré-primaire"],
            ["C104", "École élémentaire", "85.20Z", "Enseignement primaire"],
            ["C105", "École élémentaire RPI dispersé", "85.20Z", "Enseignement primaire"],
            ["C201", "Collège", "85.31Z", "Enseignement secondaire général"],

            # C3 ENSEIGNEMENT DU SECOND DEGRÉ SECOND CYCLE
            ["C301", "Lycée d'enseignement général et/ou technologique", "85.31Z", "Enseignement secondaire général"],
            ["C302", "Lycée d'enseignement professionnel", "85.32Z", "Enseignement secondaire technique ou professionnel"],
            ["C303", "Lycée d’enseignement technique et/ou professionnel agricole", "85.32Z", "Enseignement secondaire technique ou professionnel"],
            ["C304", "SGT (Section d’enseignement général et technologique en lycée pro)", "85.31Z", "Enseignement secondaire général"],
            ["C305", "SEP (Section d’enseignement professionnel en lycée général/technologique)", "85.32Z", "Enseignement secondaire technique ou professionnel"],

             # C4 ENSEIGNEMENT SUPÉRIEUR NON UNIVERSITAIRE
            ["C401", "STS (Section technicien supérieur), CPGE", "85.42Z", "Enseignement supérieur"],
            ["C402", "Formation santé", "85.42Z", "Enseignement supérieur"],
            ["C403", "Formation commerce", "85.42Z", "Enseignement supérieur"],
            ["C409", "Autre formation post bac non universitaire", "85.42Z", "Enseignement supérieur"],

            # C5 ENSEIGNEMENT SUPÉRIEUR UNIVERSITAIRE
            ["C501", "UFR (Unité de formation et de recherche)", "85.42Z", "Enseignement supérieur"],
            ["C502", "Institut universitaire (IUP, IUT…)", "85.42Z", "Enseignement supérieur"],
            ["C503", "École d’ingénieurs", "85.42Z", "Enseignement supérieur"],
            ["C504", "Enseignement général supérieur privé", "85.42Z", "Enseignement supérieur"],
            ["C505", "École d’enseignement supérieur agricole", "85.42Z", "Enseignement supérieur"],
            ["C509", "Autre enseignement supérieur", "85.42Z", "Enseignement supérieur"],

            # C6 FORMATION CONTINUE
            ["C602", "GRETA", "85.59A", "Formation continue d’adultes"],
            ["C603", "Centre dispensant de la formation continue agricole", "85.59A", "Formation continue d’adultes"],
            ["C604", "Formation aux métiers du sport (INSEP, CREPS…)", "85.51Z", "Enseignement de disciplines sportives et d’activités de loisirs"],
            ["C605", "Centre dispensant des formations d’apprentissage agricole", "85.32Z", "Enseignement secondaire technique ou professionnel"],
            ["C609", "Autre formation continue", "85.59A", "Formation continue d’adultes"],

            # C7 AUTRES SERVICES DE L’ÉDUCATION
            ["C701", "Résidence universitaire", "55.90Z", "Autres hébergements"],
            ["C702", "Restaurant universitaire", "56.29A", "Restauration collective sous contrat"],
            ["C601", "Centre de formation d'apprentis (hors agriculture)", "85.32Z", "Enseignement secondaire technique ou professionnel"],

            # D - SANTÉ ET ACTION SOCIALE
            # D1 ÉTABLISSEMENTS ET SERVICES DE SANTÉ
            ["D101", "Établissement de santé de court séjour", "86.10Z", "Activités hospitalières"],
            ["D102", "Établissement de santé de moyen séjour", "86.10Z", "Activités hospitalières (soins de suite/réadaptation)"],
            ["D103", "Établissement de santé de long séjour", "86.10Z", "Activités hospitalières (long séjour)"],
            ["D104", "Établissement psychiatrique", "86.10Z", "Activités hospitalières (psychiatrie)"],
            ["D105", "Centre de lutte contre le cancer", "86.10Z", "Activités hospitalières (spécialisées)"],
            ["D106", "Urgences", "86.10Z", "Activités hospitalières (service d'urgence)"],
            ["D107", "Maternité", "86.10Z", "Activités hospitalières (obstétrique)"],
            ["D108", "Centre de santé", "86.90", "Autres activités pour la santé humaine"],
            ["D109", "Structures psychiatriques en ambulatoire", "86.90", "Autres activités pour la santé humaine"],
            ["D110", "Centre médecine préventive", "86.90", "Autres activités pour la santé humaine"],
            ["D111", "Dialyse", "86.10Z", "Activités hospitalières (dialyse)"],
            ["D112", "Hospitalisation à domicile", "86.10Z", "Activités hospitalières (HAD)"],
            ["D113", "Maisons de santé pluridisciplinaire", "86.90", "Autres activités pour la santé humaine"],

            # D2 PROFESSIONS LIBÉRALES DE SANTÉ
            ["D201", "Médecin généraliste", "86.21Z", "Activité des médecins généralistes"],
            ["D202", "Spécialiste en cardiologie", "86.22Z", "Activité des médecins spécialistes"],
            ["D203", "Spécialiste en dermatologie et vénéréologie", "86.22Z", "Activité des médecins spécialistes"],
            ["D206", "Spécialiste en gastro-entérologie, hépatologie", "86.22Z", "Activité des médecins spécialistes"],
            ["D207", "Spécialiste en psychiatrie", "86.22Z", "Activité des médecins spécialistes"],
            ["D208", "Spécialiste en ophtalmologie", "86.22Z", "Activité des médecins spécialistes"],
            ["D209", "Spécialiste en oto-rhino-laryngologie", "86.22Z", "Activité des médecins spécialistes"],
            ["D210", "Spécialiste en pédiatrie", "86.22Z", "Activité des médecins spécialistes"],
            ["D211", "Spécialiste en pneumologie", "86.22Z", "Activité des médecins spécialistes"],
            ["D212", "Spécialiste en radiodiagnostic et imagerie médicale", "86.22Z", "Activité des médecins spécialistes (radiologie)"],
            ["D213", "Spécialiste en stomatologie", "86.22Z", "Activité des médecins spécialistes"],
            ["D214", "Spécialiste en gynécologie", "86.22Z", "Activité des médecins spécialistes"],
            ["D221", "Chirurgien dentiste", "86.23Z", "Pratique dentaire"],
            ["D231", "Sage-femme", "86.90", "Autres activités pour la santé humaine"],
            ["D232", "Infirmier (ADELI)", "86.90", "Autres activités pour la santé humaine"],
            ["D233", "Masseur kinésithérapeute", "86.90", "Autres activités pour la santé humaine"],
            ["D235", "Orthophoniste", "86.90", "Autres activités pour la santé humaine"],
            ["D236", "Orthoptiste", "86.90", "Autres activités pour la santé humaine"],
            ["D237", "Pédicure, podologue", "86.90", "Autres activités pour la santé humaine"],
            ["D238", "Audioprothésiste", "86.90", "Autres activités pour la santé humaine"],
            ["D239", "Ergothérapeute", "86.90", "Autres activités pour la santé humaine"],
            ["D240", "Psychomotricien", "86.90", "Autres activités pour la santé humaine"],
            ["D242", "Diététicien", "86.90", "Autres activités pour la santé humaine"],
            ["D243", "Psychologue", "86.90", "Autres activités pour la santé humaine"],
            ["D244", "Infirmier", "86.90", "Autres activités pour la santé humaine"],

            # D3 AUTRES ÉTABLISSEMENTS ET SERVICES À CARACTÈRE SANITAIRE
            ["D302", "Laboratoire d'analyses et de biologie médicale", "86.90B", "Laboratoires d'analyses médicales"],
            ["D303", "Ambulance", "86.90", "Autres activités pour la santé humaine (ambulances)"],
            ["D304", "Transfusion sanguine", "86.10Z", "Activités hospitalières (EFS)"],
            ["D305", "Établissement thermal", "96.04Z", "Entretien corporel (ou 86.90 si purement soins)"],
            ["D307", "Pharmacie", "47.73Z", "Commerce de détail de produits pharmaceutiques"],

            # D4 ACTION SOCIALE POUR PERSONNES ÂGÉES
            ["D401", "Personnes âgées : hébergement", "87.30A", "Hébergement social pour personnes âgées"],
            ["D402", "Personnes âgées : soins à domicile", "88.10A", "Aide à domicile"],
            ["D403", "Personnes âgées : services d'aide", "88.10A", "Aide à domicile"],

            # D5 ACTION SOCIALE POUR ENFANTS EN BAS ÂGE
            ["D502", "Établissement d’accueil du jeune enfant", "88.91A", "Accueil de jeunes enfants"],
            ["D503", "Lieu d’accueil enfants-parents", "88.91A", "Accueil de jeunes enfants"],
            ["D504", "Relais petite enfance", "88.91A", "Accueil de jeunes enfants"],
            ["D505", "Accueil de loisirs sans hébergement", "93.29Z", "Autres activités récréatives et de loisirs"],
            ["D506", "Centres sociaux", "88.99B", "Action sociale sans hébergement n.c.a."],

            # D6 ACTION SOCIALE POUR HANDICAPÉS
            ["D601", "Enfants handicapés : hébergement", "87.20A", "Hébergement social pour enfants handicapés"],
            ["D602", "Enfants handicapés : services à domicile ou ambulatoires", "88.10B", "Aide à domicile"],
            ["D603", "Adultes handicapés : accueil, hébergement", "87.30B", "Hébergement social pour adultes handicapés"],
            ["D604", "Adultes handicapés : services d’aide", "88.10B", "Aide à domicile"],
            ["D605", "Travail protégé (ESAT, etc.)", "88.99B", "Action sociale sans hébergement n.c.a."],
            ["D606", "Adultes handicapés : services de soins à domicile", "88.10B", "Aide à domicile"],

            # D7 AUTRES SERVICES D’ACTION SOCIALE
            ["D701", "Protection de l’enfance hébergement", "87.90A", "Hébergement social pour enfants en difficultés"],
            ["D702", "Protection de l’enfance : action éducative", "88.99B", "Autre action sociale sans hébergement n.c.a."],
            ["D703", "CHRS (Centre d’hébergement et de réinsertion sociale)", "88.99B", "Autre action sociale sans hébergement n.c.a."],
            ["D704", "Centre provisoire d’hébergement (CPH)", "87.90B", "Hébergement social pour adultes en difficulté"],
            ["D705", "Centre accueil demandeur d’asile (CADA)", "87.90B", "Hébergement social pour adultes en difficulté"],
            ["D709", "Autres établissements pour adultes et familles en difficulté", "87.90B", "Hébergement social pour adultes en difficultés"],

            # E TRANSPORTS ET DÉPLACEMENTS
            ["E101", "Taxi, VTC", "49.32Z", "Transports de voyageurs par taxis"],
            ["E102", "Aéroport", "52.23Z", "Services auxiliaires des transports aériens"],
            ["E107", "Gare de voyageurs d'intérêt national", "52.21Z", "Services auxiliaires des transports terrestres"],
            ["E108", "Gare de voyageurs d'intérêt régional", "52.21Z", "Services auxiliaires des transports terrestres"],
            ["E109", "Gare de voyageurs d'intérêt local", "52.21Z", "Services auxiliaires des transports terrestres"],

            # F1 ÉQUIPEMENTS SPORTIFS
            ["F101", "Bassin de natation", "93.11Z", "Gestion d'installations sportives"],
            ["F102", "Boulodrome", "93.11Z", "Gestion d'installations sportives"],
            ["F103", "Tennis", "93.11Z", "Gestion d'installations sportives"],
            ["F104", "Équipement de cyclisme (vélodrome...)", "93.11Z", "Gestion d'installations sportives"],
            ["F105", "Domaine skiable", "93.29Z", "Autres activités récréatives et de loisirs"],
            ["F106", "Centre équestre", "93.19Z", "Autres activités sportives"],
            ["F107", "Athlétisme", "93.11Z", "Gestion d'installations sportives"],
            ["F108", "Terrain de golf", "93.11Z", "Gestion d'installations sportives"],
            ["F109", "Parcours sportif/santé", "93.11Z", "Gestion d'installations sportives"],
            ["F110", "Sports de glace (patinoire...)", "93.11Z", "Gestion d'installations sportives"],
            ["F111", "Plateaux et terrains de jeux extérieurs", "93.11Z", "Gestion d'installations sportives"],
            ["F112", "Salles spécialisées (basket, volley, etc.)", "93.11Z", "Gestion d'installations sportives"],
            ["F113", "Terrain de grands jeux (foot, rugby...)", "93.11Z", "Gestion d'installations sportives"],
            ["F114", "Salles de combat", "93.11Z", "Gestion d'installations sportives"],
            ["F116", "Salles non spécialisées", "93.11Z", "Gestion d'installations sportives"],
            ["F117", "Roller, skate, vélo bicross ou freestyle", "93.11Z", "Gestion d'installations sportives"],
            ["F118", "Sports nautiques", "93.11Z", "Gestion d'installations sportives"],
            ["F119", "Bowling", "93.11Z", "Gestion d'installations sportives (ou 93.29Z)"],
            ["F120", "Salles de remise en forme", "93.13Z", "Activités des centres de fitness"],
            ["F121", "Salles multisports (gymnases)", "93.11Z", "Gestion d'installations sportives"],

            # F2 ÉQUIPEMENTS DE LOISIRS
            ["F201", "Baignade aménagée", "93.29Z", "Autres activités récréatives et de loisirs"],
            ["F202", "Port de plaisance – mouillage", "93.29Z", "Autres activités récréatives et de loisirs"],
            ["F203", "Boucle de randonnée", "93.29Z", "Autres activités récréatives et de loisirs"],

            # F3 ÉQUIPEMENTS CULTURELS ET SOCIOCULTURELS
            ["F303", "Cinéma", "59.14Z", "Projection de films cinématographiques"],
            ["F305", "Conservatoire", "85.52Z", "Enseignement culturel"],
            ["F307", "Bibliothèque", "91.01Z", "Gestion de bibliothèques et archives"],
            ["F311", "Livres et presse (labellisés LIR...)", "47.61Z", "Commerce de détail de livres"],
            ["F312", "Exposition et médiation culturelle", "91.02Z", "Gestion des musées"],
            ["F313", "Espace remarquable et patrimoine", "91.03Z", "Gestion des sites et monuments historiques"],
            ["F314", "Archives", "91.01Z", "Gestion de bibliothèques et archives"],
            ["F315", "Arts du spectacle", "90.01Z", "Arts du spectacle vivant"],

            # G TOURISME
            ["G101", "Agence de voyage", "79.11Z", "Activités des agences de voyage"],
            ["G102", "Hôtel", "55.10Z", "Hôtels et hébergement similaire"],
            ["G103", "Camping", "55.30Z", "Terrains de camping et parcs pour caravanes ou véhicules de loisirs"],
            ["G104", "Information touristique", "79.90Z", "Autres services de réservation et activités connexes"],
        ]

        # Création du DataFrame
        insee_to_naf = pd.DataFrame(
            insee_to_naf,
            columns=["code_equipement", "libelle_equipement", "code_naf", "libelle_naf"]
            )

        return(insee_to_naf)

    def prepare_noga_to_naf(self):

        # Liste des données NOGA (Suisse) → NAF
        data_noga_naf = [
            ["01", "Culture et production animale, chasse et services annexes", "01.10", "Culture et production animale"],
            ["02", "Sylviculture et exploitation forestière", "02.10", "Sylviculture et exploitation forestière"],
            ["03", "Pêche et aquaculture", "03.11", "Pêche et aquaculture"],
            ["05", "Extraction de houille et de lignite", "05.10", "Extraction de houille"],
            ["06", "Extraction d'hydrocarbures", "06.10", "Extraction d'hydrocarbures"],
            ["07", "Extraction de minerais métalliques", "07.10", "Extraction de minerais métalliques"],
            ["08", "Autres industries extractives", "08.11", "Autres industries extractives"],
            ["09", "Services de soutien aux industries extractives", "09.10", "Services de soutien aux industries extractives"],
            ["10", "Industries alimentaires", "10.11", "Industries alimentaires"],
            ["11", "Fabrication de boissons", "11.01", "Fabrication de boissons"],
            ["12", "Fabrication de produits à base de tabac", "12.00", "Fabrication de produits à base de tabac"],
            ["13", "Fabrication de textiles", "13.10", "Fabrication de textiles"],
            ["14", "Industrie de l'habillement", "14.11", "Industrie de l'habillement"],
            ["15", "Industrie du cuir et de la chaussure", "15.11", "Industrie du cuir et de la chaussure"],
            ["16", "Travail du bois et fabrication d'articles en bois et liège", "16.10", "Travail du bois et fabrication d'articles en bois et liège"],
            ["17", "Industrie du papier et du carton", "17.11", "Industrie du papier et du carton"],
            ["18", "Imprimerie et reproduction d'enregistrements", "18.11", "Imprimerie et reproduction d'enregistrements"],
            ["19", "Cokéfaction et raffinage", "19.10", "Cokéfaction et raffinage"],
            ["20", "Industrie chimique", "20.11", "Industrie chimique"],
            ["21", "Industrie pharmaceutique", "21.10", "Industrie pharmaceutique"],
            ["22", "Fabrication de produits en caoutchouc et plastique", "22.11", "Fabrication de produits en caoutchouc et plastique"],
            ["23", "Fabrication d'autres produits minéraux non métalliques", "23.11", "Fabrication d'autres produits minéraux non métalliques"],
            ["24", "Métallurgie", "24.10", "Métallurgie"],
            ["25", "Fabrication de produits métalliques", "25.11", "Fabrication de produits métalliques"],
            ["26", "Fabrication de produits informatiques, électroniques et optiques", "26.11", "Fabrication de produits informatiques, électroniques et optiques"],
            ["27", "Fabrication d'équipements électriques", "27.11", "Fabrication d'équipements électriques"],
            ["28", "Fabrication de machines et équipements", "28.11", "Fabrication de machines et équipements"],
            ["29", "Industrie automobile", "29.10", "Industrie automobile"],
            ["30", "Fabrication d'autres matériels de transport", "30.11", "Fabrication d'autres matériels de transport"],
            ["31", "Fabrication de meubles", "31.01", "Fabrication de meubles"],
            ["32", "Autres industries manufacturières", "32.11", "Autres industries manufacturières"],
            ["33", "Réparation et installation de machines et équipements", "33.11", "Réparation et installation de machines et équipements"],
            ["35", "Production et distribution d'électricité, gaz, vapeur", "35.11", "Production et distribution d'électricité, de gaz, de vapeur"],
            ["36", "Captage, traitement et distribution d'eau", "36.00", "Captage, traitement et distribution d'eau"],
            ["37", "Collecte et traitement des eaux usées", "37.00", "Collecte et traitement des eaux usées"],
            ["38", "Collecte, traitement et élimination des déchets", "38.11", "Collecte, traitement et élimination des déchets"],
            ["39", "Dépollution et autres services de gestion des déchets", "39.00", "Dépollution et autres services de gestion des déchets"],

            ["40", "Non utilisé dans NOGA", "Non applicable", "Non applicable"],
            ["41", "Construction de bâtiments", "41.10", "Construction de bâtiments"],
            ["42", "Génie civil", "42.10", "Génie civil"],
            ["43", "Travaux de construction spécialisés", "43.11", "Travaux de construction spécialisés"],
            ["45", "Commerce et réparation d'automobiles et de motocycles", "45.11", "Commerce et réparation d'automobiles et de motocycles"],
            ["46", "Commerce de gros, hors automobiles et motocycles", "46.11", "Commerce de gros, hors automobiles et motocycles"],
            ["47", "Commerce de détail, hors automobiles et motocycles", "47.11", "Commerce de détail, hors automobiles et motocycles"],

            ["48", "Non utilisé dans NOGA", "Non applicable", "Non applicable"],
            ["49", "Transports terrestres et transport par conduites", "49.10", "Transports terrestres et transport par conduites"],
            ["50", "Transports par eau", "50.10", "Transports par eau"],
            ["51", "Transports aériens", "51.10", "Transports aériens"],
            ["52", "Entreposage et services auxiliaires des transports", "52.10", "Entreposage et services auxiliaires des transports"],
            ["53", "Activités de poste et de courrier", "53.10", "Activités de poste et de courrier"],
            ["55", "Hébergement", "55.10", "Hébergement"],
            ["56", "Restauration", "56.10", "Restauration"],
            ["58", "Édition", "58.10", "Édition"],
            ["59", "Production de films et enregistrements sonores", "59.11", "Production de films et enregistrements sonores"],
            ["60", "Programmation et diffusion", "60.10", "Programmation et diffusion"],
            ["61", "Télécommunications", "61.10", "Télécommunications"],
            ["62", "Programmation et conseil informatique", "62.10", "Programmation et conseil informatique"],
            ["63", "Services d'information", "63.10", "Services d'information"],
            ["64", "Activités des services financiers, hors assurance et caisses de retraite", "64.10", "Activités des services financiers, hors assurance et caisses de retraite"],
            ["65", "Assurance", "65.10", "Assurance"],
            ["66", "Activités auxiliaires de services financiers et d'assurance", "66.10", "Activités auxiliaires de services financiers et d'assurance"],
            ["68", "Activités immobilières", "68.10", "Activités immobilières"],
            ["69", "Activités juridiques et comptables", "69.10", "Activités juridiques et comptables"],
            ["70", "Activités des sièges sociaux et conseil de gestion", "70.10", "Activités des sièges sociaux et conseil de gestion"],
            ["71", "Activités d'architecture et d'ingénierie", "71.10", "Activités d'architecture et d'ingénierie"],
            ["72", "Recherche et développement scientifique", "72.10", "Recherche et développement scientifique"],
            ["73", "Publicité et études de marché", "73.10", "Publicité et études de marché"],
            ["74", "Autres activités spécialisées, scientifiques et techniques", "74.10", "Autres activités spécialisées, scientifiques et techniques"],
            ["75", "Activités vétérinaires", "75.00", "Activités vétérinaires"],
            ["77", "Activités de location et location-bail", "77.10", "Activités de location et location-bail"],
            ["78", "Activités liées à l'emploi", "78.10", "Activités liées à l'emploi"],
            ["79", "Activités des agences de voyage et voyagistes", "79.10", "Activités des agences de voyage et voyagistes"],
            ["80", "Enquêtes et sécurité", "80.10", "Enquêtes et sécurité"],
            ["81", "Services relatifs aux bâtiments et aménagement paysager", "81.10", "Services relatifs aux bâtiments et aménagement paysager"],
            ["82", "Activités administratives et autres activités de soutien aux entreprises", "82.10", "Activités administratives et autres activités de soutien aux entreprises"],
            ["84", "Administration publique et défense; sécurité sociale obligatoire", "84.10", "Administration publique et défense; sécurité sociale obligatoire"],
            ["85", "Enseignement", "85.10", "Enseignement"],
            ["86", "Activités pour la santé humaine", "86.10", "Activités pour la santé humaine"],
            ["87", "Hébergement médico-social et social", "87.10", "Hébergement médico-social et social"],
            ["88", "Action sociale sans hébergement", "88.10", "Action sociale sans hébergement"],
            ["90", "Activités créatives, artistiques et de spectacle", "90.10", "Activités créatives, artistiques et de spectacle"],
            ["91", "Bibliothèques, archives, musées et autres activités culturelles", "91.10", "Bibliothèques, archives, musées et autres activités culturelles"],
            ["92", "Organisation de jeux de hasard et d'argent", "92.00", "Organisation de jeux de hasard et d'argent"],
            ["93", "Activités sportives, récréatives et de loisirs", "93.10", "Activités sportives, récréatives et de loisirs"],
            ["94", "Activités des organisations associatives", "94.10", "Activités des organisations associatives"],
            ["95", "Réparation d'ordinateurs et de biens personnels et domestiques", "95.10", "Réparation d'ordinateurs et de biens personnels et domestiques"],
            ["96", "Autres services personnels", "96.10", "Autres services personnels"],
            ["97", "Activités des ménages en tant qu'employeurs de personnel domestique", "97.00", "Activités des ménages en tant qu'employeurs de personnel domestique"],
            ["98", "Activités des ménages en tant que producteurs de biens et services pour usage propre", "98.10", "Activités des ménages en tant que producteurs de biens et services pour usage propre"],
            ["99", "Activités des organisations et organismes extraterritoriaux", "99.00", "Activités des organisations et organismes extraterritoriaux"],
        ]

        # Création du DataFrame pandas pour les nouvelles données
        df_noga_naf = pd.DataFrame(
            data_noga_naf,
            columns=["NOGA", "Description NOGA", "NAF", "Description NAF"]
        )

        return(df_noga_naf)
