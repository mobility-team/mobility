from dash import Dash, html, Input, Output, State, callback, dcc, Patch, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import json
import pandas as pd
import layout





#Importation des fichiers json pour les contenus et la traduction
with open('interface_content.json', 'r') as file:
    app_content = json.load(file)
    
with open('translations_content.json', 'r') as file:
    app_translation = json.load(file)

#Import des csv listes de communes et département
df_municipality = pd.read_csv('data\donneesCommunesFrance.csv')
df_county = pd.read_csv('data\departements-france.csv')

#Transformation des listes pour l'affichage
list_municipality = df_municipality['NOM_COM'].tolist()
list_municipality.sort()

list_county = [a+' ('+b+')' for a,b in zip(df_county['nom_departement'].tolist(), df_county['code_departement'].tolist())]
list_county.sort()

#Démarrage de l'application
app = Dash(suppress_callback_exceptions=True)

#Démarrage de l'affichage de l'application (côté client)
app.layout = html.Div(children=layout.Layout('fr', app_translation[app_content['box mobility']['logo']['label']]), id="global_container")



@callback(
    Output(app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], "options"),
    Input(app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], "search_value")
)
def update_multi_options(search_value : str) -> list:
    """
    Charge les possibilités de listes déroulantes et évite de charger toutes 
    les communes pour éviter les lags et gère donc la recherche
    
    Paramètres :
        search_value (str) : Valeur de la recherche
    """
    
    #Empêcher un chargement si aucune valeur n'a encore été rentrée pour éviter les lags
    if not search_value:
        raise PreventUpdate
        
    #Création de la listes de résultats de la recherche
    return [
        o for o in list_municipality if o.lower().startswith(search_value.lower())
    ]



@callback(
    Output(app_content['div_box']['box study area']['choice']['municipality']['municipality input area']['id'], "options"),
    Input(app_content['div_box']['box study area']['choice']['municipality']['municipality input area']['id'], "search_value")
)
def update_options(search_value : str) -> list:
    """
    Charge les possibilités de listes déroulantes et évite de charger toutes 
    les communes pour éviter les lags et gère donc la recherche
    
    Paramètres :
        search_value (str) : Valeur de la recherche
    """
    
    #Empêcher un chargement si aucune valeur n'a encore été rentrée pour éviter les lags
    if not search_value:
        raise PreventUpdate
        
    #Création de la listes de résultats de la recherche
    return [
        o for o in list_municipality if o.lower().startswith(search_value.lower())
    ]



#Callback Listes communes. Pas d'affichage au début pour éviter les ralentissements (ne pas charger toutes les communes de France)
@callback(
    Output(app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'], "options"),
    Input(app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'], "search_value"),
    State(app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'], "value")
)
def update_multi_options(search_value : str, value : str) -> list:
    """
    Charge les possibilités de listes déroulantes et évite de charger toutes 
    les communes pour éviter les lags et gère donc la recherche
    
    Paramètres :
        search_value (str) : Valeur de la recherche
        value (str) : Valeurs déjà sélectionnées
    """
    
    #Empêcher un chargement si aucune valeur n'a encore été rentrée pour éviter les lags
    if not search_value:
        raise PreventUpdate
        
    #Création de la listes de résultats de la recherche
    return [
        o for o in list_municipality if o.lower().startswith(search_value.lower()) or o in (value or [])
    ]



#Callback Listes communes. Pas d'affichage au début pour éviter les ralentissements (ne pas charger tout les départements de France)
@callback(
    Output(app_content['div_box']['box study area']['choice']['county']['list county']['id'], "options"),
    Input(app_content['div_box']['box study area']['choice']['county']['list county']['id'], "search_value"),
    State(app_content['div_box']['box study area']['choice']['county']['list county']['id'], "value")
)
def update_multi_options(search_value : str, value : str) -> list or PreventUpdate:
    """
    Charge les possibilités de listes déroulantes et évite de charger toutes 
    les communes pour éviter les lags et gère donc la recherche
    
    Paramètres :
        search_value (str) : Valeur de la recherche
        value (str) : Valeurs déjà sélectionnées
    
    Retourne :
        (list[str])
    """
    
    #Empêcher un chargement si aucune valeur n'a encore été rentrée pour éviter les lags
    if not search_value:
        raise PreventUpdate
        
    #Création de la listes de résultats de la recherche
    return [
        o for o in list_county if o.lower().startswith(search_value.lower()) or o in (value or [])
    ]



#Changement de la langue 
@callback(
    Output("global_container", "children"),
    Input(app_content['dropdown language']['id'], 'value')
)
def change_language(value : str) -> html.Div:
    """
    Change la langue de l'affichage
    
    Paramètres :
        value('str') : Code de langue choisie (ex : 'fr')
    
    Retourne :
        (dash.html.Div) : Retourne le résultat de Layout, qui est le code pour 
        l'interface utilisateur, traduit dans la langue sélectionnée
    """
    return layout.Layout(value, app_translation[app_content['box mobility']['logo']['label']])




#Bouton "lancer la simulation"
@callback(Output("text", "children"),
          Input("sim_button", "n_clicks"),
          State("tabs_study_zone", "value"),
          
          State(app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], "value"),
          State(app_content['div_box']['box study area']['choice']['radius']['radius input area']['id'], "value"),
          
          State(app_content['div_box']['box study area']['choice']['county']['list county']['id'], "value"),
          
          State(app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'], "value"),
          State(app_content['div_box']['box study area']['choice']['municipality']['municipality input area']['id'], "value"),
          
          State(app_content['div_box']['box means transport']['transport_means']['id'], "value"),
          
          State(app_content['div_box']['box csp']['choice csp']['id'], "value")
)
def start_sim(n_clicks, current_tab, 
              input_radius_municipality, input_radius_value, 
              input_county, 
              input_municipality, input_municipality_value,
              input_transport_means,
              input_csp):
    """
    Actuellement : Affichage des paramètres choisis dans la balise P avec l'id "text"
    Futur : Calcul avec mobility, et sortie du graph généré
    
    Paramètres :
        n_clicks(int) : Nombre de cliques sur lancer la simulation, sert à lancer le script
        input_radius_municipality(str) : Commune d'origine pour le calcul par rayon
        input_radius_value(str) : Valeur de rayon pour le calcul par rayon
        input_county(liste[str]) : Liste de départements pour le calcul par département !Format à refaire pour faciliter l'utilisation!
        input_municipality(list[str]) : Liste de communes pour le calcul par commune
        input_municipality_value(str) : Commune d'origine pour le calcul par communes
        input_transport_means(list[str]) : Liste des moyens de transport sélectionnés
        input_csp(list[str]) : Liste des catégories socio-professionnelles sélectionnées
    """

    #Affichage 
    if current_tab == "tab-rayon" :
        return f"Ville d'origine choisie : {input_radius_municipality} Rayon choisi : {input_radius_value}, Moyen de transport choisis:{input_transport_means}, CSP choisies: {input_csp}"
    
    if current_tab == "tab-municipality":
        return f"Ville d'origine choisie : {input_municipality_value} Liste des villes choisies : {input_municipality}"
    
    if current_tab == "tab-county":
        return f"Liste des départments : {input_county}"





if __name__ == '__main__':
    app.run(debug=False)