from dash import Dash, html, Input, Output, State, callback, dcc, Patch, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import json
import pandas as pd
import layout


language='fr'


with open('interface_content.json', 'r') as file:
    app_content = json.load(file)
    
with open('translations_content.json', 'r') as file:
    app_translation = json.load(file)


df_municipality = pd.read_csv('data\donneesCommunesFrance.csv')
df_county = pd.read_csv('data/departements-france.csv')

list_municipality = df_municipality['NOM_COM'].tolist()
list_municipality.sort()

list_county = [a+' ('+b+')' for a,b in zip(df_county['nom_departement'].tolist(), df_county['code_departement'].tolist())]
list_county.sort()


app = Dash(suppress_callback_exceptions=True)

app.layout = html.Div(children=layout.Layout(language), id="global_container")



@callback(
    Output(app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], "options"),
    Input(app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], "search_value"),
    State(app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], "value")
)
def update_multi_options(search_value, value):
    if not search_value:
        raise PreventUpdate
    # Make sure that the set values are in the option list, else they will disappear
    # from the shown select list, but still part of the `value`.
    return [
        o for o in list_municipality if o.lower().startswith(search_value.lower()) or o in (value or [])
    ]



@callback(
    Output(app_content['div_box']['box study area']['choice']['municipality']['municipality input area']['id'], "options"),
    Input(app_content['div_box']['box study area']['choice']['municipality']['municipality input area']['id'], "search_value")
)
def update_options(search_value):
    if not search_value:
        raise PreventUpdate
    # Make sure that the set values are in the option list, else they will disappear
    # from the shown select list, but still part of the `value`.
    return [
        o for o in list_municipality if o.lower().startswith(search_value.lower())
    ]


@callback(
    Output(app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'], "options"),
    Input(app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'], "search_value"),
    State(app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'], "value")
)
def update_multi_options(search_value, value):
    if not search_value:
        raise PreventUpdate
    # Make sure that the set values are in the option list, else they will disappear
    # from the shown select list, but still part of the `value`.
    return [
        o for o in list_municipality if o.lower().startswith(search_value.lower()) or o in (value or [])
    ]


@callback(
    Output(app_content['div_box']['box study area']['choice']['county']['list county']['id'], "options"),
    Input(app_content['div_box']['box study area']['choice']['county']['list county']['id'], "search_value"),
    State(app_content['div_box']['box study area']['choice']['county']['list county']['id'], "value")
)
def update_multi_options(search_value, value):
    if not search_value:
        raise PreventUpdate
    # Make sure that the set values are in the option list, else they will disappear
    # from the shown select list, but still part of the `value`.
    return [
        o for o in list_municipality if o.lower().startswith(search_value.lower()) or o in (value or [])
    ]


@callback(
    Output("global_container", "children"),
    Input(app_content['dropdown language']['id'], 'value'))
def change_language(value):
    return layout.Layout(value)



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
    
    
    if current_tab == "tab-rayon" :
        return f"Ville d'origine choisie : {input_radius_municipality} Rayon choisi : {input_radius_value}, Moyen de transport choisis:{input_transport_means}, CSP choisies: {input_csp}"
    
    if current_tab == "tab-municipality":
        return f"Ville d'origine choisie : {input_municipality_value} Liste des villes choisies : {input_municipality}"
    
    if current_tab == "tab-county":
        return f"Liste des départments : {input_county}"





if __name__ == '__main__':
    app.run(debug=True)