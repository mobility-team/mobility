from dash import Dash, html, Input, Output, State, callback, dcc, Patch, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import json
import pandas as pd

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

app.layout = html.Div(
    [   
         html.Div([ #Div contenant le logo mobility
         html.H1("Mobility", 
                 id=app_content['box mobility']['logo']['id']),
         
         html.Button(children="?",
                     className=app_content['box mobility']['button infobulle1']['class'],
                     id=app_content['box mobility']['button infobulle1']['id'],
                     disabled=True),
         
         dbc.Tooltip(children=app_translation[app_content['box mobility']['button infobulle1']['infobulle1']['label']][language],
                     target=app_content['box mobility']['button infobulle1']['id'],
                     className=app_content['box mobility']['button infobulle1']['infobulle1']['class'], 
                     id=app_content['box mobility']['button infobulle1']['infobulle1']['id'])
         ], id=app_content['box mobility']['id']),
         
         html.Div( #Div conrenant le menu déroulant du mode de simulation
             [
                 html.H2(children=app_translation[app_content['div_box']['box simulation']['title']['label']][language], 
                         className=app_content['div_box']['box simulation']['title']['class'],
                         id=app_content['div_box']['box simulation']['title']['id']),
                 
                 html.Button("?", 
                             className=app_content['div_box']['box simulation']['button infobulle2']['class'], 
                             id=app_content['div_box']['box simulation']['button infobulle2']['id'], 
                             disabled=True),
                 
                 dbc.Tooltip(children=app_translation[app_content['div_box']['box simulation']['button infobulle2']['infobulle3']['label']][language], 
                             target=app_content['div_box']['box simulation']['button infobulle2']['id'], 
                             id=app_content['div_box']['box simulation']['button infobulle2']['infobulle3']['id'],
                             className=app_content['div_box']['box simulation']['button infobulle2']['infobulle3']['class']),
                 
                 dcc.Dropdown(options=[app_translation[o][language] for o in app_content['div_box']['box simulation']['dropdown']['options']],
                              value=app_translation[app_content['div_box']['box simulation']['dropdown']['options'][0]]['fr'], 
                              id=app_content['div_box']['box simulation']['dropdown']['id'], 
                              searchable=False,
                              clearable=False)
             ], className=app_content['div_box']['class']),
         
         html.Div( #Div contenant le choix du mode de transport 
             [
                 html.H2(app_translation[app_content['div_box']['box means transport']['title']['label']][language], 
                         className=app_content['div_box']['box means transport']['title']['class'],
                         id=app_content['div_box']['box means transport']['title']['id']),
                 
                 html.Button("?",
                             className=app_content['div_box']['box means transport']['button infobulle3']['class'], 
                             id=app_content['div_box']['box means transport']['button infobulle3']['id'],
                             disabled=True
                             ),
                 
                 dbc.Tooltip(children=app_translation[app_content['div_box']['box means transport']['button infobulle3']['infobulle3']['label']][language], 
                             target=app_content['div_box']['box means transport']['button infobulle3']['id'],
                             id=app_content['div_box']['box means transport']['button infobulle3']['infobulle3']['id'], 
                             className=app_content['div_box']['box means transport']['button infobulle3']['infobulle3']['class']
                             ),
                 
                 dcc.Checklist(options=[app_translation[o][language] for o in app_content['div_box']['box means transport']['transport_means']['options']],
                               value=[app_translation[o][language] for o in app_content['div_box']['box means transport']['transport_means']['options']], 
                               id=app_content['div_box']['box means transport']['transport_means']['id'])
             ], className=app_content['div_box']['class']),
         
         html.Div( #Div contenant le choix de la zone d'étude
             [
                 html.H2(app_translation[app_content['div_box']['box study area']['title']['label']][language],
                         className=app_content['div_box']['box study area']['title']['class'],
                         id=app_content['div_box']['box study area']['title']['id']),
                 
                 html.Button("?", 
                             className=app_content['div_box']['box study area']['button infobulle4']['class'], 
                             id=app_content['div_box']['box study area']['button infobulle4']['id'], 
                             disabled=True),
                 
                 dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['button infobulle4']['infobulle6']['label']][language], 
                             target=app_content['div_box']['box study area']['button infobulle4']['id'], 
                             id=app_content['div_box']['box study area']['button infobulle4']['infobulle6']['id'], 
                             className=app_content['div_box']['box study area']['button infobulle4']['infobulle6']['class']),
                 
                 dcc.Tabs(id="tabs_study_zone", 
                          value='tab-rayon', 
                          children=[
                                 dcc.Tab(label=app_translation[app_content['div_box']['box study area']['choice']['radius']['title']['label']][language], 
                                         value='tab-rayon', 
                                         className=app_content['div_box']['box study area']['choice']['class'], 
                                         children = [html.P(app_translation[app_content['div_box']['box study area']['choice']['radius']['municipality origin txt']['label']][language]),                                                     
                                                     
                                                     html.Button("?", 
                                                                 className=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['class'], 
                                                                 id=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['id'], 
                                                                 disabled=True),
                                                     
                                                     dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle6']['label']][language], 
                                                                 target=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['id'], 
                                                                 id=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle6']['id'], 
                                                                 className=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle6']['class']),
                                                     
                                                     dcc.Dropdown(id=app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], 
                                                                  className='zone_input',
                                                                  multi=True), 
                                                     
                                                     html.P(app_translation[app_content['div_box']['box study area']['choice']['radius']['radius research txt']['label']][language]),
                                                     dcc.Input(id=app_content['div_box']['box study area']['choice']['radius']['radius input area']['id'], 
                                                               className='zone_input')
                                                 ]
                                 ),
                                 
                                 dcc.Tab(label=app_translation[app_content['div_box']['box study area']['choice']['municipality']['title']['label']][language],
                                         value='tab-municipality',
                                         className=app_content['div_box']['box study area']['choice']['class'],
                                         children=[html.P(app_translation[app_content['div_box']['box study area']['choice']['municipality']['municipality origin txt']['label']][language]),
                                                   
                                                   html.Button("?", 
                                                               className=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['class'], 
                                                               id=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['id'], 
                                                               disabled=True),
                                                   
                                                   dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['infobulle6']['label']][language],
                                                               target=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['id'], 
                                                               id=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['infobulle6']['id'], 
                                                               className=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['infobulle6']['class']),
                                                   
                                                   dcc.Dropdown(options=list_municipality,
                                                                id=app_content['div_box']['box study area']['choice']['municipality']['municipality input area']['id'],
                                                                className='zone_input'),
                                                   
                                                   html.P("Liste de communes"),
                                                   dcc.Dropdown(options=list_municipality,
                                                                id=app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'],
                                                                className='zone_input'),     
                                                 ]
                                 ),
                                 dcc.Tab(label=app_translation[app_content['div_box']['box study area']['choice']['county']['title']['label']][language], 
                                         
                                         
                                         value='tab-county',
                                         className=app_content['div_box']['box study area']['choice']['class'],
                                         children=[html.P("Liste de départements"),
                                                   
                                                   html.Button("?", 
                                                               className=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['class'],
                                                               id=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['id'],
                                                               disabled=True),
                                                   
                                                   dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle8']['label']][language],
                                                               target=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['id'], 
                                                               id=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle8']['id'], 
                                                               className=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle8']['class']),
                                                   
                                                   dcc.Dropdown(options=list_county,
                                                                id="div_container_county",
                                                                className='zone_input'),
                                                 ]
                                 )
                 ])
             ], className=app_content['div_box']['class']),

         html.Div( #Div contenant le choix des catégories S-P
             [
                 html.H2("Catégories socio-professionnelles", className='title_box'),
                 
                 html.Button("?", className=app_content['div_box']['box csp']['button infobulle8']['class'],
                             id=app_content['div_box']['box csp']['button infobulle8']['id'],
                             disabled=True),
                 
                 dbc.Tooltip(children=app_translation[app_content['div_box']['box csp']['button infobulle8']['infobulle8']['label']][language], 
                             target=app_content['div_box']['box csp']['button infobulle8']['id'], 
                             id=app_content['div_box']['box csp']['button infobulle8']['infobulle8']['id'], 
                             className=app_content['div_box']['box csp']['button infobulle8']['infobulle8']['class']),
                 
                 dcc.Checklist(options=[app_translation[o][language] for o in app_content['div_box']['box csp']['choice csp']['options']], 
                               value=[app_translation[o][language] for o in app_content['div_box']['box csp']['choice csp']['options']],
                               id=app_content['div_box']['box csp']['choice csp']['id'])
             ], className=app_content['div_box']['class']),

         html.Button(children=app_translation[app_content['button launch simulation']['label']][language], 
                     id=app_content['button launch simulation']['id']),
         
         html.Div( #Div contenant les bouton de téléchargement
             children=[
                 html.Button(children=app_translation[app_content['button dl csv']['label']][language], 
                             id=app_content['button dl csv']['id'],
                             className=app_content['button dl csv']['class']),
                 
                 html.Button(children=app_translation[app_content['button dl svg']['label']][language], 
                             id=app_content['button dl svg']['id'],
                             className=app_content['button dl svg']['class'])
             ], id="dl_container"),
         
         html.Button('Paramètres',
                     id='settings'),
         
         html.P(id="text")
    ], id="global_container")



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
        o for o in list_municipality if search_value in o or o in (value or [])
    ]






@callback(Output("text", "children"),
          Input("sim_button", "n_clicks"),
          State("tabs_study_zone", "value"),
          
          State(app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], "value"),
          State(app_content['div_box']['box study area']['choice']['radius']['radius input area']['id'], "value"),
          
          State({"type": "input_county", "index": ALL}, "value"),
          
          State({"type": "input_municipality", "index": ALL}, "value"),
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