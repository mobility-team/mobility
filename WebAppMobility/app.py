from dash import Dash, html, Input, Output, State, callback, dcc, Patch, ALL
import dash_bootstrap_components as dbc
import json

language='fr'


with open('interface_content.json', 'r') as file:
    app_content = json.load(file)
    
with open('translations_content.json', 'r') as file:
    app_translation = json.load(file)

key = [app_translation[o][language] for o in app_content['div_box']['box simulation']['dropdown']['options']]
print(key)

app = Dash(suppress_callback_exceptions=True)

app.layout = html.Div(
    [   
         html.Div([ #Div contenant le logo mobility
         html.H1("Mobility", id=app_content['box mobility']['logo']['id']),
         
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
                 
                 dcc.Dropdown([app_translation[o][language] for o in app_content['div_box']['box simulation']['dropdown']['options']],
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
                 
                 dcc.Checklist([app_translation[o][language] for o in app_content['div_box']['box means transport']['transport_means']['options']],
                               [app_translation[o][language] for o in app_content['div_box']['box means transport']['transport_means']['options']], 
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
               
                 
                 html.Button("?", 
                             className=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['class'], 
                             id=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['id'], 
                             disabled=True),
                 
                 dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle6']['label']][language], 
                             target=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['id'], 
                             id=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle6']['id'], 
                             className=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle6']['class']),
                 
                 
                 html.Button("?", 
                             className=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['class'], 
                             id=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['id'], 
                             disabled=True),
                 
                 dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['infobulle6']['label']][language],
                             target=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['id'], 
                             id=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['infobulle6']['id'], 
                             className=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['infobulle6']['class']),
                 
                 
                 html.Button("?", 
                             className=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['class'],
                             id=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['id'],
                             disabled=True),
                 
                 dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle8']['label']][language],
                             target=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['id'], 
                             id=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle8']['id'], 
                             className=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle8']['class']),
                 
                 dcc.Tabs(id="tabs_study_zone", 
                          value='tab-rayon', 
                          children=[
                                 dcc.Tab(label=app_translation[app_content['div_box']['box study area']['choice']['radius']['title']['label']][language], 
                                         value='tab-rayon', 
                                         className=app_content['div_box']['box study area']['choice']['class'], 
                                         children = [html.P(app_translation[app_content['div_box']['box study area']['choice']['radius']['municipality origin txt']['label']][language]),                                                     
                                                     dcc.Input(id=app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], 
                                                               className='zone_input'), 
                                                     
                                                     html.P(app_translation[app_content['div_box']['box study area']['choice']['radius']['radius research txt']['label']][language]),
                                                     dcc.Input(id=app_content['div_box']['box study area']['choice']['radius']['radius input area']['id'], 
                                                               className='zone_input')
                                                 ]
                                 ),
                                 
                                 dcc.Tab(label=app_translation[app_content['div_box']['box study area']['choice']['municipality']['title']['label']][language],
                                         value='tab-municipality',
                                         className=app_content['div_box']['box study area']['choice']['class'],
                                         children=[html.P(app_translation[app_content['div_box']['box study area']['choice']['municipality']['municipality origin txt']['label']][language]),
                                                   dcc.Input(id=app_content['div_box']['box study area']['choice']['municipality']['municipality input area']['id'],
                                                             className='zone_input'),
                                                   
                                                   html.P("Liste de communes"),
                                                   html.Div(id=app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'],
                                                            children=[]),
                                                   html.Button(children="+", 
                                                               id=app_content['div_box']['box study area']['choice']['municipality']['button add municipality']['id'], 
                                                               className=app_content['div_box']['box study area']['choice']['municipality']['button add municipality']['class'],
                                                               n_clicks=0)      
                                                 ]
                                 ),
                                 dcc.Tab(label=app_translation[app_content['div_box']['box study area']['choice']['county']['title']['label']][language], 
                                         value='tab-county',
                                         className=app_content['div_box']['box study area']['choice']['class'],
                                         children=[html.P("Liste de départements"),
                                                   html.Div(id="div_container_county", children=[]),
                                                   html.Button("+", id="add_input_county", className='zone_button', n_clicks=0)
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
                 
                 dbc.Tooltip(children="Choix catégories SP", target="b_infobulle8", id="infobulle8", className="infobulle"),
                 
                 dcc.Checklist(['Agriculteur', 'Artisants', 'Ouvriers'], 
                               [],
                               id='csp_checkbox')
             ], className='div_box'),

         html.Button('Lancer la simulation', 
                     id='sim_button'),
         
         html.Div( #Div contenant les bouton de téléchargement
             children=[
                 html.Button('Télécharger le CSV', 
                             id='dl_CSV'),
                 
                 html.Button('Télécharger le SVF', 
                         id='dl_SVF'),
             ], id="dl_container"),
         
         html.Button('Paramètres',
                     id='settings'),
         
         html.P(id="text")
    ], id="global_container")




@callback(Output("div_container_municipality", "children"),
          Input("add_input_community", "n_clicks"))


def add_municipality_input(n_clicks):
    patched_children = Patch()
    new_input = dcc.Input(id={"type": "input_municipality", "index": n_clicks})
    
    patched_children.append(new_input)
    return patched_children



@callback(Output("div_container_county", "children"),
          Input("add_input_county", "n_clicks"))


def add_county_input(n_clicks):
    patched_children = Patch()
    new_input = dcc.Input(id={"type": "input_county", "index": n_clicks})
    
    patched_children.append(new_input)
    return patched_children


@callback(Output("text", "children"),
          Input("sim_button", "n_clicks"),
          State("tabs_study_zone", "value"),
          
          State("input_radius_municipality", "value"),
          State("input_radius_value", "value"),
          
          State({"type": "input_county", "index": ALL}, "value"),
          
          State({"type": "input_municipality", "index": ALL}, "value"),
          State("input_municipality_value", "value")
)
          


def start_sim(n_clicks, current_tab, 
              input_radius_municipality, input_radius_value, 
              input_county, 
              input_municipality, input_municipality_value):
    
    
    
    if current_tab == "tab-rayon" :
        return f"Ville d'origine choisie : {input_radius_municipality} Rayon choisi : {input_radius_value}"
    
    if current_tab == "tab-municipality":
        return f"Ville d'origine choisie : {input_municipality_value} Liste des villes choisies : {input_municipality}"
    
    if current_tab == "tab-county":
        return f"Liste des départments : {input_county}"





if __name__ == '__main__':
    app.run(debug=True)