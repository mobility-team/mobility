from dash import Dash, html, Input, Output, State, callback, dcc, Patch, ALL
import dash_bootstrap_components as dbc
import json


with open('interface_content.json', 'r') as file:
    app_content = json.load(file)


app = Dash(suppress_callback_exceptions=True)

app.layout = html.Div(
    [   
         html.Div([
         html.H1("Mobility", id="mobility"),
         html.Button("?", className="button_infobulle", disabled=True, id="b_infobulle1"),
         dbc.Tooltip(children="Mobility est l'application de mobilité", target="b_infobulle1", id="infobulle1", className="infobulle")
         ], id="div_mobility"),
         
         html.Div(
             [
                 html.H2('Simulation choisie', className='title_box'),
                 html.Button("?", className="button_infobulle", disabled=True, id="b_infobulle2"),
                 dbc.Tooltip(children="Choix simulation", target="b_infobulle2", id="infobulle2", className="infobulle"),
                 dcc.Dropdown(["Probabilité de destination", "Choix B ?"], "Probabilité de destination", 
                              id="sim_dropdown", 
                              searchable=False,
                              clearable=False)
             ], className='div_box'),
         
         html.Div(
             [
                 html.H2('Modes de transports', className='title_box'),
                 html.Button("?", className="button_infobulle", disabled=True, id="b_infobulle3"),
                 dbc.Tooltip(children="Choix mode de transport", target="b_infobulle3", id="infobulle3", className="infobulle"),
                 dcc.Checklist(['Marche', 'Vélo', 'Avion', 'Voiture', 'Transport en commun'],
                               [], 
                               id='transport_mode')
             ], className='div_box'),
         
         html.Div(
             [
                 html.H2("Zone d'étude", className='title_box'),
                 html.Button("?", className="button_infobulle", disabled=True, id="b_infobulle4"),
                 dbc.Tooltip(children="Choix zone d'étude", target="b_infobulle4", id="infobulle4", className="infobulle"),
                 
                 html.Button("?", className="button_infobulle", disabled=True, id="b_infobulle5"),
                 dbc.Tooltip(children="Info rayon", target="b_infobulle5", id="infobulle5", className="infobulle"),
                 html.Button("?", className="button_infobulle", disabled=True, id="b_infobulle6"),
                 dbc.Tooltip(children="Info communes", target="b_infobulle6", id="infobulle6", className="infobulle"),
                 html.Button("?", className="button_infobulle", disabled=True, id="b_infobulle7"),
                 dbc.Tooltip(children="Info départements", target="b_infobulle7", id="infobulle7", className="infobulle"),
                 
                 dcc.Tabs(id="tabs_study_zone", value='tab-rayon', children=[
                     dcc.Tab(label='Rayon', 
                             value='tab-rayon', 
                             className="zone_tab", 
                             children = [html.P("Commune d'origine"), 
                                         dcc.Input(id='input_radius_municipality', className='zone_input'), 
                                         html.P("Rayon de recherche"),
                                         dcc.Input(id='input_radius_value', className='zone_input')
                                     ]
                     ),
                     
                     dcc.Tab(label='Commune',
                             value='tab-municipality',
                             className="zone_tab",
                             children=[html.P("Commune d'origine"),
                                       dcc.Input(id='input_municipality_value'),
                                       html.P("Liste de communes"),
                                       html.Div(id="div_container_municipality", children=[]),
                                       html.Button("+", id="add_input_municipality", className='zone_button', n_clicks=0)      
                                     ]
                     ),
                     dcc.Tab(label='Départements', 
                             value='tab-county',
                             className="zone_tab",
                             children=[html.P("Liste de départements"),
                                       html.Div(id="div_container_county", children=[]),
                                       html.Button("+", id="add_input_county", className='zone_button', n_clicks=0)
                                     ]
                     )
                 ])
             ], className='div_box'),

         html.Div(
             [
                 html.H2("Catégories socio-professionnelles", className='title_box'),
                 html.Button("?", className="button_infobulle", disabled=True, id="b_infobulle8"),
                 dbc.Tooltip(children="Choix catégories SP", target="b_infobulle8", id="infobulle8", className="infobulle"),
                 dcc.Checklist(['Agriculteur', 'Artisants', 'Ouvriers'], 
                               [],
                               id='csp_checkbox')
             ], className='div_box'),

         html.Button('Lancer la simulation', 
                     id='sim_button'),
         
         html.Div(children=[
         html.Button('Télécharger le CSV', 
                     id='dl_CSV'),
         
         html.Button('Télécharger le SVF', 
                     id='dl_SVF'),
         ], id="dl_container"),
         
         html.Button('Paramètres',
                     id='settings')
    ], id="global_container")




@callback(Output("div_container_municipality", "children"),
          Input("add_input_municipality", "n_clicks"))


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