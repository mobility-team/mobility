from dash import html, dcc
import dash_bootstrap_components as dbc
import json


with open('interface_content.json', 'r') as file:
    app_content = json.load(file)
    
with open('translations_content.json', 'r') as file:
    app_translation = json.load(file)
    

def Layout(language):
    return[   
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
             ], className=app_content['div_box']['class']),
             
             html.Div( #Div conrenant le menu déroulant du mode de simulation
                 [
                     html.H2(children=app_translation[app_content['div_box']['box simulation']['title']['label']][language], 
                             className=app_content['div_box']['box simulation']['title']['class'],
                             id=app_content['div_box']['box simulation']['title']['id']),
                     
                     html.Button("?", 
                                 className=app_content['div_box']['box simulation']['button infobulle2']['class'], 
                                 id=app_content['div_box']['box simulation']['button infobulle2']['id'], 
                                 disabled=True),
                     
                     dbc.Tooltip(children=app_translation[app_content['div_box']['box simulation']['button infobulle2']['infobulle2']['label']][language], 
                                 target=app_content['div_box']['box simulation']['button infobulle2']['id'], 
                                 id=app_content['div_box']['box simulation']['button infobulle2']['infobulle2']['id'],
                                 className=app_content['div_box']['box simulation']['button infobulle2']['infobulle2']['class']),
                     
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
                     
                     dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['button infobulle4']['infobulle4']['label']][language], 
                                 target=app_content['div_box']['box study area']['button infobulle4']['id'], 
                                 id=app_content['div_box']['box study area']['button infobulle4']['infobulle4']['id'], 
                                 className=app_content['div_box']['box study area']['button infobulle4']['infobulle4']['class']),
                     
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
                                                         
                                                         dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle5']['label']][language], 
                                                                     target=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['id'], 
                                                                     id=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle5']['id'], 
                                                                     className=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle5']['class']),
                                                         
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
                                                       
                                                       dcc.Dropdown(id=app_content['div_box']['box study area']['choice']['municipality']['municipality input area']['id'],
                                                                    className='zone_input',
                                                                    multi=False),
                                                       
                                                       html.P("Liste de communes"),
                                                       dcc.Dropdown(id=app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'],
                                                                    className='zone_input',
                                                                    multi=True),     
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
                                                       
                                                       dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle7']['label']][language],
                                                                   target=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['id'], 
                                                                   id=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle7']['id'], 
                                                                   className=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle7']['class']),
                                                       
                                                       dcc.Dropdown(id=app_content['div_box']['box study area']['choice']['county']['list county']['id'],
                                                                    className='zone_input',
                                                                    multi=True),
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
             
             html.Button(children=app_translation[app_content['button settings']['label']][language],
                         id=app_content['button settings']['id']),
             
             dcc.Dropdown(options=[{'label': app_content['dropdown language']['languages'][key],
                                     'value': key} for key in app_content['dropdown language']['languages'].keys()],
                          value='fr',
                          id=app_content['dropdown language']['id']),
             
             html.P(id="text")
        ]