# app/pages/main/main.py
from pathlib import Path
from dash import Dash
import dash_mantine_components as dmc
from app.components.layout.header.header import Header
from app.components.features.map.map import Map
from app.components.layout.footer.footer import Footer


ASSETS_PATH = Path(__file__).resolve().parents[3] / "assets"
HEADER_HEIGHT = 60



app = Dash(
    __name__,
    suppress_callback_exceptions=True,
    assets_folder=str(ASSETS_PATH),
    assets_url_path="/assets",
)

app.layout = dmc.MantineProvider(
    dmc.AppShell(
        children=[
            Header("MOBILITY"),
            dmc.AppShellMain(
                Map(),  
                style={
                    "height": f"calc(100vh - {HEADER_HEIGHT}px)",
                    "padding": 0,
                    "margin": 0,
                    "overflow": "hidden",
                },
            ),
            Footer(),
        ],
        padding=0,                    
        styles={"main": {"padding": 0}},  
    )
)

if __name__ == "__main__":
    app.run(debug=True, dev_tools_ui=False)
