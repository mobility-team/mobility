import dash_mantine_components as dmc

def Footer():
    return dmc.AppShellFooter(
        dmc.Group(
            [
                dmc.Text("Basemap: ", size="xs"),
                dmc.Anchor(
                    "© OpenStreetMap contributors",
                    href="https://www.openstreetmap.org/copyright",
                    target="_blank",
                    attributes={"rel": "noopener noreferrer"},
                    size="xs",
                ),
                dmc.Text("•", size="xs"),
                dmc.Anchor(
                    "© CARTO",
                    href="https://carto.com/attributions/",
                    target="_blank",
                    attributes={"rel": "noopener noreferrer"},
                    size="xs",
                ),
            ],
            justify="center",
            gap="xs",
            h="100%",
        ),
        withBorder=True,
        h=28 ,
        px="md",
        style={"fontSize": "11px"},
    )