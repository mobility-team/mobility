import dash_mantine_components as dmc

def Header(title: str = "AREP Mobility Dashboard"):
    return dmc.AppShellHeader(
        dmc.Group(
            [                
                 dmc.Image(
                        src="/assets/images/logo_mobility.png", 
                        h=60,            
                        w="auto",
                        alt="Mobility",
                        fit="contain",
                        mb="10",
                        mt="10"
                    )
            ],
            h="100%",
            px="md",
            justify="space-between",
        )
    )
