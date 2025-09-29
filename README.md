# Mobility - Open source multi-scale travel behavior modelling
Mobility is an open source Python library for modelling the travel behavior of local populations, from short range to long range trips, for different travel motives (personnal and professional) and on multimodal transport networks (walk, bicycle, public transport and car). It can be used on regions composed of hundreds of cities (up to a thousand), located in France and Switzerland.

It provides estimated travel diaries of a local sample population, based on indidividual socio-economical characteristics, expected daily activity programmes, competition over opportunities at places of interest (jobs, shops, leisure facilities...) and congestion of transport infrastructures.

It uses discrete choice models to evaluate destination and mode decisions based on generalized cost estimates, estimated from detailed unimodal and intermodal travel costs between the transport zones of the studied region.

It handles the preparation of most inputs from open data (administrative boundaries, housing and places of interest spatial distribution, transport infrastructure, public transport schedules, activity programmes) and provides reasonable default values for model parameters. 

To see how Mobility works, take a look at the [installation instructions](docs/installation.md) and the [quickstart page](docs/quickstart.md). If you want to contribute, see our [guidelines](docs/contributing.md) and the [issue tracker](https://github.com/mobility-team/mobility).