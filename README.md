# Mobility, an open-source package for transport modelling

Mobility is an open-source Python package for modelling how people move in a local territory, from short trips to longer trips, for personal and work-related motives, on multimodal transport networks.

It was built for transport modellers, urban planners, students, and analysts who want to work with a full mobility workflow: define a study area, build a synthetic population, describe transport options, simulate daily trips, and compare scenarios. It can be used on regions made of hundreds of communes or zones, mainly in France and Switzerland today.

Mobility estimates travel diaries for a local sample population. The model uses individual socio-economic characteristics, expected daily activity programmes, opportunities at places of interest, congestion on transport networks, and generalized transport costs. Most inputs can be prepared from open data. The package provides default parameters so a modeller can build a first working base case. These defaults still need local checks and calibration.

Mobility can help you explore questions such as:

- how many trips are made by car, bicycle, walking, or public transport,
- where the main origin-destination flows are,
- how travel distances, travel times, and emissions change between scenarios,
- what happens when a new line, a cost change, or a land-use change is added to the model.

After a first run, you can inspect simulated daily plan steps, aggregate indicators by mode or activity, origin-destination flow plots, diagnostics by iteration, and scenario comparisons.

The package is mainly focused today on French and Swiss territories. It uses open data, mobility surveys, Python, R, and compiled tools such as `osmium-tool`. For now, the supported installation path is the mamba environment provided by this repository.

<img width="305" height="256" alt="Flow map of Bayonne region" src="https://github.com/user-attachments/assets/629e5ed0-aa5a-4949-acc6-60615e8f31b5" />
<img width="305" height="256" alt="Car modal share of Bayonne region" src="https://github.com/user-attachments/assets/9fb95b35-4443-40d0-8640-ce1c9846d83b" />

## Start Here

- [Install Mobility](docs/source/installation.md), then run the local quickstart.
- [Run the quickstart](docs/source/quickstart.md) to build a small Limoges model and read first indicators.
- [Read the full documentation](https://mobility.readthedocs.io/en/latest/) when you want to adapt the workflow to a real study.
- [Visit the French project website](https://mobility-team.github.io/)
- [Open an issue](https://github.com/mobility-team/mobility/issues)

Mobility has been developed mainly by [AREP](https://arep.fr) and [Elioth](https://elioth.com/) with [ADEME](https://wiki.resilience-territoire.ademe.fr/wiki/Mobility) support, but anyone can join the project.

More project history, contributors, and example uses are listed in the documentation.
