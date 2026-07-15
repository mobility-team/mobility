# Dependencies

Mobility uses open-source Python and R packages. They are listed below.

Keep the dependency list short. Add a package only when the project needs it.

If you want to add a dependency:
- Make your case in an issue.
- Add it to the lists here.
- If it's a Python package, add it to the [pyproject.toml](../pyproject.toml) dependencies list.
- If it's an R package, add it to:
    - the R package list in [config.py](../mobility/config.py),
    - [DESCRIPTION](../DESCRIPTION), so GitHub can install it during automated tests.

# Python
List from [pyproject.toml](../pyproject.toml) and [docs/requirements.txt](requirements.txt):
- [geopandas](https://github.com/geopandas/geopandas)
- [numpy](https://numpy.org/)
- [pandas](https://pandas.pydata.org/)
- [scipy](https://scipy.org/)
- [requests](https://github.com/psf/requests)
- [shortuuid](https://github.com/skorokithakis/shortuuid)
- [pyarrow](https://github.com/apache/arrow)
- [openpyxl](https://foss.heptapod.net/openpyxl/openpyxl)
- [py7zr](https://github.com/miurahr/py7zr)
- [rich](https://github.com/Textualize/rich)
- [python-dotenv](https://github.com/theskumar/python-dotenv)
- [geojson](https://github.com/jazzband/geojson)
- [matplotlib](https://matplotlib.org/)
- [pyogrio](https://github.com/geopandas/pyogrio)
- [dash](https://github.com/plotly/dash)
- [dash-cytoscape](https://github.com/plotly/dash-cytoscape)
- [sphinx_copybutton](https://sphinx-copybutton.readthedocs.io/)
- [sphinx_rtd_theme](https://sphinx-rtd-theme.readthedocs.io/)
- [myst-parser](https://github.com/executablebooks/MyST-Parser)
- [polars](https://github.com/pola-rs/polars)
- [psutil](https://github.com/giampaolo/psutil)
- [networkx](https://networkx.org/)
- [plotly](https://plotly.com/python/)
- [scikit-learn](https://scikit-learn.org/)
- [gtfs_kit](https://github.com/mrcagney/gtfs_kit)
- [kaleido](https://github.com/plotly/Kaleido)
- [pydantic](https://docs.pydantic.dev/)
- [tenacity](https://github.com/jd/tenacity)
- [mobility-mode-sequence-search](https://github.com/mobility-team/mobility-mode-sequence-search)

# R
List from [config.py](../mobility/config.py) or [DESCRIPTION](../DESCRIPTION):
- [arrow](https://arrow.apache.org/)
- [cppRoutingCCH](https://github.com/mobility-team/cppRoutingCCH)
- [data.table](https://r-datatable.com/)
- [DBI](https://dbi.r-dbi.org/)
- [dbscan](https://cran.r-project.org/package=dbscan)
- [dodgr](https://github.com/ATFutures/dodgr)
- [dplyr](https://dplyr.tidyverse.org/)
- [duckdb](https://duckdb.org/)
- [FNN](https://cran.r-project.org/package=FNN)
- [future.apply](https://github.com/HenrikBengtsson/future.apply)
- [geos](https://github.com/r-spatial/geos)
- [gtfsrouter](https://github.com/ropensci/gtfsrouter)
- [jsonlite](https://github.com/jeroen/jsonlite)
- [log4r](https://github.com/johnmyleswhite/log4r)
- [lubridate](https://lubridate.tidyverse.org/)
- [nngeo](https://github.com/r-spatial/nngeo)
- [osmdata](https://github.com/ropensci/osmdata)
- [remotes](https://github.com/r-lib/remotes)
- [sf](https://r-spatial.github.io/sf/)
- [sfheaders](https://github.com/dcooley/sfheaders)
- [wk](https://paleolimbot.github.io/wk/)

`cppRoutingCCH` is the Mobility routing backend used for CH and CCH road-network queries. It is installed from Mobility's r-universe repository.
