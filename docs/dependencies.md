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
List from [pyproject.toml](../pyproject.toml):
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
- [seaborn](https://seaborn.pydata.org/)
- [pyogrio](https://github.com/geopandas/pyogrio)
- [sphinxcontrib-napoleon](https://github.com/sphinx-doc/sphinxcontrib-napoleon)
- [sphinx_copybutton](https://sphinx-copybutton.readthedocs.io/)
- [myst-parser](https://github.com/executablebooks/MyST-Parser)
- [streamlit](https://github.com/streamlit/streamlit)
- [streamlit-lottie](https://github.com/andfanilo/streamlit-lottie)
- [streamlit-option-menu](https://github.com/victoryhb/streamlit-option-menu)
- [polars](https://github.com/pola-rs/polars)
- [psutil](https://github.com/giampaolo/psutil)
- [dash](https://github.com/plotly/dash)
- [dash-leaflet](https://github.com/thedirtyfew/dash-leaflet)
- [dash-bootstrap-components](https://github.com/facultyai/dash-bootstrap-components)
- [networkx](https://networkx.org/)

# R
List from [config.py](../mobility/config.py) or [DESCRIPTION](../DESCRIPTION):
- [arrow](https://arrow.apache.org/)
- [cluster](https://stat.ethz.ch/R-manual/R-devel/library/cluster/html/00Index.html)
- [codetools](https://stat.ethz.ch/R-manual/R-devel/library/codetools/html/00Index.html)
- [cppRouting](https://github.com/vlarmet/cppRouting)
- [data.table](https://r-datatable.com/)
- [dbscan](https://cran.r-project.org/package=dbscan)
- [dodgr](https://github.com/ATFutures/dodgr)
- [dplyr](https://dplyr.tidyverse.org/)
- [duckdb](https://duckdb.org/)
- [FNN](https://cran.r-project.org/package=FNN)
- [future](https://github.com/HenrikBengtsson/future)
- [future.apply](https://github.com/HenrikBengtsson/future.apply)
- [geodist](https://github.com/hypertidy/geodist)
- [geos](https://github.com/r-spatial/geos)
- [ggplot2](https://ggplot2.tidyverse.org/)
- [gtfsrouter](https://github.com/ropensci/gtfsrouter)
- [hms](https://github.com/tidyverse/hms)
- [jsonlite](https://github.com/jeroen/jsonlite)
- [log4r](https://github.com/johnmyleswhite/log4r)
- [lubridate](https://lubridate.tidyverse.org/)
- [nngeo](https://github.com/r-spatial/nngeo)
- [osmdata](https://github.com/ropensci/osmdata)
- [pbapply](https://github.com/psolymos/pbapply)
- [readxl](https://readxl.tidyverse.org/)
- [remotes](https://github.com/r-lib/remotes)
- [reshape2](https://github.com/hadley/reshape)
- [sf](https://r-spatial.github.io/sf/)
- [sfheaders](https://github.com/dcooley/sfheaders)
- [stringr](https://stringr.tidyverse.org/)
- [svglite](https://github.com/r-lib/svglite)
