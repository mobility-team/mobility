# Installation

Mobility currently uses Python, R, and compiled tools such as `osmium-tool`.

The supported installation path uses a mamba environment for Python, R, and system tools, then installs Mobility from PyPI as `mobility-tools`.
The Python import name is still `mobility`.

## 1. Install the tools

Install:

- [Miniforge](https://github.com/conda-forge/miniforge), to get `mamba`,
- an editor such as VS Code.

## 2. Create the mamba environment

Download the environment file from the latest Mobility release, then create the environment.

### Windows

Open a Miniforge Prompt or PowerShell and run:

```powershell
$version = "v0.2.0"
$base = "https://github.com/mobility-team/mobility/releases/download/$version"

Invoke-WebRequest "$base/environment.yml" -OutFile "environment.yml"
mamba env create -n mobility -f environment.yml
mamba activate mobility
```

### macOS

```shell
version="v0.2.0"
base="https://github.com/mobility-team/mobility/releases/download/$version"

curl -L -o environment.yml "$base/environment.yml"
mamba env create -n mobility -f environment.yml
mamba activate mobility
```

### Linux

```shell
version="v0.2.0"
base="https://github.com/mobility-team/mobility/releases/download/$version"

curl -L -o environment.yml "$base/environment.yml"
mamba env create -n mobility -f environment.yml
mamba activate mobility
```

## 3. Install Mobility

Install Mobility from PyPI:

```shell
pip install mobility-tools==0.2.0
```

Then check that Python can import it:

```shell
python -c "import mobility; print(mobility.__file__)"
```

## 4. Choose data folders

Mobility stores downloaded and prepared data on disk. Use two folders:

- one package data folder for shared datasets,
- one project data folder for project-specific inputs and cache files.

You can pass these folders directly:

```python
import mobility

mobility.set_params(
    package_data_folder_path=r"C:\Users\your.name\Documents\mobility-data",
    project_data_folder_path=r"C:\Users\your.name\Documents\mobility-projects\first-project",
)
```

If you do not pass paths, Mobility uses a default folder in your user directory and asks for confirmation when it has to create it.

For repeated project work, it is usually clearer to store the paths in a `.env` file and load them in your scripts:

```text
MOBILITY_PACKAGE_DATA_FOLDER=C:\Users\your.name\Documents\mobility-data
MOBILITY_PROJECT_DATA_FOLDER=C:\Users\your.name\Documents\mobility-projects\first-project
```

## 5. Install R packages

The first call to `mobility.set_params(...)` installs the R packages used by Mobility.

This can take time on a new computer. Later runs reuse the installed packages.

If you are behind a company proxy on Windows and R package download fails, try:

```python
import mobility

mobility.set_params(
    package_data_folder_path=r"C:\Users\your.name\Documents\mobility-data",
    project_data_folder_path=r"C:\Users\your.name\Documents\mobility-projects\first-project",
    r_packages_download_method="wininet",
)
```

## 6. Check the installation

Run the quickstart from the documentation:

[Quickstart](quickstart.md)

The first run can be slow because Mobility may download and prepare local data.

You are done when the script finishes without an error and creates a first set of cached project files in your project data folder. If the origin-destination plot opens, your Python, R, data folders, and basic routing setup are working together.

If the command fails before the model starts, first check that the `mobility` mamba environment is active and that `mobility.set_params(...)` can access your data folders.

## Editors

You can use Mobility from any editor that points to the `mobility` mamba environment.

### VS Code

In VS Code, select the Python interpreter from the `mobility` environment. On Windows, it is often located in a folder like:

```text
C:\Users\your.name\AppData\Local\miniforge3\envs\mobility
```

## Common Problems

### OSM parsing is very slow on Windows

Make sure the R package `osmdata` uses the Mobility-provided patched version. On Windows, `mobility.set_params(...)` installs this version automatically from the installed Python package.

If needed, print the ZIP file path with:

```shell
python -c "from importlib import resources; print(resources.files('mobility.runtime.resources').joinpath('osmdata_0.2.5.005.zip'))"
```

To check the installed R packages, open R inside the `mobility` environment and run:

```r
ip = as.data.frame(installed.packages()[, c(1, 3:4)])
ip = ip[is.na(ip$Priority), 1:2, drop = FALSE]
ip
```

If the `osmdata` version differs from `0.2.5.005`, install the Mobility ZIP manually:

```r
install.packages(file.choose(), repos = NULL)
```

Then select the printed `osmdata_0.2.5.005.zip` path.

### R package installation fails behind a proxy

Use `r_packages_download_method="wininet"` in `mobility.set_params(...)`.

If the error continues, you can install the R packages manually.

First open R inside the `mobility` environment:

```shell
mamba activate mobility
R
```

Then install `pak`:

```r
install.packages("pak")
```

If the download still fails on Windows, retry with:

```r
install.packages("pak", method = "wininet")
```

Exit R with `q()`, then retry:

```python
import mobility

mobility.set_params(debug=True)
```

If R still reports missing packages, reopen R and install the main package list with `wininet`:

```r
install.packages(
    c(
        "remotes", "dodgr", "sf", "dplyr", "sfheaders", "nngeo",
        "data.table", "arrow", "lubridate", "future.apply", "cppRouting",
        "duckdb", "DBI", "jsonlite", "gtfsrouter", "geos", "wk", "FNN",
        "dbscan"
    ),
    method = "wininet"
)
```

Then install the patched `osmdata` ZIP as described above.

### You need help with a new error

Check the [open bug reports](https://github.com/mobility-team/mobility/issues?q=is%3Aissue%20state%3Aopen%20label%3Abug). If no open issue matches the problem, open a new issue with the command you ran and the full error message.
