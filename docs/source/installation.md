# Installation

Mobility uses Python, R, and compiled tools such as `osmium-tool`.

The recommended installation path uses Pixi. Pixi prepares the tools that Mobility needs in your project folder, then installs Mobility from PyPI as `mobility-tools`.
The Python import name is still `mobility`.

The mamba installation path is still supported for now. Use it if you already have a working mamba setup or if Pixi is not available on your computer.

## 1. Install the tools

Install:

- [Pixi](https://pixi.sh/latest/),
- an editor such as VS Code.

## 2. Create a Mobility project folder

Create one folder for your first Mobility study. This folder will contain:

- the Pixi environment file,
- your Python scripts,
- optional project notes and output files.

### Windows

Open PowerShell and run:

```powershell
mkdir first-mobility-study
cd first-mobility-study

$version = "v0.2.1"
$base = "https://github.com/mobility-team/mobility/releases/download/$version"
Invoke-WebRequest "$base/pixi.toml" -OutFile "pixi.toml"

pixi install
```

### macOS

```shell
mkdir first-mobility-study
cd first-mobility-study

version="v0.2.1"
base="https://github.com/mobility-team/mobility/releases/download/$version"
curl -L -o pixi.toml "$base/pixi.toml"

pixi install
```

### Linux

```shell
mkdir first-mobility-study
cd first-mobility-study

version="v0.2.1"
base="https://github.com/mobility-team/mobility/releases/download/$version"
curl -L -o pixi.toml "$base/pixi.toml"

pixi install
```

## 3. Check Mobility

Run Python through Pixi from the folder that contains `pixi.toml`:

```shell
pixi run python -c "import mobility; print(mobility.__file__)"
```

Then run your own script in the same way:

```shell
pixi run python my_script.py
```

The release `pixi.toml` installs Mobility from PyPI. You do not need a local copy of the Mobility repository when you only want to run a study.

## Mamba Fallback

If you already use mamba, you can still create the Mobility environment from the release `environment.yml` file.

### Windows

Open a Miniforge Prompt or PowerShell and run:

```powershell
$version = "v0.2.1"
$base = "https://github.com/mobility-team/mobility/releases/download/$version"

Invoke-WebRequest "$base/environment.yml" -OutFile "environment.yml"
mamba env create -n mobility -f environment.yml
mamba activate mobility
pip install mobility-tools==0.2.1
```

### macOS

```shell
version="v0.2.1"
base="https://github.com/mobility-team/mobility/releases/download/$version"

curl -L -o environment.yml "$base/environment.yml"
mamba env create -n mobility -f environment.yml
mamba activate mobility
pip install mobility-tools==0.2.1
```

### Linux

```shell
version="v0.2.1"
base="https://github.com/mobility-team/mobility/releases/download/$version"

curl -L -o environment.yml "$base/environment.yml"
mamba env create -n mobility -f environment.yml
mamba activate mobility
pip install mobility-tools==0.2.1
```

Check that Python can import Mobility:

```shell
python -c "import mobility; print(mobility.__file__)"
```

## Docker

If you use Docker, you can use the Mobility runtime image instead of creating the mamba environment yourself.
The container includes Python, R, `osmium-tool`, and the system libraries needed by Mobility.

From a folder that contains your script, run one of the commands below.

On macOS, Linux, WSL, or Git Bash:

```shell
docker run --rm -it \
  -v "$PWD:/app" \
  -w /app \
  ghcr.io/mobility-team/mobility-runtime:0.2.1 \
  python your_script.py
```

On Windows PowerShell:

```powershell
docker run --rm -it `
  -v "${PWD}:/app" `
  -w /app `
  ghcr.io/mobility-team/mobility-runtime:0.2.1 `
  python your_script.py
```

If you cloned the Mobility repository and want to run the French quickstart, replace `your_script.py` with `examples/quickstart-fr.py`:

```shell
docker run --rm -it \
  -v "$PWD:/app" \
  -w /app \
  ghcr.io/mobility-team/mobility-runtime:0.2.1 \
  python examples/quickstart-fr.py
```

Use Docker when you want one prepared environment and do not want to manage R packages and system libraries by hand.

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

Use the default setup first. If a download fails on a company network, see the certificate and proxy notes in Common Problems.

## 6. Check the installation

Run the quickstart from the documentation:

[Quickstart](quickstart.md)

The first run can be slow because Mobility may download and prepare local data.

You are done when the script finishes without an error and creates a first set of cached project files in your project data folder. If the origin-destination plot opens, your Python, R, data folders, and basic routing setup are working together.

If the command fails before the model starts, first check that you are running Python from the right environment and that `mobility.set_params(...)` can access your data folders.

With Pixi, run scripts from the folder that contains `pixi.toml`:

```shell
pixi run python your_script.py
```

With mamba, check that the `mobility` environment is active.

## Editors

You can use Mobility from any editor that points to the Python environment prepared for your project.

### VS Code

In VS Code, select the Python interpreter from the Pixi environment in your project folder. It is inside the local `.pixi` folder.

If you use mamba, select the Python interpreter from the `mobility` environment. On Windows, it is often located in a folder like:

```text
C:\Users\your.name\AppData\Local\miniforge3\envs\mobility
```

## Common Problems

### Pixi cannot download packages on a company network

Use this section only if `pixi install` fails with a certificate error.

On some company networks, Pixi must be told to use the certificate store from your operating system:

```shell
pixi config set tls-root-certs system
```

Then retry from the folder that contains `pixi.toml`:

```shell
pixi install
```

This setting affects Pixi itself. It helps Pixi download Python, R, `osmium-tool`, and Python packages while it prepares the environment.

If the command still fails, your computer may need the company certificate to be installed in the operating system certificate store. Ask your IT team how certificates are managed on your network.

If you use an older Pixi version and `system` is not accepted, update Pixi first.

### Mobility downloads fail with a certificate error

Use this section only if the Pixi or mamba environment is installed, but a Mobility script fails while downloading data.

Some Python downloads may fail on company networks because Python does not use the same certificate store as your browser. In that case, retry the setup with truststore injection:

```python
import mobility

mobility.set_params(
    package_data_folder_path=r"C:\Users\your.name\Documents\mobility-data",
    project_data_folder_path=r"C:\Users\your.name\Documents\mobility-projects\first-project",
    inject_into_ssl=True,
)
```

This setting affects Python downloads made by Mobility. It does not configure Pixi or mamba.

### OSM parsing is very slow on Windows

Make sure the R package `osmdata` uses the Mobility-provided patched version. On Windows, `mobility.set_params(...)` installs this version automatically from the installed Python package.

If needed, print the ZIP file path with:

```shell
pixi run python -c "from importlib import resources; print(resources.files('mobility.runtime.resources').joinpath('osmdata_0.2.5.005.zip'))"
```

If you use mamba, run the same Python command inside the active `mobility` environment.

To check the installed R packages, open R in the same environment and run:

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

First open R inside the same environment:

```shell
pixi run R
```

If you use mamba:

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
        "data.table", "arrow", "lubridate", "future.apply",
        "duckdb", "DBI", "jsonlite", "gtfsrouter", "geos", "wk", "FNN",
        "dbscan"
    ),
    method = "wininet"
)
install.packages(
    "cppRoutingCCH",
    repos = c(
        "https://mobility-team.r-universe.dev",
        "https://cloud.r-project.org"
    ),
    method = "wininet"
)
```

`cppRoutingCCH` is installed from Mobility's r-universe repository because Mobility uses it for CH and CCH road-network routing.

Then install the patched `osmdata` ZIP as described above.

### Mamba cannot download packages on a company network

Pixi has `tls-root-certs`. Mamba does not use that Pixi setting.

If `mamba env create` fails with a certificate error, configure certificates in your Miniforge or conda setup. On many installations this means setting `ssl_verify` to a company certificate PEM file, but the right file and path depend on your computer.

Ask your IT team for the company certificate file if the default mamba setup cannot download packages.

### You need help with a new error

Check the [open bug reports](https://github.com/mobility-team/mobility/issues?q=is%3Aissue%20state%3Aopen%20label%3Abug). If no open issue matches the problem, open a new issue with the command you ran and the full error message.
