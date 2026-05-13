import json
import logging
import os
import pathlib
import platform
import sys
import warnings

from importlib import resources
from mobility.runtime.r_integration.r_script_runner import RScriptRunner

# This is a workaround for retained native memory after heavy Polars workloads.
# Keep the references below so we can revisit this if upstream behavior improves:
# - https://github.com/pola-rs/polars/issues/22871
# - https://github.com/pola-rs/polars/issues/23128
# - https://github.com/pola-rs/polars/issues/27061
# - https://github.com/pola-rs/polars/pull/27395


def set_params(
    package_data_folder_path=None,
    project_data_folder_path=None,
    path_to_pem_file=None,
    http_proxy_url=None,
    https_proxy_url=None,
    inject_into_ssl=False,
    r_packages=True,
    r_packages_force_reinstall=False,
    r_packages_download_method="auto",
    debug=False,
    logging_level="INFO",
    r_timeout_seconds=None,
    r_max_retries=0,
    r_retry_delay_seconds=5,
    r_heartbeat_interval_seconds=30,
):
    """
    Sets up the necessary environment for the Mobility package.

    This function configures logging, sets various environment variables, and establishes default paths
    for package and project data folders.

    Parameters:
    package_data_folder_path (str, optional): The file path for storing common datasets used by all projects.
    project_data_folder_path (str, optional): The file path for storing project-specific datasets.
    path_to_pem_file (str, optional): The file path to the PEM file for SSL certification.
    http_proxy_url (str, optional): The URL for the HTTP proxy.
    https_proxy_url (str, optional): The URL for the HTTPS proxy.
    inject_into_ssl (bool, optional): Whether to inject the truststore package into Python's SSL handling.
    r_packages (boolean, optional): whether to install R packages or not by running RScriptRunner (does not work for github actions so is handled by a separate r-lib github action)
    r_packages_force_reinstall (bool, optional)
    r_packages_download_method (str, optional): set this parameter to "wininet" to be able to install packages on some proxies. See the installation.md page for details.
    debug (bool, optional): set debug to True to see the R logs, including error messages
    logging_level (str|int, optional): root logging level, e.g. "INFO" or "DEBUG"
    r_timeout_seconds (int, optional): timeout applied to each R script run. Leave as None to disable the timeout.
    r_max_retries (int, optional): number of times to retry a failed or timed out R script run.
    r_retry_delay_seconds (int, optional): waiting time between two R script attempts.
    r_heartbeat_interval_seconds (int, optional): frequency of the R runner heartbeat logs.
    """

    setup_logging(logging_level)

    set_env_variable("MOBILITY_ENV_PATH", str(pathlib.Path(sys.executable).parent))
    set_env_variable("MOBILITY_CERT_FILE", path_to_pem_file)
    set_env_variable("HTTP_PROXY", http_proxy_url)
    set_env_variable("HTTPS_PROXY", https_proxy_url)
    set_env_variable("MOBILITY_R_TIMEOUT_SECONDS", r_timeout_seconds)
    set_env_variable("MOBILITY_R_MAX_RETRIES", r_max_retries)
    set_env_variable("MOBILITY_R_RETRY_DELAY_SECONDS", r_retry_delay_seconds)
    set_env_variable("MOBILITY_R_HEARTBEAT_INTERVAL_SECONDS", r_heartbeat_interval_seconds)

    os.environ["MOBILITY_DEBUG"] = "1" if debug else "0"
    setup_ssl_truststore(inject_into_ssl)

    setup_package_data_folder_path(package_data_folder_path)
    setup_project_data_folder_path(project_data_folder_path)

    install_r_packages(r_packages, r_packages_force_reinstall, r_packages_download_method)


def update(memory_reclaim_policy=None):
    """
    Creates or updates the Mobility user config file.

    Parameters:
    memory_reclaim_policy (str, optional): Startup memory reclaim policy.
    Returns:
    pathlib.Path: Path to the updated config file.

    This function persists the preferred startup policy for future Python
    sessions in ``~/.mobility/mobility_config.json``. It does not try to
    reconfigure the current process, because allocator settings are usually
    applied too early for a live update to be reliable once Polars has already
    been imported.
    """
    if memory_reclaim_policy is None:
        raise ValueError(
            "update() needs a memory_reclaim_policy value. "
            "Use 'aggressive' or 'default'."
        )

    config_path = pathlib.Path.home() / ".mobility" / "mobility_config.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        if not isinstance(config, dict):
            raise ValueError(
                "Mobility config file must contain a JSON object. "
                f"Please check this file: {config_path}"
            )
    else:
        config = {}

    if memory_reclaim_policy not in {"aggressive", "default"}:
        raise ValueError(
            "Unknown memory reclaim policy: "
            f"{memory_reclaim_policy}. Supported values are: "
            "'aggressive' and 'default'."
        )
    config["memory_reclaim_policy"] = memory_reclaim_policy

    with config_path.open("w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2, sort_keys=True)
        handle.write("\n")

    warnings.warn(
        "Saved Mobility config to "
        f"{config_path}. Restart Python or your notebook kernel to apply the new "
        "startup memory policy. Import mobility before polars, otherwise the "
        "allocator settings will be applied too late for the current process.",
        stacklevel=2,
    )

    return config_path


def apply_import_time_memory_reclaim_policy():
    """
    Applies the startup memory reclaim policy before Polars is imported.

    Parameters:
    Returns:
    str: Applied memory reclaim policy.

    Why this exists:
    heavy Polars workloads can keep a high amount of reserved native memory
    after peak operations. More aggressive allocator reclaim settings can
    reduce retained RAM a lot on long simulations, which is usually a better
    default for Mobility users than maximizing raw throughput.

    The startup config is read from ``~/.mobility/mobility_config.json`` on
    purpose. It must be available before runtime setup, because custom package
    data folders are usually only known later in the process.

    Upstream context:
    - https://github.com/pola-rs/polars/issues/22871
    - https://github.com/pola-rs/polars/issues/23128
    - https://github.com/pola-rs/polars/issues/27061
    - https://github.com/pola-rs/polars/pull/27395
    """
    config_path = pathlib.Path.home() / ".mobility" / "mobility_config.json"
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        if not isinstance(config, dict):
            raise ValueError(
                "Mobility config file must contain a JSON object. "
                f"Please check this file: {config_path}"
            )
    else:
        config = {}
    policy = config.get("memory_reclaim_policy", "aggressive")
    if policy not in {"aggressive", "default"}:
        raise ValueError(
            "Unknown memory reclaim policy in Mobility config file: "
            f"{policy}. Supported values are: 'aggressive' and 'default'. "
            f"Please update this file: {config_path}"
        )

    if policy == "default":
        return policy

    if platform.system() == "Windows":
        # Windows Python Polars builds use mimalloc, so this is the relevant
        # low-level knob to encourage faster purge of unused pages.
        os.environ.setdefault("MIMALLOC_PURGE_DELAY", "0")
    else:
        # Unix Python Polars builds typically use jemalloc. Setting both decay
        # values to zero makes unused pages eligible for release immediately.
        os.environ.setdefault("_RJEM_MALLOC_CONF", "dirty_decay_ms:0,muzzy_decay_ms:0")

    return policy


def set_env_variable(key, value):
    """
    Sets an environment variable.

    Parameters:
    key (str): The name of the environment variable.
    value (str): The value to be set for the environment variable.
    """
    if value is not None:
        os.environ[key] = str(value)


def setup_logging(logging_level="INFO"):
    """
    Configures the logging for the Mobility package.

    This function sets up basic logging configuration including format, level, and date format.
    """
    if isinstance(logging_level, str):
        level = getattr(logging, logging_level.upper(), None)
        if not isinstance(level, int):
            raise ValueError(f"Unknown logging level: {logging_level}")
    else:
        level = int(logging_level)

    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True,
    )

    # Keep Mobility debug logs available while muting very noisy third-party
    # internals that do not help most users, like Matplotlib font discovery.
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)


def setup_ssl_truststore(inject_into_ssl=False):
    """
    Optionally inject truststore into Python's SSL handling.

    Parameters:
    inject_into_ssl (bool, optional): Whether to inject truststore into ssl.
    """
    if inject_into_ssl:
        try:
            import truststore
        except ImportError as exc:
            raise ImportError(
                "truststore is required when inject_into_ssl=True. "
                "Install the optional dependency with `pip install 'mobility[truststore]'`."
            ) from exc

        truststore.inject_into_ssl()


def setup_package_data_folder_path(package_data_folder_path):
    """
    Sets up the package data folder path.

    If a path is provided, it is used; otherwise, a default path is set. This function also ensures
    the creation of the default folder if it doesn't exist, after user confirmation.

    Parameters:
    package_data_folder_path (str, optional): The file path for storing common datasets.
    """

    if package_data_folder_path is not None:

        if not pathlib.Path(package_data_folder_path).exists():
            os.makedirs(package_data_folder_path)

        os.environ["MOBILITY_PACKAGE_DATA_FOLDER"] = str(package_data_folder_path)

    else:
        default_path = pathlib.Path.home() / ".mobility/data"
        os.environ["MOBILITY_PACKAGE_DATA_FOLDER"] = str(default_path)

        if not default_path.exists():
            logging.info("Mobility needs a folder to store common datasets, that will be used for every project.")
            logging.info("You did not provide the package_data_folder_path argument, so we'll use a default folder : " + str(default_path))

            inp = input("Is this location OK for you ? Yes / No\n")
            inp = inp.lower()

            if "y" in inp:
                os.makedirs(default_path)
            else:
                raise ValueError("Please re run setup_mobility with the package_data_folder_path pointed to your desired location.")


def setup_project_data_folder_path(project_data_folder_path):
    """
    Sets up the project data folder path.

    If a path is provided, it is used; otherwise, a default path is set. This function also ensures
    the creation of the default folder if it doesn't exist, after user confirmation.

    Parameters:
    project_data_folder_path (str, optional): The file path for storing project-specific datasets.
    """

    if project_data_folder_path is not None:

        if not pathlib.Path(project_data_folder_path).exists():
            os.makedirs(project_data_folder_path)

        os.environ["MOBILITY_PROJECT_DATA_FOLDER"] = str(project_data_folder_path)

    else:
        default_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "projects"
        os.environ["MOBILITY_PROJECT_DATA_FOLDER"] = str(default_path)

        if not default_path.exists():

            logging.info("Mobility needs a folder to cache datasets that are specific to projects.")
            logging.info("You did not provide the project_data_folder_path argument, so we'll use a default folder : " + str(default_path))

            inp = input("Is this location OK for you ? Yes / No\n")
            inp = inp.lower()

            if "y" in inp:
                os.makedirs(default_path)
            else:
                raise ValueError("Please re run setup_mobility with the project_data_folder_path pointed to your desired location.")


def install_r_packages(r_packages, r_packages_force_reinstall, r_packages_download_method):
    if r_packages:

        packages = [
            {'source': 'CRAN', 'name': 'remotes'},
            {'source': 'CRAN', 'name': 'dodgr'},
            {'source': 'CRAN', 'name': 'sf'},
            {'source': 'CRAN', 'name': 'dplyr'},
            {'source': 'CRAN', 'name': 'sfheaders'},
            {'source': 'CRAN', 'name': 'nngeo'},
            {'source': 'CRAN', 'name': 'data.table'},
            {'source': 'CRAN', 'name': 'arrow'},
            {'source': 'CRAN', 'name': 'hms'},
            {'source': 'CRAN', 'name': 'lubridate'},
            {'source': 'CRAN', 'name': 'future'},
            {'source': 'CRAN', 'name': 'future.apply'},
            {'source': 'CRAN', 'name': 'ggplot2'},
            {'source': 'CRAN', 'name': 'cppRouting'},
            {'source': 'CRAN', 'name': 'duckdb'},
            {'source': 'CRAN', 'name': 'gtfsrouter'},
            {'source': 'CRAN', 'name': 'geos'},
            {'source': 'CRAN', 'name': 'FNN'},
            {'source': 'CRAN', 'name': 'cluster'},
            {'source': 'CRAN', 'name': 'dbscan'}
        ]

        if platform.system() == "Windows":
            packages.append(
                {
                    "source": "local",
                    "path": str(resources.files('mobility.runtime.resources').joinpath('osmdata_0.2.5.005.zip'))
                }
            )
        else:
            packages.append({'source': 'CRAN', 'name': 'osmdata'})

        args = [
            json.dumps(packages),
            str(r_packages_force_reinstall),
            r_packages_download_method
        ]

        script = RScriptRunner(resources.files('mobility.runtime.r_integration').joinpath('install_packages.R'))
        script.run(args)
