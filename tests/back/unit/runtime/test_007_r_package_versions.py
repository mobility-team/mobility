import pathlib
import subprocess


def test_r_universe_package_version_decisions():
    helper_path = (
        pathlib.Path(__file__).parents[4]
        / "mobility"
        / "runtime"
        / "r_integration"
        / "package_versions.R"
    )
    r_code = f'''
source("{helper_path.as_posix()}")

stopifnot(r_universe_package_needs_install(NULL, "3.3.0"))
stopifnot(!r_universe_package_needs_install("3.3.0", "3.3.0"))
stopifnot(!r_universe_package_needs_install("3.3.0", "3.2.1"))
stopifnot(r_universe_package_needs_install("3.2.1", "3.3.0"))
stopifnot(r_universe_package_needs_install("3.3.0", "3.3.0", TRUE))

stopifnot(package_version_meets_minimum("3.3.0", "3.3.0"))
stopifnot(package_version_meets_minimum("3.4.0", "3.3.0"))
stopifnot(!package_version_meets_minimum("3.2.1", "3.3.0"))
'''

    subprocess.run(["Rscript", "-e", r_code], check=True)
