FROM ubuntu:24.04

ARG R_VERSION=4.4.1
ARG UV_VERSION=0.10.9

ENV DEBIAN_FRONTEND=noninteractive
ENV R_VERSION=${R_VERSION}
ENV RSPM=https://packagemanager.posit.co/cran/__linux__/noble/latest
ENV RENV_CONFIG_REPOS_OVERRIDE=${RSPM}
ENV R_LIBS_SITE=/opt/R/${R_VERSION}/lib/R/site-library
ENV R_LIB_FOR_PAK=${R_LIBS_SITE}
ENV VIRTUAL_ENV=/opt/venv-ci
ENV PATH=/opt/venv-ci/bin:/opt/uv/bin:/opt/R/${R_VERSION}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Install only bootstrap tools and non-R runtime tools. pak installs the system
# libraries needed by the R packages from DESCRIPTION during the image build.
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    gdebi-core \
    git \
    gnupg \
    osmium-tool \
    python3.12 \
    python3.12-venv \
    sudo \
    && curl -fsSL -o /tmp/r.deb "https://cdn.posit.co/r/ubuntu-2404/pkgs/r-${R_VERSION}_1_$(dpkg --print-architecture).deb" \
    && gdebi --non-interactive /tmp/r.deb \
    && ln -sf "/opt/R/${R_VERSION}/bin/R" /usr/local/bin/R \
    && ln -sf "/opt/R/${R_VERSION}/bin/Rscript" /usr/local/bin/Rscript \
    && python3.12 -m venv /opt/uv \
    && /opt/uv/bin/pip install --no-cache-dir "uv==${UV_VERSION}" \
    && rm -rf /var/lib/apt/lists/* /tmp/r.deb

# Copy only dependency files so Docker keeps these layers when package code changes.
COPY requirements-ci.txt requirements-min.txt /tmp/mobility-python-deps/
COPY DESCRIPTION /tmp/mobility-r-deps/

# Pre-install both Python dependency sets. The workflow selects one venv per job.
RUN python3.12 -m venv /opt/venv-ci \
    && uv pip sync --python /opt/venv-ci/bin/python /tmp/mobility-python-deps/requirements-ci.txt \
    && python3.12 -m venv /opt/venv-min \
    && uv pip sync --python /opt/venv-min/bin/python /tmp/mobility-python-deps/requirements-min.txt

# Pre-install the R packages declared in DESCRIPTION, plus sessioninfo for CI logs.
RUN Rscript -e "options(repos = c(mobilityteam = 'https://mobility-team.r-universe.dev', CRAN = Sys.getenv('RSPM'))); install.packages('pak', repos = sprintf('https://r-lib.github.io/p/pak/stable/%s/%s/%s', .Platform\$pkgType, R.Version()\$os, R.Version()\$arch)); pak::pkg_install(c('deps::/tmp/mobility-r-deps', 'any::sessioninfo'), dependencies = TRUE)" \
    && rm -rf /var/lib/apt/lists/*
