FROM ubuntu:24.04

ARG R_VERSION=4.4.1
ARG UV_VERSION=0.10.9
ARG MOBILITY_PACKAGE=mobility-tools

ENV DEBIAN_FRONTEND=noninteractive
ENV R_VERSION=${R_VERSION}
ENV RSPM=https://packagemanager.posit.co/cran/__linux__/noble/latest
ENV RENV_CONFIG_REPOS_OVERRIDE=${RSPM}
ENV R_LIBS_SITE=/opt/R/${R_VERSION}/lib/R/site-library
ENV R_LIB_FOR_PAK=${R_LIBS_SITE}
ENV VIRTUAL_ENV=/opt/venv
ENV PATH=/opt/venv/bin:/opt/uv/bin:/opt/R/${R_VERSION}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Install the operating system tools, Python, and R with one Ubuntu toolchain.
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
    && python3.12 -m venv /opt/venv \
    && rm -rf /var/lib/apt/lists/* /tmp/r.deb

# Copy dependency metadata first so Docker can reuse these layers.
COPY DESCRIPTION /tmp/mobility-r-deps/

# Install the R packages used by Mobility. pak also installs the matching
# Ubuntu system libraries, which avoids mixing conda and system compilers.
RUN Rscript -e "options(repos = c(mobilityteam = 'https://mobility-team.r-universe.dev', CRAN = Sys.getenv('RSPM'))); install.packages('pak', repos = sprintf('https://r-lib.github.io/p/pak/stable/%s/%s/%s', .Platform\$pkgType, R.Version()\$os, R.Version()\$arch)); pak::pkg_install(c('deps::/tmp/mobility-r-deps', 'any::sessioninfo'), dependencies = TRUE)" \
    && rm -rf /var/lib/apt/lists/*

# Install Mobility from PyPI. Release publishing passes an exact package version.
RUN uv pip install --python /opt/venv/bin/python "${MOBILITY_PACKAGE}"

WORKDIR /app

CMD ["python"]
