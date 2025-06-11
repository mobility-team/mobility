FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# Install system packages
RUN apt-get update && apt-get install -y \
    curl wget git bash bzip2 ca-certificates sudo locales \
    libcurl4-openssl-dev libssl-dev libxml2-dev libfontconfig1-dev libfreetype6-dev libharfbuzz-dev \
    libfribidi-dev libpng-dev libtiff5-dev libjpeg-dev \
    software-properties-common make g++ zlib1g-dev \
    python3.11 python3.11-venv python3.11-dev python3-pip \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install R
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends software-properties-common dirmngr gnupg && \
    wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc && \
    add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/" && \
    apt-get update -qq && \
    apt-get install -y --no-install-recommends r-base

# Set up Python
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \
    python -m pip install --upgrade pip setuptools wheel

# Add proxy certificate
ARG PROXY_CA_PATH
COPY ${PROXY_CA_PATH} /usr/local/share/ca-certificates/proxy-ca.crt
RUN update-ca-certificates

# Install micromamba
# Install micromamba (official method)
ENV MAMBA_ROOT_PREFIX=/opt/conda
ENV PATH=$MAMBA_ROOT_PREFIX/bin:$PATH
RUN mkdir -p $MAMBA_ROOT_PREFIX && \
    curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj -C $MAMBA_ROOT_PREFIX

# Ensure environment auto-activates in bash sessions
RUN echo "source /opt/conda/etc/profile.d/micromamba.sh" >> /etc/bash.bashrc && \
    echo "micromamba activate test-env" >> /etc/bash.bashrc

# Copy environment and project files
COPY environment-test.yml /tmp/environment-test.yml
COPY . /app
WORKDIR /app

# Create and activate environment
RUN /opt/conda/bin/micromamba create -y -n test-env -f /tmp/environment-test.yml

# Install pip dependencies
RUN bash -c "source /opt/conda/etc/profile.d/micromamba.sh && micromamba activate test-env && pip install .[dev]"

# Install pak and R dependencies from DESCRIPTION
COPY DESCRIPTION /app/DESCRIPTION
RUN Rscript -e "install.packages('pak', repos = 'https://r-lib.github.io/p/pak/stable')" \
    -e "pak::pkg_install('deps::.', dependencies = TRUE)"

# Set bash as default shell
SHELL ["/bin/bash", "-c"]

# Default CMD for interactive container use
CMD ["bash"]
