FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN apt-get update && apt-get install -y \
    curl ca-certificates bzip2 

# Add proxy certificate
COPY certs/ /tmp/certs/

RUN if [ -d /tmp/certs ] && [ "$(ls -A /tmp/certs)" ]; then \
      cp /tmp/certs/* /usr/local/share/ca-certificates/ && \
      update-ca-certificates; \
    fi && rm -rf /tmp/certs

# Install micromamba
ENV MAMBA_ROOT_PREFIX=/opt/conda
ENV PATH=$MAMBA_ROOT_PREFIX/bin:$PATH
RUN mkdir -p $MAMBA_ROOT_PREFIX && \
    curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj -C $MAMBA_ROOT_PREFIX

# Copy environment and project files
WORKDIR /workspaces/mobility
COPY environment.yml . 
COPY DESCRIPTION . 

# Create and activate environment
RUN micromamba create -y -f environment.yml

# Install pak and R dependencies from DESCRIPTION
RUN echo "\
    options(\
        async_http_cainfo = '/etc/ssl/certs/ca-certificates.crt',\
        repos = c(CRAN = 'https://packagemanager.posit.co/cran/__linux__/jammy/latest')\
    )" > .Rprofile

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

RUN micromamba run -n mobility Rscript -e "\
  install.packages('pak', repos = 'https://cloud.r-project.org/'); \
  pak::pkg_install('deps::.', dependencies = TRUE); \
"

# Set bash as default shell
RUN echo 'eval "$(micromamba shell hook -s bash)"' >> /etc/bash.bashrc && \
    echo "micromamba activate mobility" >> /etc/bash.bashrc
