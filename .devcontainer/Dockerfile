# -------- BUILDER STAGE --------
FROM python:3.12-slim AS builder

# Add optional proxy certificate
COPY certs/ /tmp/certs/
RUN if [ -d /tmp/certs ] && [ "$(ls -A /tmp/certs)" ]; then \
      cp /tmp/certs/* /usr/local/share/ca-certificates/ && \
      update-ca-certificates; \
    fi && rm -rf /tmp/certs

# System packages for compiling osmium + R/Python deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git curl pkg-config libcurl4-openssl-dev \
    libosmium2-dev libprotozero-dev nlohmann-json3-dev \
    libboost-program-options-dev libbz2-dev zlib1g-dev libexpat1-dev \
    libutfcpp-dev liblz4-dev \
    r-base r-base-dev \
    && rm -rf /var/lib/apt/lists/*

# Build osmium-tool from source (v1.18.0)
RUN git clone --branch v1.18.0 https://github.com/osmcode/osmium-tool.git /tmp/osmium \
    && cd /tmp/osmium && mkdir build && cd build \
    && cmake .. && make -j2 && make install

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Copy only dependency metadata for caching
WORKDIR /build
COPY pyproject.toml uv.lock* ./
RUN uv pip install --system .
RUN uv pip install --system ipykernel

# Install R deps with pak
RUN echo "options( \
    async_http_cainfo = '/etc/ssl/certs/ca-certificates.crt', \
    repos = c( \
        P3M  = 'https://p3m.dev/cran/__linux__/bookworm/latest', \
        CRAN = 'https://cloud.r-project.org' \
    ))" \
    > .Rprofile
RUN Rscript -e "install.packages('pak', repos = 'https://cloud.r-project.org')"
COPY DESCRIPTION .
RUN Rscript -e "pak::cache_cleanup(force = TRUE)"
RUN Rscript -e "pak::pkg_install('deps::.', dependencies = TRUE)"

# # -------- RUNTIME STAGE --------
# FROM python:3.12-slim

# # Add optional proxy certificate
# COPY certs/ /tmp/certs/
# RUN if [ -d /tmp/certs ] && [ "$(ls -A /tmp/certs)" ]; then \
#       cp /tmp/certs/* /usr/local/share/ca-certificates/ && \
#       update-ca-certificates; \
#     fi && rm -rf /tmp/certs

# # Runtime libs only
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     curl \
#     libboost-program-options-dev \
#     libbz2-1.0 \
#     zlib1g \
#     libexpat1 \
#     liblz4-1 \
#     libosmium2-dev \
#     libprotozero-dev \
#     r-base \
#     && rm -rf /var/lib/apt/lists/*

# # Copy osmium / python / uv /  R from the build stage
# COPY --from=builder /usr/local/bin/osmium /usr/local/bin/osmium
# COPY --from=builder /usr/local/lib/python3.12 /usr/local/lib/python3.12
# COPY --from=builder /root/.local/bin/uv /usr/local/bin/uv
# COPY --from=builder /usr/local/lib/R /usr/local/lib/R