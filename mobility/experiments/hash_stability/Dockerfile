FROM python:3.12-slim
ARG POLARS_VERSION=1.37.1
WORKDIR /app
RUN pip install --no-cache-dir polars==${POLARS_VERSION}
COPY hash_stability.py /app/hash_stability.py
CMD ["python", "hash_stability.py"]