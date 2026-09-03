FROM public.ecr.aws/lambda/python:3.12

# Build one image per engine so a Polars cold start does not carry the DuckDB
# wheel (or vice versa). Both are current stable releases as of 2026-08-21.
ARG ENGINE
ARG DUCKDB_VERSION=1.5.5
ARG POLARS_VERSION=1.43.2

RUN test "$ENGINE" = duckdb || test "$ENGINE" = polars
RUN if [ "$ENGINE" = duckdb ]; then \
      pip install --no-cache-dir "duckdb==$DUCKDB_VERSION" && \
      mkdir -p /opt/duckdb && \
      HOME=/opt/duckdb python -c "import duckdb; c=duckdb.connect(':memory:'); c.execute('INSTALL httpfs'); c.execute('INSTALL aws')" && \
      mkdir -p "${LAMBDA_TASK_ROOT}/duckdb_extensions" && \
      cp -a /opt/duckdb/.duckdb/extensions/. "${LAMBDA_TASK_ROOT}/duckdb_extensions/"; \
    else \
      pip install --no-cache-dir "polars==$POLARS_VERSION"; \
    fi

# DuckDB must load its S3 extension from the image layer. Fetching extensions
# during an invocation would turn package-registry availability into benchmark
# noise and penalize every cold start.
ENV DUCKDB_EXTENSION_DIRECTORY=${LAMBDA_TASK_ROOT}/duckdb_extensions

COPY handler.py ${LAMBDA_TASK_ROOT}

CMD ["handler.handler"]
