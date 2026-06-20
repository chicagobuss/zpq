FROM public.ecr.aws/lambda/python:3.13

# Polars + DuckDB are the real-world Python comparables for the
# "filter Parquet on S3" workload. boto3 ships in the base image.
RUN pip install --no-cache-dir polars==1.36.1 duckdb==1.4.3

COPY handler.py ${LAMBDA_TASK_ROOT}

CMD ["handler.handler"]
