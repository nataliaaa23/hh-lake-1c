FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb boto3 psutil
WORKDIR /app
