FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AIOPS_HTTP_PORT=8085 \
    AIOPS_DATA_DIR=/data \
    PROMETHEUS_BASE_URL=http://prometheus:9090

COPY infra/aiops/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY infra/aiops/app /app/app

EXPOSE 8085

CMD ["bash", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${AIOPS_HTTP_PORT}"]
