FROM apache/airflow:3.3.2

USER root

RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        unixodbc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:root pyproject.toml /opt/shopee-pipeline/
COPY --chown=airflow:root src /opt/shopee-pipeline/src

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    /opt/shopee-pipeline
