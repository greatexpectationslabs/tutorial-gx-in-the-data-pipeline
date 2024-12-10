FROM quay.io/astronomer/astro-runtime:12.5.0-python-3.11-slim

USER root
RUN apt-get update && apt-get -y install jq && rm -rf /var/lib/apt/lists/*
COPY airflow-healthcheck.sh /airflow-healthcheck.sh

USER astro
