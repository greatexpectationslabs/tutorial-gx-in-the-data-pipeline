#!/bin/bash

printf "Healthcheck: airflow scheduler\n"
curl --fail -s http://localhost:8080/health | jq -e '.scheduler.status == "healthy"' || exit 1

printf "Healthcheck: hit airflow api\n"
curl --fail -s --user "admin:gx" http://airflow:8080/api/v1/dags | jq -e '.dags[0].dag_display_name | test("cookbook")' || exit 1

printf "All airflow checks are healthy."
exit 0
