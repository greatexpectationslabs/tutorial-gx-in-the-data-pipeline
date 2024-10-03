#!/bin/bash

printf "Running tutorial code tests...\n\n"
docker exec -t tutorial-gx-in-the-data-pipeline-jupyterlab bash -c 'pytest -sv /tests'
printf "Completed tutorial code tests.\n\n"

printf "Running notebook tests...\n\n"
docker exec -t tutorial-gx-in-the-data-pipeline-jupyterlab bash -c 'cd /cookbooks && pytest --nbmake Cookbook*.ipynb'
printf "Completed notebook tests.\n"
