#!/bin/bash

docker exec -t tutorial-gx-in-the-data-pipeline-jupyterlab bash -c 'isort --profile=black ./ /tests && black ./ /tests'
