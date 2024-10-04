#!/bin/bash

curl --fail -s http://localhost:8080/health | jq -e '.scheduler.status == "healthy"' || exit 1