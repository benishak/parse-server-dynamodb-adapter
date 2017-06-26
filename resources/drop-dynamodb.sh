#!/bin/bash
aws dynamodb delete-table --table-name parse-server --endpoint http://localhost:8000 > /dev/null && sleep 0.05
