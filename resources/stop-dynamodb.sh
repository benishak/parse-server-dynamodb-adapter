#!/bin/bash
aws dynamodb delete-table --table-name parse-server --endpoint http://localhost:8000 && pkill -f 'java.*DynamoDB' && rm -rf /tmp/DynamoDBLocal*
