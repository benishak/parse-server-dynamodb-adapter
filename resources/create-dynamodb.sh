#!/bin/bash
aws dynamodb create-table --table-name parse-server --attribute-definitions AttributeName=_pk_className,AttributeType=S AttributeName=_sk_id,AttributeType=S --key-schema AttributeName=_pk_className,KeyType=HASH AttributeName=_sk_id,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 --endpoint-url http://localhost:8000 > /dev/null && sleep 0.1
