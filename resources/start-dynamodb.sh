#!/bin/bash
wget http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz -O /tmp/dynamodb_local_latest.tar.gz \
    && tar -xzf /tmp/dynamodb_local_latest.tar.gz -C /tmp \
    && rm -rf /tmp/dynamodb_local_latest.tar.gz \
    && (nohup java -Djava.library.path=/tmp/DynamoDBLocal_lib -jar /tmp/DynamoDBLocal.jar -inMemory > /dev/null 2>&1 &) \
    && aws dynamodb create-table --table-name parse-server --attribute-definitions AttributeName=_pk_className,AttributeType=S AttributeName=_sk_id,AttributeType=S --key-schema AttributeName=_pk_className,KeyType=HASH AttributeName=_sk_id,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 --endpoint-url http://localhost:8000 && sleep 1
