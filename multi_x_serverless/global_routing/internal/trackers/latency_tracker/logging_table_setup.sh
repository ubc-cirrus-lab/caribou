#!/bin/bash

TABLE_NAME="multi-x-serverless-ping-results"
REGION="us-west-2"

# Check if the table exists
TABLE_EXISTS=$(aws dynamodb describe-table --table-name $TABLE_NAME --region $REGION)

if [ -z "$TABLE_EXISTS" ]; then
    # Create the table if it doesn't exist
    aws dynamodb create-table \
        --table-name $TABLE_NAME \
        --attribute-definitions \
            AttributeName=timestamp,AttributeType=S \
            AttributeName=region_from,AttributeType=S \
        --key-schema \
            AttributeName=timestamp,KeyType=HASH \
            AttributeName=region_from,KeyType=RANGE \
        --billing-mode PAY_PER_REQUEST \
        --region $REGION
fi