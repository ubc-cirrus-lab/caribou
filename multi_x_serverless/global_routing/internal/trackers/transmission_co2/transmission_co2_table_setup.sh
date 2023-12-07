#!/bin/bash

TABLE_NAME="multi-x-serverless-transmission-co2"
REGION="us-west-2"

# Check if the table exists
TABLE_EXISTS=$(aws dynamodb describe-table --table-name $TABLE_NAME --region $REGION)

if [ -z "$TABLE_EXISTS" ]; then
    # Create the table if it doesn't exist
    aws dynamodb create-table \
        --table-name $TABLE_NAME \
        --attribute-definitions \
            AttributeName=region_from_to_codes,AttributeType=S \
            AttributeName=timestamp,AttributeType=S \
        --key-schema \
            AttributeName=region_from_to_codes,KeyType=HASH \
            AttributeName=timestamp,KeyType=RANGE \
        --billing-mode PAY_PER_REQUEST \
        --region $REGION
fi