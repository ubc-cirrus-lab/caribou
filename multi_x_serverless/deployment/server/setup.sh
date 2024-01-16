#!/bin/bash

# Setup the necessary tables in DynamoDB

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

create_table() {
    local table_name=$1
    aws dynamodb create-table \
        --table-name "$table_name" \
        --attribute-definitions AttributeName=key,AttributeType=S \
        --key-schema AttributeName=key,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1
}

# List of table names
table_names=($(grep '_TABLE' "$SCRIPT_DIR/../common/constants.py" | cut -d'=' -f2 | tr -d ' "' | tr -d "'"))

# Loop through the table names and create each table
for table_name in "${table_names[@]}"; do
    echo "Creating table: $table_name"
    create_table "$table_name"
done
