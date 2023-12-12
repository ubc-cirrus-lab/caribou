#!/bin/bash

# Usage: ./free_aws_resources.sh "function_name1 function_name2" "rule_name1 rule_name2"

# Set AWS CLI profile and region
AWS_REGIONS=$(aws ec2 describe-regions --query "Regions[].RegionName" --output text)

# Check if Lambda function names and CloudWatch Events rule names are provided
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <lambda_function_names> <cwe_rule_names>"
    exit 1
fi

# Extract Lambda function names and CloudWatch Events rule names from command-line parameters
LAMBDA_FUNCTION_NAMES=("$1")
CWE_RULE_NAMES=("$2")

# Delete Lambda functions
for region in $AWS_REGIONS; do
    echo "Deleting Lambda functions in $region"
    for function_name in "${LAMBDA_FUNCTION_NAMES[@]}"; do
        aws lambda delete-function --function-name $function_name --region $region
    done
done

# Remove targets from CloudWatch Events rules
for region in $AWS_REGIONS; do
    echo "Removing targets from CloudWatch Events rules in $region"
    for rule_name in "${CWE_RULE_NAMES[@]}"; do
        targets_ids=$(aws events list-targets-by-rule --rule $rule_name --region $region --query "Targets[].Id" --output text)
        if [ ! -z "$targets_ids" ]; then
            aws events remove-targets --rule $rule_name --ids $targets_ids --region $region
        fi
    done
done

# Delete CloudWatch Events rules
for region in $AWS_REGIONS; do
    echo "Deleting CloudWatch Events rules in $region"
    for rule_name in "${CWE_RULE_NAMES[@]}"; do
        aws events delete-rule --name $rule_name --region $region
    done
done
