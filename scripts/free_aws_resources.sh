#!/bin/bash

# Set AWS CLI profile and region
AWS_REGIONS=$(aws ec2 describe-regions --query "Regions[].RegionName" --output text)

# Delete Lambda functions
for region in $AWS_REGIONS; do
  echo "Deleting Lambda functions in $region"
  aws lambda list-functions --region $region --query "Functions[].FunctionName" --output text | \
    xargs -I {} aws lambda delete-function --function-name {} --region $region
done

# Remove targets from CloudWatch Events rules
for region in $AWS_REGIONS; do
  echo "Removing targets from CloudWatch Events rules in $region"
  for rule_name in $(aws events list-rules --region $region --query "Rules[].Name" --output text); do
    targets_ids=$(aws events list-targets-by-rule --rule $rule_name --region $region --query "Targets[].Id" --output text)
    if [ ! -z "$targets_ids" ]; then
      aws events remove-targets --rule $rule_name --ids $targets_ids --region $region
    fi
  done
done

# Delete CloudWatch Events rules
for region in $AWS_REGIONS; do
  echo "Deleting CloudWatch Events rules in $region"
  aws events list-rules --region $region --query "Rules[].Name" --output text | \
    xargs -I {} aws events delete-rule --name {} --region $region
done
