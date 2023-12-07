#!/bin/bash

OPT_IN_REGIONS=(
    af-south-1
    ap-east-1
    ap-south-2
    ap-southeast-3
    ap-southeast-4
    eu-south-1
    eu-south-2
    eu-central-2
    me-south-1
    me-central-1
    il-central-1
)

# Get all AWS regions
REGIONS=$(aws ec2 describe-regions \
    --all-regions \
    --query "Regions[].{Name:RegionName}" \
    --output text)

# Deploy to regions not in the exclusion list
for region in $REGIONS; do
    if [[ ! " ${OPT_IN_REGIONS[@]} " =~ " ${region} " ]]; then
        export AWS_DEFAULT_REGION=$region
        chalice deploy --stage prod
    else
        echo "Skipping deployment for region: $region"
    fi
done
